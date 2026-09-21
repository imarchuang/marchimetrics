package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/imarchuang/marchimetrics/storage"
)

// server wires the storage engine to the HTTP API.
type server struct {
	store *storage.Storage
}

func newServer(store *storage.Storage) *server { return &server{store: store} }

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/import", s.handleImport)
	mux.HandleFunc("/api/v1/query_range", s.handleQueryRange)
	mux.HandleFunc("/internal/force_flush", s.handleForceFlush)
	return mux
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}

// importRequest is the MVP ingest format (PLAN.md section 3). A single
// object or a JSON array of objects is accepted. Timestamps are
// milliseconds since the Unix epoch.
type importRequest struct {
	Metric  string            `json:"metric"`
	Labels  map[string]string `json:"labels"`
	Samples []struct {
		T int64   `json:"t"`
		V float64 `json:"v"`
	} `json:"samples"`
}

func (s *server) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		http.Error(w, "cannot read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	var reqs []importRequest
	if body[0] == '[' {
		err = json.Unmarshal(body, &reqs)
	} else {
		var single importRequest
		err = json.Unmarshal(body, &single)
		reqs = []importRequest{single}
	}
	if err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	ingested := 0
	for i := range reqs {
		req := &reqs[i]
		if req.Metric == "" {
			http.Error(w, "every entry needs a metric name", http.StatusBadRequest)
			return
		}
		labels := make([]storage.Label, 0, len(req.Labels)+1)
		labels = append(labels, storage.Label{Name: storage.MetricNameLabel, Value: req.Metric})
		for k, v := range req.Labels {
			labels = append(labels, storage.Label{Name: k, Value: v})
		}
		samples := make([]storage.Sample, len(req.Samples))
		for j, sm := range req.Samples {
			samples[j] = storage.Sample{Timestamp: sm.T, Value: sm.V}
		}
		s.store.Append(labels, samples)
		ingested += len(samples)
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ingested":%d}`+"\n", ingested)
}

// queryResult is a Prometheus-compatible matrix response.
type queryResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string        `json:"resultType"`
		Result     []matrixEntry `json:"result"`
	} `json:"data"`
}

type matrixEntry struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

func (s *server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	matchers, err := storage.ParseSelector(r.URL.Query().Get("query"))
	if err != nil {
		http.Error(w, "bad query: "+err.Error(), http.StatusBadRequest)
		return
	}
	start, err := parseTimeParam(r.URL.Query().Get("start"))
	if err != nil {
		http.Error(w, "bad start: "+err.Error(), http.StatusBadRequest)
		return
	}
	end, err := parseTimeParam(r.URL.Query().Get("end"))
	if err != nil {
		http.Error(w, "bad end: "+err.Error(), http.StatusBadRequest)
		return
	}
	if end < start {
		http.Error(w, "end must not be before start", http.StatusBadRequest)
		return
	}

	results, stats, err := s.store.QueryRange(matchers, start, end)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Query cost visibility (PLAN.md pass bar: parts/blocks/points scanned).
	w.Header().Set("X-Marchimetrics-Parts-Scanned", strconv.Itoa(stats.PartsScanned))
	w.Header().Set("X-Marchimetrics-Blocks-Scanned", strconv.Itoa(stats.BlocksScanned))
	w.Header().Set("X-Marchimetrics-Points-Scanned", strconv.Itoa(stats.PointsScanned))
	w.Header().Set("X-Marchimetrics-Points-Returned", strconv.Itoa(stats.PointsReturned))

	var resp queryResult
	resp.Status = "success"
	resp.Data.ResultType = "matrix"
	resp.Data.Result = []matrixEntry{}
	for _, sr := range results {
		entry := matrixEntry{Metric: make(map[string]string, len(sr.Labels))}
		for _, l := range sr.Labels {
			entry.Metric[l.Name] = l.Value
		}
		for _, sm := range sr.Samples {
			ts := float64(sm.Timestamp) / 1000
			entry.Values = append(entry.Values, [2]any{ts, strconv.FormatFloat(sm.Value, 'g', -1, 64)})
		}
		resp.Data.Result = append(resp.Data.Result, entry)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleForceFlush triggers an immediate flush of the in-memory buffer
// to disk parts (same name as VictoriaMetrics' endpoint).
func (s *server) handleForceFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if err := s.store.Flush(); err != nil {
		http.Error(w, "flush failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseTimeParam accepts RFC3339 or Unix seconds (Prometheus convention)
// and returns milliseconds.
func parseTimeParam(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty time parameter")
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixMilli(), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("want RFC3339 or unix seconds, got %q", s)
	}
	return int64(f * 1000), nil
}
