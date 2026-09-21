package main

import (
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/imarchuang/marchimetrics/storage"
)

// handleRemoteWrite is a Prometheus remote_write receiver: the body is a
// snappy-compressed prompb.WriteRequest, exactly what a real Prometheus
// or vmagent sends. Any 2xx tells the sender the batch is durably
// accepted (into the mem buffer — see README "Durability & retention");
// 4xx means "do not retry".
//
// Series without a __name__ label are invalid in Prometheus' model; they
// are skipped rather than failing the whole batch (VM does the same for
// bad rows), because one bad series must not block the good ones.
func (s *server) handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		http.Error(w, "cannot read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		http.Error(w, "cannot snappy-decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req prompb.WriteRequest
	if err := req.Unmarshal(raw); err != nil {
		http.Error(w, "cannot unmarshal WriteRequest: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ingested, skipped int
	for i := range req.Timeseries {
		ts := &req.Timeseries[i]
		labels := make([]storage.Label, 0, len(ts.Labels))
		hasName := false
		for _, l := range ts.Labels {
			if l.Name == storage.MetricNameLabel {
				hasName = true
			}
			labels = append(labels, storage.Label{Name: l.Name, Value: l.Value})
		}
		if !hasName || len(ts.Samples) == 0 {
			skipped++
			continue
		}
		samples := make([]storage.Sample, 0, len(ts.Samples))
		for _, sm := range ts.Samples {
			samples = append(samples, storage.Sample{Timestamp: sm.Timestamp, Value: sm.Value})
		}
		s.store.Append(labels, samples)
		ingested += len(samples)
	}
	if skipped > 0 {
		log.Printf("remote_write: skipped %d series without %s or samples", skipped, storage.MetricNameLabel)
	}
	w.Header().Set("X-Marchimetrics-Points-Ingested", strconv.Itoa(ingested))
	w.WriteHeader(http.StatusNoContent)
}
