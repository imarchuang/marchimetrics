package main

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/imarchuang/marchimetrics/storage"
)

// encodeWriteRequest builds the exact payload a Prometheus server sends:
// a snappy-compressed, protobuf-marshaled WriteRequest.
func encodeWriteRequest(t *testing.T, series ...prompb.TimeSeries) []byte {
	t.Helper()
	req := prompb.WriteRequest{Timeseries: series}
	raw, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal WriteRequest: %s", err)
	}
	return snappy.Encode(nil, raw)
}

func ts(labels map[string]string, samples ...prompb.Sample) prompb.TimeSeries {
	var ls []prompb.Label
	for n, v := range labels {
		ls = append(ls, prompb.Label{Name: n, Value: v})
	}
	return prompb.TimeSeries{Labels: ls, Samples: samples}
}

func TestRemoteWriteIngests(t *testing.T) {
	store, _ := storage.Open(t.TempDir())
	srv := newServer(store)

	body := encodeWriteRequest(t,
		ts(map[string]string{"__name__": "http_requests_total", "job": "api"},
			prompb.Sample{Timestamp: 1000, Value: 1}, prompb.Sample{Timestamp: 2000, Value: 2}),
		ts(map[string]string{"__name__": "http_requests_total", "job": "web"},
			prompb.Sample{Timestamp: 1000, Value: 5}),
	)
	req := httptest.NewRequest("POST", "/api/v1/write", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)

	if rec.Code != 204 {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("X-Marchimetrics-Points-Ingested"); got != "3" {
		t.Fatalf("Points-Ingested = %q, want 3", got)
	}

	// Data is immediately queryable through the normal path.
	got := mustQueryHTTP(t, srv, "/api/v1/query_range?query=http_requests_total&start=0&end=10")
	if got != 3 {
		t.Fatalf("queryable points = %d, want 3", got)
	}
}

func TestRemoteWriteSkipsSeriesWithoutName(t *testing.T) {
	store, _ := storage.Open(t.TempDir())
	srv := newServer(store)

	body := encodeWriteRequest(t,
		ts(map[string]string{"job": "api"}, prompb.Sample{Timestamp: 1000, Value: 1}), // no __name__
		ts(map[string]string{"__name__": "m", "job": "api"}, prompb.Sample{Timestamp: 1000, Value: 2}),
	)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/write", bytes.NewReader(body)))

	if rec.Code != 204 {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("X-Marchimetrics-Points-Ingested"); got != "1" {
		t.Fatalf("Points-Ingested = %q, want 1 (nameless series skipped)", got)
	}
}

func TestRemoteWriteBadPayloads(t *testing.T) {
	store, _ := storage.Open(t.TempDir())
	srv := newServer(store)

	for name, body := range map[string][]byte{
		"not snappy":             []byte("this is not snappy data"),
		"snappy of non-protobuf": snappy.Encode(nil, []byte("not a WriteRequest")),
	} {
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/write", bytes.NewReader(body)))
		if rec.Code != 400 {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/write", nil))
	if rec.Code != 405 {
		t.Errorf("GET: status = %d, want 405", rec.Code)
	}
}

// mustQueryHTTP queries the HTTP endpoint and returns Points-Returned.
func mustQueryHTTP(t *testing.T, srv *server, url string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
	if rec.Code != 200 {
		t.Fatalf("GET %s: status = %d, body %s", url, rec.Code, rec.Body)
	}
	var n int
	if _, err := fmt.Sscanf(rec.Header().Get("X-Marchimetrics-Points-Returned"), "%d", &n); err != nil {
		t.Fatalf("bad Points-Returned header: %s", err)
	}
	return n
}
