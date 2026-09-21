package main

import (
	"net/http/httptest"
	"testing"

	"github.com/imarchuang/marchimetrics/storage"
)

// PR3: the query endpoint exposes scan cost via X-Marchimetrics-* headers.
func TestQueryRangeStatsHeaders(t *testing.T) {
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	labels := []storage.Label{{Name: storage.MetricNameLabel, Value: "m"}, {Name: "job", Value: "api"}}
	store.Append(labels, []storage.Sample{{Timestamp: 1000, Value: 1}, {Timestamp: 2000, Value: 2}})
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	store.Append(labels, []storage.Sample{{Timestamp: 3000, Value: 3}}) // mem tier

	srv := newServer(store)
	req := httptest.NewRequest("GET", "/api/v1/query_range?query=m&start=0&end=10", nil)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	checks := map[string]string{
		"X-Marchimetrics-Parts-Scanned":   "1",
		"X-Marchimetrics-Blocks-Scanned":  "1",
		"X-Marchimetrics-Points-Scanned":  "2", // disk tier only
		"X-Marchimetrics-Points-Returned": "3", // disk + mem
	}
	for h, want := range checks {
		if got := rec.Header().Get(h); got != want {
			t.Errorf("header %s = %q, want %q", h, got, want)
		}
	}
}

// PR4: POST /internal/force_merge compacts small parts on demand.
func TestForceMergeEndpoint(t *testing.T) {
	store, _ := storage.Open(t.TempDir())
	labels := []storage.Label{{Name: storage.MetricNameLabel, Value: "m"}}
	store.Append(labels, []storage.Sample{{Timestamp: 1000, Value: 1}})
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	store.Append(labels, []storage.Sample{{Timestamp: 2000, Value: 2}})
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}

	srv := newServer(store)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("POST", "/internal/force_merge", nil))
	if rec.Code != 204 {
		t.Fatalf("force_merge status = %d, body %s", rec.Code, rec.Body)
	}

	// Data survives the merge: both points queryable.
	req := httptest.NewRequest("GET", "/api/v1/query_range?query=m&start=0&end=10", nil)
	rec = httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("query status = %d", rec.Code)
	}
	if got := rec.Header().Get("X-Marchimetrics-Points-Returned"); got != "2" {
		t.Fatalf("Points-Returned = %s, want 2 after merge", got)
	}
	if got := rec.Header().Get("X-Marchimetrics-Parts-Scanned"); got != "1" {
		t.Fatalf("Parts-Scanned = %s, want 1 (merged big part)", got)
	}
}

func TestQueryRangeBadRequests(t *testing.T) {
	store, _ := storage.Open(t.TempDir())
	srv := newServer(store)
	for _, url := range []string{
		"/api/v1/query_range?query=&start=0&end=10",           // empty selector
		"/api/v1/query_range?query=m&start=abc&end=10",        // bad start
		"/api/v1/query_range?query=m&start=10&end=0",          // end before start
		"/api/v1/query_range?query=m{job=api}&start=0&end=10", // unquoted value
	} {
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
		if rec.Code != 400 {
			t.Errorf("GET %s: status = %d, want 400", url, rec.Code)
		}
	}
}
