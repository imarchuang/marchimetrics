package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func testLabels(metric, job string) []Label {
	return []Label{{Name: MetricNameLabel, Value: metric}, {Name: "job", Value: job}}
}

func mustQuery(t *testing.T, s *Storage, matchers []Matcher, start, end int64) []SeriesResult {
	t.Helper()
	got, _, err := s.QueryRange(matchers, start, end)
	if err != nil {
		t.Fatalf("QueryRange: %s", err)
	}
	return got
}

// The PR1 acceptance test: data appended to the in-memory buffer is
// queryable before any flush to disk exists.
func TestAppendQueryableBeforeFlush(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.Append(testLabels("http_requests_total", "api"), []Sample{
		{Timestamp: 1000, Value: 1.5},
		{Timestamp: 2000, Value: 2.5},
	})

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "http_requests_total"}}, 0, 10_000)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	if len(got[0].Samples) != 2 || got[0].Samples[0].Value != 1.5 || got[0].Samples[1].Value != 2.5 {
		t.Fatalf("unexpected samples: %+v", got[0].Samples)
	}
}

func TestQueryRangeTimeFilterAndOrder(t *testing.T) {
	s, _ := Open(t.TempDir())
	// Appended out of order across two calls on purpose: the query side
	// must merge and time-sort.
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 3000, Value: 3}, {Timestamp: 1000, Value: 1}})
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 2000, Value: 2}})

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 1500, 3500)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	want := []Sample{{Timestamp: 2000, Value: 2}, {Timestamp: 3000, Value: 3}}
	if len(got[0].Samples) != len(want) {
		t.Fatalf("got %+v, want %+v", got[0].Samples, want)
	}
	for i := range want {
		if got[0].Samples[i] != want[i] {
			t.Fatalf("sample %d = %+v, want %+v", i, got[0].Samples[i], want[i])
		}
	}
}

func TestQueryRangeSelectsByLabels(t *testing.T) {
	s, _ := Open(t.TempDir())
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 1000, Value: 1}})
	s.Append(testLabels("m", "web"), []Sample{{Timestamp: 1000, Value: 2}})

	got := mustQuery(t, s, []Matcher{{Name: "job", Value: "api"}}, 0, 10_000)
	if len(got) != 1 || got[0].Samples[0].Value != 1 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestConcurrentAppendAndQuery(t *testing.T) {
	s, _ := Open(t.TempDir())
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			labels := testLabels("m", fmt.Sprintf("job-%d", w))
			for i := 0; i < 100; i++ {
				s.Append(labels, []Sample{{Timestamp: int64(i), Value: float64(i)}})
			}
		}(w)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			if _, _, err := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 1000); err != nil {
				t.Errorf("QueryRange: %s", err)
			}
		}
	}()
	wg.Wait()
	<-done

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 1000)
	total := 0
	for _, sr := range got {
		total += len(sr.Samples)
	}
	if total != 800 {
		t.Fatalf("total samples = %d, want 800", total)
	}
}

// PR2: after a flush the mem buffer is empty and queries are served from
// the immutable disk part.
func TestFlushThenQueryFromDisk(t *testing.T) {
	s, _ := Open(t.TempDir())
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 1000, Value: 1}, {Timestamp: 2000, Value: 2}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}

	s.mu.RLock()
	memLen := len(s.mem)
	s.mu.RUnlock()
	if memLen != 0 {
		t.Fatalf("mem buffer not empty after flush: %d series", memLen)
	}

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 10_000)
	if len(got) != 1 || len(got[0].Samples) != 2 {
		t.Fatalf("unexpected result after flush: %+v", got)
	}
}

// PR2 acceptance (PLAN.md first-week checklist #4): flush, reopen the
// storage, and the query still works — registry and parts recovered.
func TestFlushReopenQueryStillWorks(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 1000, Value: 1.5}})
	if err := s.Close(); err != nil { // Close performs the final flush
		t.Fatalf("Close: %s", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	got := mustQuery(t, s2, []Matcher{{Name: "job", Value: "api"}}, 0, 10_000)
	if len(got) != 1 || len(got[0].Samples) != 1 || got[0].Samples[0].Value != 1.5 {
		t.Fatalf("unexpected result after reopen: %+v", got)
	}

	// IDs must continue past the reloaded maximum, not collide.
	newID := s2.Append(testLabels("m", "web"), []Sample{{Timestamp: 1000, Value: 9}})
	if newID <= got[0].ID {
		t.Fatalf("new series got ID %d, want > %d", newID, got[0].ID)
	}
}

// PR2: samples are grouped into day partitions by their own timestamps.
func TestFlushGroupsSamplesByDay(t *testing.T) {
	s, _ := Open(t.TempDir())
	day1 := time.Date(2026, 9, 20, 23, 59, 59, 0, time.UTC).UnixMilli()
	day2 := day1 + 2000 // 2026-09-21T00:00:01Z
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: day2, Value: 2}, {Timestamp: day1, Value: 1}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}

	for _, day := range []string{"20260920", "20260921"} {
		p := s.partitions[day]
		if p == nil {
			t.Fatalf("partition %s not created", day)
		}
		if parts := p.partsSnapshot(); len(parts) != 1 {
			t.Fatalf("partition %s has parts %v, want exactly 1", day, parts)
		}
	}

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, day2+10_000)
	if len(got) != 1 || len(got[0].Samples) != 2 {
		t.Fatalf("unexpected cross-day result: %+v", got)
	}
	if got[0].Samples[0].Timestamp != day1 || got[0].Samples[1].Timestamp != day2 {
		t.Fatalf("cross-day samples not merged in order: %+v", got[0].Samples)
	}
}

// PR2: a query spanning flushed and unflushed data merges both tiers.
func TestQueryMergesMemAndDisk(t *testing.T) {
	s, _ := Open(t.TempDir())
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 1000, Value: 1}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 2000, Value: 2}})

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 10_000)
	if len(got) != 1 || len(got[0].Samples) != 2 {
		t.Fatalf("unexpected merged result: %+v", got)
	}
	if got[0].Samples[0].Value != 1 || got[0].Samples[1].Value != 2 {
		t.Fatalf("merged samples out of order: %+v", got[0].Samples)
	}
}

// PR3: query stats reflect the disk tier — parts/blocks/points scanned
// vs. points returned after merging with the mem buffer.
func TestQueryStatsTiers(t *testing.T) {
	s, _ := Open(t.TempDir())
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 1000, Value: 1}, {Timestamp: 2000, Value: 2}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 3000, Value: 3}}) // stays in mem

	_, stats, err := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 10_000)
	if err != nil {
		t.Fatalf("QueryRange: %s", err)
	}
	want := QueryStats{PartsScanned: 1, BlocksScanned: 1, PointsScanned: 2, PointsReturned: 3}
	if *stats != want {
		t.Fatalf("stats = %+v, want %+v", *stats, want)
	}
}

// PR3: a query window that misses every part scans nothing on disk but
// still returns mem-tier points.
func TestQueryStatsSkipNonOverlappingParts(t *testing.T) {
	s, _ := Open(t.TempDir())
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 1000, Value: 1}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: 500_000, Value: 5}})

	_, stats, err := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 400_000, 600_000)
	if err != nil {
		t.Fatalf("QueryRange: %s", err)
	}
	want := QueryStats{PartsScanned: 0, BlocksScanned: 0, PointsScanned: 0, PointsReturned: 1}
	if *stats != want {
		t.Fatalf("stats = %+v, want %+v", *stats, want)
	}
}

// PR2: concurrent flushes and appends/queries must not race or lose data.
func TestConcurrentFlushAppendQuery(t *testing.T) {
	s, _ := Open(t.TempDir())
	stop := make(chan struct{})

	var workers sync.WaitGroup
	for w := 0; w < 4; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			labels := testLabels("m", fmt.Sprintf("job-%d", w))
			for i := 0; i < 50; i++ {
				s.Append(labels, []Sample{{Timestamp: int64(i * 1000), Value: float64(i)}})
			}
		}(w)
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 50; i++ {
			if _, _, err := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 1_000_000); err != nil {
				t.Errorf("QueryRange: %s", err)
			}
		}
	}()

	var flusher sync.WaitGroup
	flusher.Add(1)
	go func() {
		defer flusher.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := s.Flush(); err != nil {
					t.Errorf("Flush: %s", err)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	workers.Wait() // appends + queries done, then stop the flusher
	close(stop)
	flusher.Wait()

	// Final flush, then every sample must be exactly once on disk.
	if err := s.Flush(); err != nil {
		t.Fatalf("final Flush: %s", err)
	}
	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 1_000_000)
	total := 0
	for _, sr := range got {
		total += len(sr.Samples)
	}
	if total != 200 {
		t.Fatalf("total samples = %d, want 200 (4 jobs x 50)", total)
	}
}
