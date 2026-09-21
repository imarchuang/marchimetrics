package storage

import (
	"fmt"
	"sync"
	"testing"
)

func testLabels(metric, job string) []Label {
	return []Label{{Name: MetricNameLabel, Value: metric}, {Name: "job", Value: job}}
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

	got := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "http_requests_total"}}, 0, 10_000)
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

	got := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 1500, 3500)
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

	got := s.QueryRange([]Matcher{{Name: "job", Value: "api"}}, 0, 10_000)
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
			s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 1000)
		}
	}()
	wg.Wait()
	<-done

	got := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 1000)
	total := 0
	for _, sr := range got {
		total += len(sr.Samples)
	}
	if total != 800 {
		t.Fatalf("total samples = %d, want 800", total)
	}
}
