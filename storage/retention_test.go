package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var testNow = time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC).UnixMilli()

func appendFlushed(t *testing.T, s *Storage, ts int64, v float64) {
	t.Helper()
	s.Append(testLabels("m", "api"), []Sample{{Timestamp: ts, Value: v}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
}

func countSamples(t *testing.T, s *Storage, end int64) int {
	t.Helper()
	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, end)
	total := 0
	for _, sr := range got {
		total += len(sr.Samples)
	}
	return total
}

func TestApplyRetentionDropsExpiredPartitions(t *testing.T) {
	s, _ := Open(t.TempDir())
	appendFlushed(t, s, testNow-10*dayMs, 1) // expired
	appendFlushed(t, s, testNow-3*dayMs, 2)  // kept
	appendFlushed(t, s, testNow, 3)          // kept

	if err := s.ApplyRetention(7, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}

	expiredDay := dayString(testNow - 10*dayMs)
	if _, err := os.Stat(filepath.Join(s.path, partitionsDir, expiredDay)); !os.IsNotExist(err) {
		t.Fatalf("expired partition dir %s still on disk", expiredDay)
	}
	if s.partitions[expiredDay] != nil {
		t.Fatalf("expired partition %s still in map", expiredDay)
	}
	if total := countSamples(t, s, testNow+dayMs); total != 2 {
		t.Fatalf("total samples = %d, want 2 (expired one dropped)", total)
	}
}

// The cutoff is day-granular: the day containing the cutoff instant is
// kept entirely; only strictly earlier days are dropped.
func TestRetentionCutoffBoundary(t *testing.T) {
	s, _ := Open(t.TempDir())
	appendFlushed(t, s, testNow-7*dayMs, 1) // day of the cutoff instant → kept
	appendFlushed(t, s, testNow-8*dayMs, 2) // day before → dropped

	if err := s.ApplyRetention(7, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}
	if s.partitions[dayString(testNow-7*dayMs)] == nil {
		t.Fatalf("cutoff-day partition %s was dropped, want kept", dayString(testNow-7*dayMs))
	}
	if s.partitions[dayString(testNow-8*dayMs)] != nil {
		t.Fatalf("day before cutoff %s kept, want dropped", dayString(testNow-8*dayMs))
	}
	if total := countSamples(t, s, testNow+dayMs); total != 1 {
		t.Fatalf("total samples = %d, want 1", total)
	}
}

func TestRetentionDisabledKeepsEverything(t *testing.T) {
	s, _ := Open(t.TempDir())
	appendFlushed(t, s, testNow-365*dayMs, 1)
	appendFlushed(t, s, testNow, 2)

	if err := s.ApplyRetention(0, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}
	if total := countSamples(t, s, testNow+dayMs); total != 2 {
		t.Fatalf("total samples = %d, want 2 (retention disabled)", total)
	}
}

// A dropped partition stays gone after reopen, and new data written to a
// still-open recent day is unaffected.
func TestRetentionPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	appendFlushed(t, s, testNow-10*dayMs, 1)
	appendFlushed(t, s, testNow, 2)
	if err := s.ApplyRetention(7, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %s", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	if len(s2.partitions) != 1 || s2.partitions[dayString(testNow)] == nil {
		t.Fatalf("partitions after reopen = %v", func() []string {
			var days []string
			for d := range s2.partitions {
				days = append(days, d)
			}
			return days
		}())
	}
	if total := countSamples(t, s2, testNow+dayMs); total != 1 {
		t.Fatalf("total samples after reopen = %d, want 1", total)
	}
}

// Queries racing a retention drop must never error on a deleted dir:
// they either finish their scan before the drop or skip the dropped
// partition. Run with -race.
func TestQueryDuringRetentionDrop(t *testing.T) {
	s, _ := Open(t.TempDir())
	for i := 0; i < 10; i++ {
		appendFlushed(t, s, testNow-int64(i)*dayMs, float64(i))
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, _, err := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, testNow+dayMs); err != nil {
					t.Errorf("QueryRange during retention: %s", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		if err := s.ApplyRetention(5, testNow); err != nil {
			t.Fatalf("ApplyRetention: %s", err)
		}
	}
	wg.Wait()

	if got := len(s.partitions); got != 6 { // cutoff day + 5 newer days
		t.Fatalf("partitions left = %d, want 6", got)
	}
}
