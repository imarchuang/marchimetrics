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

// PR-B: a series with data in two days stays findable by label after
// the old day expires — only that day's data disappears.
func TestRetentionKeepsSeriesAliveInRemainingDays(t *testing.T) {
	s, _ := Open(t.TempDir())
	labels := []Label{{Name: MetricNameLabel, Value: "m"}, {Name: "job", Value: "api"}}
	s.Append(labels, []Sample{{Timestamp: testNow - 10*dayMs, Value: 1}})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	s.Append(labels, []Sample{{Timestamp: testNow, Value: 2}})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := s.ApplyRetention(7, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}

	// Still findable by label, via the surviving day's inverted index.
	ids := s.Registry().Match([]Matcher{{Name: "job", Value: "api"}})
	if len(ids) != 1 {
		t.Fatalf("match after retention = %v, want 1 series", ids)
	}
	// Only the recent sample remains.
	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, testNow+dayMs)
	if len(got) != 1 || len(got[0].Samples) != 1 || got[0].Samples[0].Value != 2 {
		t.Fatalf("query after retention = %+v, want the day-0 sample only", got)
	}
	// The expired day's inverted dir is gone from disk.
	oldDay := dayString(testNow - 10*dayMs)
	if _, err := os.Stat(filepath.Join(s.path, seriesDir, invertedDir, oldDay)); !os.IsNotExist(err) {
		t.Fatalf("inverted dir for %s still on disk", oldDay)
	}
}

// PR-B: a series seen only in an expired day becomes unfindable by
// label, but its identity (Labels) survives — the forward registry never
// expires.
func TestRetentionForgetsOldOnlySeries(t *testing.T) {
	s, _ := Open(t.TempDir())
	old := []Label{{Name: MetricNameLabel, Value: "old_metric"}, {Name: "job", Value: "api"}}
	idOld := s.Append(old, []Sample{{Timestamp: testNow - 10*dayMs, Value: 1}})
	s.Append([]Label{{Name: MetricNameLabel, Value: "new_metric"}, {Name: "job", Value: "api"}},
		[]Sample{{Timestamp: testNow, Value: 2}})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := s.ApplyRetention(7, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}

	// Unfindable by label...
	if ids := s.Registry().Match([]Matcher{{Name: MetricNameLabel, Value: "old_metric"}}); len(ids) != 0 {
		t.Fatalf("expired-only series still matches: %v", ids)
	}
	// ...but its identity is preserved.
	if labels, ok := s.Registry().Labels(idOld); !ok || CanonicalKey(labels) != CanonicalKey(old) {
		t.Fatalf("identity of expired series lost: ok=%v labels=%v", ok, labels)
	}
	// And the surviving series is unaffected.
	if ids := s.Registry().Match([]Matcher{{Name: MetricNameLabel, Value: "new_metric"}}); len(ids) != 1 {
		t.Fatalf("surviving series no longer matches: %v", ids)
	}
}

// PR-B: a series active in the same day across many flushes is persisted
// into that day's inverted index exactly once.
func TestInvertedIndexDedupesWithinDay(t *testing.T) {
	s, _ := Open(t.TempDir())
	labels := []Label{{Name: MetricNameLabel, Value: "m"}}
	for i := 0; i < 3; i++ {
		s.Append(labels, []Sample{{Timestamp: testNow, Value: float64(i)}})
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	day := dayString(testNow)
	segs, err := listIndexSegments(filepath.Join(s.path, seriesDir, invertedDir, day))
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("inverted segments for one day = %v, want exactly 1 (deduped)", segs)
	}
}

// PR-B: the inverted index is rebuilt from per-day segments at open, and
// a retention drop survives another restart.
func TestInvertedIndexRebuiltAtOpen(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	appendFlushed(t, s, testNow-10*dayMs, 1)
	appendFlushed(t, s, testNow, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: both days' series findable again.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	if total := countSamples(t, s2, testNow+dayMs); total != 2 {
		t.Fatalf("samples after reopen = %d, want 2", total)
	}

	// Retention after reopen drops the old day and its index...
	if err := s2.ApplyRetention(7, testNow); err != nil {
		t.Fatalf("ApplyRetention: %s", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// ...and the drop persists across yet another restart.
	s3, err := Open(dir)
	if err != nil {
		t.Fatalf("second reopen: %s", err)
	}
	if total := countSamples(t, s3, testNow+dayMs); total != 1 {
		t.Fatalf("samples after second reopen = %d, want 1", total)
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
