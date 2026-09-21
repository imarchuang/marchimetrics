package storage

import (
	"path/filepath"
	"sync"
	"testing"
)

// PLAN.md first-week checklist #5: two flushes + force merge; point
// count unchanged.
func TestForceMergePointCountUnchanged(t *testing.T) {
	s, _ := Open(t.TempDir())
	labels := testLabels("m", "api")
	s.Append(labels, []Sample{{Timestamp: 1000, Value: 1}, {Timestamp: 2000, Value: 2}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	s.Append(labels, []Sample{{Timestamp: 3000, Value: 3}, {Timestamp: 4000, Value: 4}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}

	p := s.partitions[dayString(1000)]
	if parts := p.partsSnapshot(); len(parts) != 2 {
		t.Fatalf("parts before merge = %v, want 2 small parts", parts)
	}

	if err := s.ForceMerge(); err != nil {
		t.Fatalf("ForceMerge: %s", err)
	}

	parts := p.partsSnapshot()
	if len(parts) != 1 {
		t.Fatalf("parts after merge = %v, want 1 big part", parts)
	}
	meta, err := readPartMeta(filepath.Join(p.dir, "parts", parts[0]))
	if err != nil {
		t.Fatalf("readPartMeta: %s", err)
	}
	if meta.Tier != tierBig {
		t.Fatalf("merged part tier = %q, want %q", meta.Tier, tierBig)
	}
	if meta.SamplesCount != 4 || meta.SeriesCount != 1 {
		t.Fatalf("merged meta = %+v, want 4 samples in 1 series", meta)
	}

	// Old small part dirs are gone.
	for _, name := range []string{"000001", "000002"} {
		if _, err := readPartMeta(filepath.Join(p.dir, "parts", name)); err == nil {
			t.Fatalf("merged small part %s still on disk", name)
		}
	}

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 10_000)
	if len(got) != 1 || len(got[0].Samples) != 4 {
		t.Fatalf("query after merge = %+v, want 1 series with 4 samples", got)
	}
	for i, sm := range got[0].Samples {
		if sm.Timestamp != int64((i+1)*1000) || sm.Value != float64(i+1) {
			t.Fatalf("sample %d = %+v, want t=%d v=%d", i, sm, (i+1)*1000, i+1)
		}
	}
}

// After each flush, a partition with >= SmallPartsMergeThreshold small
// parts merges them automatically.
func TestAutoMergeAtThreshold(t *testing.T) {
	s, _ := Open(t.TempDir()) // default threshold: 3
	labels := testLabels("m", "api")
	for i := 0; i < 3; i++ {
		s.Append(labels, []Sample{{Timestamp: int64(i * 1000), Value: float64(i)}})
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush %d: %s", i, err)
		}
	}

	p := s.partitions[dayString(0)]
	parts := p.partsSnapshot()
	if len(parts) != 1 {
		t.Fatalf("parts = %v, want auto-merge into 1 big part", parts)
	}
	meta, err := readPartMeta(filepath.Join(p.dir, "parts", parts[0]))
	if err != nil {
		t.Fatalf("readPartMeta: %s", err)
	}
	if meta.Tier != tierBig || meta.SamplesCount != 3 {
		t.Fatalf("meta = %+v, want tier=big samples=3", meta)
	}
}

// Big parts are not re-merged in the MVP: repeated merge cycles
// accumulate big parts.
func TestMergeLeavesBigPartsAlone(t *testing.T) {
	s, _ := Open(t.TempDir())
	labels := testLabels("m", "api")
	for i := 0; i < 6; i++ {
		s.Append(labels, []Sample{{Timestamp: int64(i * 1000), Value: float64(i)}})
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush %d: %s", i, err)
		}
	}

	p := s.partitions[dayString(0)]
	parts := p.partsSnapshot()
	if len(parts) != 2 {
		t.Fatalf("parts = %v, want 2 big parts after two merge cycles", parts)
	}
	for _, name := range parts {
		meta, err := readPartMeta(filepath.Join(p.dir, "parts", name))
		if err != nil {
			t.Fatalf("readPartMeta %s: %s", name, err)
		}
		if meta.Tier != tierBig {
			t.Fatalf("part %s tier = %q, want big", name, meta.Tier)
		}
	}
	// ForceMerge must not touch big parts either.
	if err := s.ForceMerge(); err != nil {
		t.Fatalf("ForceMerge: %s", err)
	}
	if parts := p.partsSnapshot(); len(parts) != 2 {
		t.Fatalf("parts after ForceMerge = %v, want still 2 big parts", parts)
	}
}

// Duplicate (series, timestamp) pairs survive the merge — dedup is a
// documented non-goal.
func TestMergeKeepsDuplicateTimestamps(t *testing.T) {
	s, _ := Open(t.TempDir())
	labels := testLabels("m", "api")
	s.Append(labels, []Sample{{Timestamp: 1000, Value: 1}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	s.Append(labels, []Sample{{Timestamp: 1000, Value: 2}}) // same t, new value
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	if err := s.ForceMerge(); err != nil {
		t.Fatalf("ForceMerge: %s", err)
	}

	got := mustQuery(t, s, []Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 10_000)
	if len(got) != 1 || len(got[0].Samples) != 2 {
		t.Fatalf("merged result = %+v, want both duplicate-timestamp samples", got)
	}
}

// Queries running concurrently with a merge must always see a consistent
// part set — either the old small parts or the new big part — and never
// a deleted file. Run with -race.
func TestQueryDuringMerge(t *testing.T) {
	s, _ := Open(t.TempDir())
	labels := testLabels("m", "api")
	for i := 0; i < 5; i++ {
		s.Append(labels, []Sample{{Timestamp: int64(i * 1000), Value: float64(i)}})
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush %d: %s", i, err)
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				got, _, err := s.QueryRange([]Matcher{{Name: MetricNameLabel, Value: "m"}}, 0, 10_000)
				if err != nil {
					t.Errorf("QueryRange during merge: %s", err)
					return
				}
				if len(got) != 1 || len(got[0].Samples) != 5 {
					t.Errorf("inconsistent view during merge: %+v", got)
					return
				}
			}
		}()
	}
	for m := 0; m < 5; m++ {
		if err := s.ForceMerge(); err != nil {
			t.Fatalf("ForceMerge: %s", err)
		}
	}
	wg.Wait()
}
