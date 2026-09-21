package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteReadPartRoundTrip(t *testing.T) {
	dir := t.TempDir()
	data := map[uint64][]Sample{
		1: {{Timestamp: 3000, Value: 3}, {Timestamp: 1000, Value: 1}}, // unsorted on purpose
		2: {{Timestamp: 2000, Value: 2.5}},
	}
	meta, err := writePart(dir, data)
	if err != nil {
		t.Fatalf("writePart: %s", err)
	}
	if meta.MinTime != 1000 || meta.MaxTime != 3000 {
		t.Fatalf("meta time range = [%d,%d], want [1000,3000]", meta.MinTime, meta.MaxTime)
	}
	if meta.SeriesCount != 2 || meta.SamplesCount != 3 || meta.Tier != tierSmall {
		t.Fatalf("unexpected meta: %+v", meta)
	}

	st := &QueryStats{}
	want := map[uint64]struct{}{1: {}, 2: {}}
	got, err := readPart(dir, want, 0, 10_000, st)
	if err != nil {
		t.Fatalf("readPart: %s", err)
	}
	if len(got) != 2 {
		t.Fatalf("readPart returned %d series, want 2", len(got))
	}
	// Blocks come back time-sorted.
	if len(got[1]) != 2 || got[1][0].Timestamp != 1000 || got[1][1].Timestamp != 3000 {
		t.Fatalf("series 1 samples not sorted: %+v", got[1])
	}
	if len(got[2]) != 1 || got[2][0].Value != 2.5 {
		t.Fatalf("series 2 samples wrong: %+v", got[2])
	}
	// Stats: 1 part, 2 blocks, 3 points decoded.
	if st.PartsScanned != 1 || st.BlocksScanned != 2 || st.PointsScanned != 3 {
		t.Fatalf("stats = %+v, want parts=1 blocks=2 points=3", st)
	}
}

func TestReadPartFiltersByTimeAndSeries(t *testing.T) {
	dir := t.TempDir()
	data := map[uint64][]Sample{
		1: {{Timestamp: 1000, Value: 1}, {Timestamp: 5000, Value: 5}},
		2: {{Timestamp: 2000, Value: 2}},
	}
	if _, err := writePart(dir, data); err != nil {
		t.Fatalf("writePart: %s", err)
	}

	// Series filter: only series 2.
	st := &QueryStats{}
	got, err := readPart(dir, map[uint64]struct{}{2: {}}, 0, 10_000, st)
	if err != nil {
		t.Fatalf("readPart: %s", err)
	}
	if len(got) != 1 || len(got[2]) != 1 {
		t.Fatalf("series filter broken: %+v", got)
	}
	if st.BlocksScanned != 1 {
		t.Fatalf("BlocksScanned = %d, want 1 (unwanted series not decoded)", st.BlocksScanned)
	}

	// Time filter: only the t=5000 sample of series 1 is returned, but
	// the whole block (2 points) is decoded — scanned > returned is the
	// selectivity signal the stats exist for.
	st = &QueryStats{}
	got, err = readPart(dir, map[uint64]struct{}{1: {}}, 4000, 6000, st)
	if err != nil {
		t.Fatalf("readPart: %s", err)
	}
	if len(got[1]) != 1 || got[1][0].Value != 5 {
		t.Fatalf("time filter broken: %+v", got)
	}
	if st.PointsScanned != 2 {
		t.Fatalf("PointsScanned = %d, want 2 (whole block decoded)", st.PointsScanned)
	}

	// No overlap with the part's time range at all.
	st = &QueryStats{}
	got, err = readPart(dir, map[uint64]struct{}{1: {}}, 60_000, 70_000, st)
	if err != nil {
		t.Fatalf("readPart: %s", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no samples outside part range, got %+v", got)
	}
	if st.PartsScanned != 0 {
		t.Fatalf("PartsScanned = %d, want 0 (part skipped via meta.json)", st.PartsScanned)
	}
}

func TestReadPartSkipsMissingOverlap(t *testing.T) {
	dir := t.TempDir()
	if _, err := writePart(dir, map[uint64][]Sample{1: {{Timestamp: 1000, Value: 1}}}); err != nil {
		t.Fatalf("writePart: %s", err)
	}
	got, err := readPart(dir, map[uint64]struct{}{1: {}}, 2000, 3000, &QueryStats{})
	if err != nil {
		t.Fatalf("readPart: %s", err)
	}
	if got != nil {
		t.Fatalf("expected nil for non-overlapping range, got %+v", got)
	}
}

func TestPartFilesExist(t *testing.T) {
	dir := t.TempDir()
	if _, err := writePart(dir, map[uint64][]Sample{1: {{Timestamp: 1000, Value: 1}}}); err != nil {
		t.Fatalf("writePart: %s", err)
	}
	for _, name := range []string{partMetaFile, partSeriesIndex, partTimestampsBin, partValuesBin} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("part file %s missing: %s", name, err)
		}
	}
}
