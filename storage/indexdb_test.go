package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// replayAll reads every segment in dir into one map.
func replayAll(t *testing.T, dir string) map[uint64][]Label {
	t.Helper()
	names, err := listIndexSegments(dir)
	if err != nil {
		t.Fatalf("listIndexSegments: %s", err)
	}
	out := make(map[uint64][]Label)
	for _, n := range names {
		err := readIndexSegment(filepath.Join(dir, n), func(id uint64, labels []Label) error {
			out[id] = labels
			return nil
		})
		if err != nil {
			t.Fatalf("readIndexSegment %s: %s", n, err)
		}
	}
	return out
}

func TestIndexSegmentRoundTrip(t *testing.T) {
	dir := t.TempDir()
	entries := map[uint64][]Label{
		1: {{Name: MetricNameLabel, Value: "m"}, {Name: "job", Value: "api"}},
		2: {{Name: MetricNameLabel, Value: "m"}, {Name: "job", Value: "web"}},
		5: {{Name: MetricNameLabel, Value: "node_cpu"}, {Name: "mode", Value: "idle"}},
	}
	name, err := appendIndexSegment(dir, entries)
	if err != nil {
		t.Fatalf("appendIndexSegment: %s", err)
	}
	if name != "000001.seg" {
		t.Fatalf("segment name = %q, want 000001.seg", name)
	}

	got := replayAll(t, dir)
	if len(got) != 3 {
		t.Fatalf("replayed %d entries, want 3", len(got))
	}
	for id, want := range entries {
		if CanonicalKey(got[id]) != CanonicalKey(want) {
			t.Errorf("id %d: got %v, want %v", id, got[id], want)
		}
	}
}

// Each flush appends a new segment; nothing is rewritten.
func TestIndexSegmentsAreAppendOnly(t *testing.T) {
	dir := t.TempDir()
	if _, err := appendIndexSegment(dir, map[uint64][]Label{1: {{Name: MetricNameLabel, Value: "a"}}}); err != nil {
		t.Fatal(err)
	}
	n2, err := appendIndexSegment(dir, map[uint64][]Label{2: {{Name: MetricNameLabel, Value: "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if n2 != "000002.seg" {
		t.Fatalf("second segment = %q, want 000002.seg", n2)
	}
	got := replayAll(t, dir)
	if len(got) != 2 {
		t.Fatalf("replayed %d entries across 2 segments, want 2", len(got))
	}
}

// A torn tail record (crash between segment rename and the data part it
// accompanied) is tolerated: replay stops at the last complete record.
func TestIndexSegmentToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	name, err := appendIndexSegment(dir, map[uint64][]Label{
		1: {{Name: MetricNameLabel, Value: "a"}},
		2: {{Name: MetricNameLabel, Value: "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Chop the file mid-record: keep the first record plus a partial second.
	truncated := data[:len(data)-3]
	if err := os.WriteFile(path, truncated, 0o644); err != nil {
		t.Fatal(err)
	}

	var got []uint64
	err = readIndexSegment(path, func(id uint64, labels []Label) error {
		got = append(got, id)
		return nil
	})
	if err != nil {
		t.Fatalf("readIndexSegment with torn tail: %s", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("replayed ids = %v, want [1] (torn record dropped)", got)
	}
}

// Leftover .tmp from a crashed publish is removed and ignored.
func TestIndexSegmentIgnoresTmp(t *testing.T) {
	dir := t.TempDir()
	if _, err := appendIndexSegment(dir, map[uint64][]Label{1: {{Name: MetricNameLabel, Value: "a"}}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000002.seg.tmp"), []byte("half-written"), 0o644); err != nil {
		t.Fatal(err)
	}
	names, err := listIndexSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "000001.seg" {
		t.Fatalf("segments = %v, want [000001.seg]", names)
	}
	if _, err := os.Stat(filepath.Join(dir, "000002.seg.tmp")); !os.IsNotExist(err) {
		t.Fatalf("stale .tmp not cleaned up")
	}
}

// End to end through Storage: series survive a reopen, and there is no
// names.json anymore.
func TestRegistryPersistsViaIndexDB(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	id := s.Append([]Label{{Name: MetricNameLabel, Value: "m"}, {Name: "job", Value: "api"}},
		[]Sample{{Timestamp: 1000, Value: 1}})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %s", err)
	}

	// names.json is gone; the indexdb segment is the source of truth.
	if _, err := os.Stat(filepath.Join(dir, seriesDir, "names.json")); !os.IsNotExist(err) {
		t.Fatalf("names.json still exists")
	}
	segs, err := listIndexSegments(filepath.Join(dir, seriesDir, indexdbDir))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no indexdb segments: %v %v", segs, err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	got := s2.Registry().Resolve([]Label{{Name: "job", Value: "api"}, {Name: MetricNameLabel, Value: "m"}})
	if got != id {
		t.Fatalf("after reopen, series resolved to %d, want %d", got, id)
	}
	// Inverted index rebuilt from segments.
	if ids := s2.Registry().Match([]Matcher{{Name: "job", Value: "api"}}); len(ids) != 1 || ids[0] != id {
		t.Fatalf("match after reopen = %v, want [%d]", ids, id)
	}
	// New IDs continue past the reloaded maximum.
	if id3 := s2.Registry().Resolve([]Label{{Name: MetricNameLabel, Value: "new"}}); id3 <= id {
		t.Fatalf("new ID %d after reopen, want > %d", id3, id)
	}
}
