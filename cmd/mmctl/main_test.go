package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/imarchuang/marchimetrics/storage"
)

// flushOnePart writes one series through the real storage and returns
// the data path and the flushed part dir.
func flushOnePart(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Append([]storage.Label{
		{Name: storage.MetricNameLabel, Value: "http_requests_total"},
		{Name: "job", Value: "api"},
	}, []storage.Sample{
		{Timestamp: 1790000000000, Value: 1.5},
		{Timestamp: 1790000015000, Value: 2.5},
	})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	parts, err := filepath.Glob(filepath.Join(dir, "partitions", "*", "parts", "*"))
	if err != nil || len(parts) != 1 {
		t.Fatalf("parts = %v, err %v", parts, err)
	}
	return dir, parts[0]
}

func TestInspectSummary(t *testing.T) {
	root, partDir := flushOnePart(t)
	var buf bytes.Buffer
	if err := runInspect(&buf, []string{"-storageDataPath", root, partDir}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"encoding: gorilla",
		"series: 1  samples: 2",
		`http_requests_total{job="api"}`,
		"(2 samples)",
		"first:", "last:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestInspectSamples(t *testing.T) {
	root, partDir := flushOnePart(t)
	var buf bytes.Buffer
	if err := runInspect(&buf, []string{"-storageDataPath", root, "-samples", partDir}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "1.5") || !strings.Contains(out, "2.5") {
		t.Errorf("samples not decoded:\n%s", out)
	}
	if !strings.Contains(out, "2025-") && !strings.Contains(out, "2026-") {
		t.Errorf("timestamps not rendered:\n%s", out)
	}
}

func TestInspectSeriesFilter(t *testing.T) {
	root, partDir := flushOnePart(t)
	var buf bytes.Buffer
	if err := runInspect(&buf, []string{"-storageDataPath", root, "-series", "999", partDir}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "http_requests_total") {
		t.Errorf("series filter did not exclude series:\n%s", buf.String())
	}
}
