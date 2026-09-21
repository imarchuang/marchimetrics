package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPartitionAddPartAndManifest(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, "20260921")
	if err != nil {
		t.Fatalf("openPartition: %s", err)
	}

	if err := p.addPart(map[uint64][]Sample{1: {{Timestamp: 1000, Value: 1}}}); err != nil {
		t.Fatalf("addPart: %s", err)
	}
	if err := p.addPart(map[uint64][]Sample{2: {{Timestamp: 2000, Value: 2}}}); err != nil {
		t.Fatalf("addPart: %s", err)
	}

	parts := p.partsSnapshot()
	if len(parts) != 2 || parts[0] != "000001" || parts[1] != "000002" {
		t.Fatalf("parts = %v, want [000001 000002]", parts)
	}

	// Manifest on disk mirrors the in-memory list.
	m, err := loadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("loadManifest: %s", err)
	}
	if len(m.Parts) != 2 || m.Parts[0] != "000001" {
		t.Fatalf("manifest = %+v", m)
	}

	// No staging dirs left behind.
	entries, _ := os.ReadDir(filepath.Join(dir, "parts"))
	for _, e := range entries {
		if len(e.Name()) > 0 && e.Name()[0] == '.' {
			t.Fatalf("stale staging dir left behind: %s", e.Name())
		}
	}
}

func TestOpenPartitionRecoversAndCleansStaging(t *testing.T) {
	dir := t.TempDir()
	p, _ := openPartition(dir, "20260921")
	if err := p.addPart(map[uint64][]Sample{1: {{Timestamp: 1000, Value: 1}}}); err != nil {
		t.Fatalf("addPart: %s", err)
	}

	// Simulate a crash mid-publish: a staging dir that was never renamed.
	stale := filepath.Join(dir, "parts", publishingPrefix+"000099")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}

	p2, err := openPartition(dir, "20260921")
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale publishing dir not cleaned: %s", stale)
	}
	if parts := p2.partsSnapshot(); len(parts) != 1 || parts[0] != "000001" {
		t.Fatalf("recovered parts = %v, want [000001]", parts)
	}

	// IDs continue past the maximum existing part, never reuse.
	if err := p2.addPart(map[uint64][]Sample{2: {{Timestamp: 2000, Value: 2}}}); err != nil {
		t.Fatalf("addPart after reopen: %s", err)
	}
	parts := p2.partsSnapshot()
	if parts[len(parts)-1] != "000002" {
		t.Fatalf("part ID reused after reopen: %v", parts)
	}
}

func TestDayStringUTC(t *testing.T) {
	// 2026-09-21T00:00:00Z exactly, and one millisecond before.
	boundary := int64(1789948800000)
	if got := dayString(boundary); got != "20260921" {
		t.Fatalf("dayString(%d) = %s, want 20260921", boundary, got)
	}
	if got := dayString(boundary - 1); got != "20260920" {
		t.Fatalf("dayString(%d) = %s, want 20260920", boundary-1, got)
	}
}
