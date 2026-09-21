package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// publishingPrefix marks staged part directories that are not yet
// atomically renamed into place (VM uses the same .publishing- scheme).
const publishingPrefix = ".publishing-"

// partition is one day of data: partitions/YYYYMMDD/ holding immutable
// parts plus manifest.json (PLAN.md section 4).
type partition struct {
	day string
	dir string

	mu     sync.Mutex
	parts  []string // live part names; mirrors manifest.json
	nextID int
}

// dayString maps a millisecond timestamp to its UTC day partition name.
func dayString(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("20060102")
}

// openPartition loads an existing partition directory (creating it if
// needed), removes stale .publishing-* staging dirs left by a crash, and
// restores the manifest and next part ID.
func openPartition(dir, day string) (*partition, error) {
	partsDir := filepath.Join(dir, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create parts dir: %w", err)
	}
	entries, err := os.ReadDir(partsDir)
	if err != nil {
		return nil, err
	}
	maxID := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), publishingPrefix) {
			os.RemoveAll(filepath.Join(partsDir, e.Name()))
			continue
		}
		if id, err := strconv.Atoi(e.Name()); err == nil && id > maxID {
			maxID = id
		}
	}
	m, err := loadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	return &partition{day: day, dir: dir, parts: m.Parts, nextID: maxID + 1}, nil
}

// addPart writes data as a new immutable part: staged into a
// .publishing-* directory, atomically renamed into place, then appended
// to manifest.json. A crash between rename and manifest save leaves an
// orphan dir that the manifest (source of truth) hides from queries.
func (p *partition) addPart(data map[uint64][]Sample) error {
	p.mu.Lock()
	id := p.nextID
	p.nextID++
	p.mu.Unlock()

	name := fmt.Sprintf("%06d", id)
	staging := filepath.Join(p.dir, "parts", publishingPrefix+name)
	final := filepath.Join(p.dir, "parts", name)
	if _, err := writePart(staging, data); err != nil {
		os.RemoveAll(staging)
		return err
	}
	if err := os.Rename(staging, final); err != nil {
		os.RemoveAll(staging)
		return fmt.Errorf("cannot publish part %q: %w", name, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.parts = append(p.parts, name)
	m := &Manifest{Parts: append([]string(nil), p.parts...)}
	if err := m.save(filepath.Join(p.dir, "manifest.json")); err != nil {
		return fmt.Errorf("cannot update manifest: %w", err)
	}
	return nil
}

// partsSnapshot returns the currently live part names.
func (p *partition) partsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.parts...)
}
