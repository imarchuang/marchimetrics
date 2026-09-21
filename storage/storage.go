// Package storage implements the marchimetrics time series storage engine:
// samples grouped into day partitions of immutable parts under
// -storageDataPath. It is a deliberately thin re-implementation of the
// VictoriaMetrics lib/storage design — see docs/LEARNING.md for the
// concept mapping and the list of omitted features.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Directory layout under the data path (PLAN.md section 4):
//
//	<path>/
//	  partitions/YYYYMMDD/   day partitions holding immutable parts
//	  series/                global series registry (label set -> SeriesID)
const (
	partitionsDir = "partitions"
	seriesDir     = "series"
)

// Sample is one (timestamp, value) point of a series.
// Timestamp is milliseconds since the Unix epoch, as in VictoriaMetrics.
type Sample struct {
	Timestamp int64
	Value     float64
}

// Storage is the root handle for the on-disk time series database.
type Storage struct {
	path     string
	registry *Registry

	// mu guards mem, the in-memory append buffer. Samples sit here
	// (immediately queryable) until a flush turns them into an immutable
	// disk part (PR2). A crash loses at most this buffer — that is the
	// durability window controlled by -inmemoryDataFlushInterval.
	mu  sync.RWMutex
	mem map[uint64][]Sample
}

// Open creates the data directory layout at path if needed and returns
// the storage handle. Existing data is left untouched.
func Open(path string) (*Storage, error) {
	for _, dir := range []string{partitionsDir, seriesDir} {
		full := filepath.Join(path, dir)
		if err := os.MkdirAll(full, 0o755); err != nil {
			return nil, fmt.Errorf("cannot create data directory %q: %w", full, err)
		}
	}
	return &Storage{
		path:     path,
		registry: NewRegistry(),
		mem:      make(map[uint64][]Sample),
	}, nil
}

// Path returns the root data path passed to Open.
func (s *Storage) Path() string { return s.path }

// Registry exposes the series registry (used by HTTP handlers and tests).
func (s *Storage) Registry() *Registry { return s.registry }

// Append resolves the label set to a SeriesID and buffers the samples in
// memory, where they are immediately visible to QueryRange. It returns
// the SeriesID, or 0 when no samples were given.
func (s *Storage) Append(labels []Label, samples []Sample) uint64 {
	if len(samples) == 0 {
		return 0
	}
	id := s.registry.Resolve(labels)
	s.mu.Lock()
	s.mem[id] = append(s.mem[id], samples...)
	s.mu.Unlock()
	return id
}

// Close flushes pending in-memory data and releases resources.
// There is nothing to flush until PR2, so it is a no-op.
func (s *Storage) Close() error { return nil }
