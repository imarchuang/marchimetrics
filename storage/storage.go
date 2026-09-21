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

// Storage is the root handle for the on-disk time series database.
type Storage struct {
	path string
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
	return &Storage{path: path}, nil
}

// Path returns the root data path passed to Open.
func (s *Storage) Path() string { return s.path }

// Close flushes pending in-memory data and releases resources.
// PR0 has no in-memory buffers yet, so it is a no-op.
func (s *Storage) Close() error { return nil }
