// Package storage implements the marchimetrics time series storage engine:
// samples grouped into day partitions of immutable parts under
// -storageDataPath. It is a deliberately thin re-implementation of the
// VictoriaMetrics lib/storage design — see docs/LEARNING.md for the
// concept mapping and the list of omitted features.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Directory layout under the data path (PLAN.md section 4):
//
//	<path>/
//	  partitions/YYYYMMDD/   day partitions holding immutable parts
//	  series/                global series registry (label set -> SeriesID)
const (
	partitionsDir = "partitions"
	seriesDir     = "series"
	namesFile     = "names.json"
)

// partitionNameRE matches day partition directory names (YYYYMMDD).
var partitionNameRE = regexp.MustCompile(`^\d{8}$`)

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
	// (immediately queryable) until Flush turns them into an immutable
	// disk part. A crash loses at most this buffer — that is the
	// durability window controlled by -inmemoryDataFlushInterval.
	mu  sync.RWMutex
	mem map[uint64][]Sample

	// pmu guards partitions, the opened day partitions.
	pmu        sync.Mutex
	partitions map[string]*partition

	flushMu   sync.Mutex // serializes concurrent Flush calls
	stopCh    chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Open creates the data directory layout at path if needed, reloads the
// series registry and all day partitions, and returns the storage handle.
func Open(path string) (*Storage, error) {
	for _, dir := range []string{partitionsDir, seriesDir} {
		full := filepath.Join(path, dir)
		if err := os.MkdirAll(full, 0o755); err != nil {
			return nil, fmt.Errorf("cannot create data directory %q: %w", full, err)
		}
	}
	s := &Storage{
		path:       path,
		registry:   NewRegistry(),
		mem:        make(map[uint64][]Sample),
		partitions: make(map[string]*partition),
		stopCh:     make(chan struct{}),
	}
	if err := s.loadRegistry(); err != nil {
		return nil, err
	}
	if err := s.loadPartitions(); err != nil {
		return nil, err
	}
	return s, nil
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

// StartFlushLoop flushes the in-memory buffer every interval until Close.
func (s *Storage) StartFlushLoop(interval time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.Flush(); err != nil {
					log.Printf("background flush failed: %s", err)
				}
			case <-s.stopCh:
				return
			}
		}
	}()
}

// Flush atomically swaps the in-memory buffer for a fresh one, persists
// the series registry, and writes the swapped samples as immutable parts
// grouped into day partitions by sample timestamp. On error, unflushed
// samples are returned to the buffer so the next flush retries them.
func (s *Storage) Flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	mem := s.mem
	s.mem = make(map[uint64][]Sample)
	s.mu.Unlock()

	if err := s.saveRegistry(); err != nil {
		s.restoreMem(mem)
		return fmt.Errorf("cannot save series registry: %w", err)
	}
	if len(mem) == 0 {
		return nil
	}

	byDay := make(map[string]map[uint64][]Sample)
	for id, samples := range mem {
		for _, sm := range samples {
			day := dayString(sm.Timestamp)
			m := byDay[day]
			if m == nil {
				m = make(map[uint64][]Sample)
				byDay[day] = m
			}
			m[id] = append(m[id], sm)
		}
	}
	for day, data := range byDay {
		p, err := s.getPartition(day)
		if err != nil {
			s.restoreMem(data)
			return fmt.Errorf("cannot open partition %s: %w", day, err)
		}
		if err := p.addPart(data); err != nil {
			s.restoreMem(data)
			return fmt.Errorf("cannot flush partition %s: %w", day, err)
		}
	}
	return nil
}

// restoreMem puts samples back into the buffer after a failed flush.
func (s *Storage) restoreMem(data map[uint64][]Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, samples := range data {
		s.mem[id] = append(s.mem[id], samples...)
	}
}

// Close stops the flush loop and performs a final flush.
func (s *Storage) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.stopCh)
		s.wg.Wait()
		err = s.Flush()
	})
	return err
}

func (s *Storage) getPartition(day string) (*partition, error) {
	s.pmu.Lock()
	defer s.pmu.Unlock()
	if p := s.partitions[day]; p != nil {
		return p, nil
	}
	p, err := openPartition(filepath.Join(s.path, partitionsDir, day), day)
	if err != nil {
		return nil, err
	}
	s.partitions[day] = p
	return p, nil
}

// partitionsInRange returns opened partitions whose day overlaps
// [startMs, endMs], ordered by day. Day names compare lexicographically.
func (s *Storage) partitionsInRange(startMs, endMs int64) []*partition {
	lo, hi := dayString(startMs), dayString(endMs)
	s.pmu.Lock()
	defer s.pmu.Unlock()
	out := make([]*partition, 0, len(s.partitions))
	for day, p := range s.partitions {
		if day >= lo && day <= hi {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].day < out[j].day })
	return out
}

func (s *Storage) loadRegistry() error {
	path := filepath.Join(s.path, seriesDir, namesFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read %q: %w", path, err)
	}
	var names map[uint64][]Label
	if err := json.Unmarshal(data, &names); err != nil {
		return fmt.Errorf("cannot parse %q: %w", path, err)
	}
	s.registry.Load(names)
	return nil
}

func (s *Storage) saveRegistry() error {
	data, err := json.Marshal(s.registry.Snapshot())
	if err != nil {
		return err
	}
	path := filepath.Join(s.path, seriesDir, namesFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Storage) loadPartitions() error {
	entries, err := os.ReadDir(filepath.Join(s.path, partitionsDir))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || !partitionNameRE.MatchString(e.Name()) {
			continue
		}
		p, err := openPartition(filepath.Join(s.path, partitionsDir, e.Name()), e.Name())
		if err != nil {
			return fmt.Errorf("cannot open partition %q: %w", e.Name(), err)
		}
		s.partitions[e.Name()] = p
	}
	return nil
}
