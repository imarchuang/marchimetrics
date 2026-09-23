// Package storage implements the marchimetrics time series storage engine:
// samples grouped into day partitions of immutable parts under
// -storageDataPath. It is a deliberately thin re-implementation of the
// VictoriaMetrics lib/storage design — see docs/LEARNING.md for the
// concept mapping and the list of omitted features.
package storage

import (
	"fmt"
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
//	  series/indexdb/        append-only registry segments (label set -> SeriesID)
const (
	partitionsDir = "partitions"
	seriesDir     = "series"
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

	// SmallPartsMergeThreshold is the number of small parts in one
	// partition that triggers a merge into a tier:big part after a
	// flush. Set before StartFlushLoop. Default 3.
	SmallPartsMergeThreshold int

	// RetentionDays: partitions whose entire day is older than this are
	// dropped by the retention loop. 0 (the default) keeps data forever.
	// Set before StartRetentionLoop.
	RetentionDays int

	flushMu   sync.Mutex // serializes concurrent Flush calls
	compactMu sync.Mutex // serializes all part merges
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
		path:                     path,
		registry:                 NewRegistry(),
		mem:                      make(map[uint64][]Sample),
		partitions:               make(map[string]*partition),
		SmallPartsMergeThreshold: 3,
		stopCh:                   make(chan struct{}),
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
// the newly registered series as one indexdb segment, and writes the
// swapped samples as immutable parts grouped into day partitions by
// sample timestamp. On error, unflushed samples and pending registry
// entries are returned so the next flush retries them.
func (s *Storage) Flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	mem := s.mem
	s.mem = make(map[uint64][]Sample)
	s.mu.Unlock()

	// Persist only the series registered since the last flush — the
	// append-only replacement for rewriting names.json. Done before the
	// data parts so a crash never leaves a part pointing at a SeriesID
	// the registry has never heard of.
	pending := s.registry.DrainPending()
	if err := s.saveRegistrySegment(pending); err != nil {
		s.registry.TrackPending(pending)
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
		// The flush succeeded and the data is durable; a failed merge is
		// not a flush failure, so it is only logged.
		if err := s.maybeCompact(p); err != nil {
			log.Printf("cannot compact partition %s after flush: %s", day, err)
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

// loadRegistry replays every indexdb segment in order into the registry.
func (s *Storage) loadRegistry() error {
	dir := filepath.Join(s.path, seriesDir, indexdbDir)
	names, err := listIndexSegments(dir)
	if err != nil {
		return fmt.Errorf("cannot list indexdb segments: %w", err)
	}
	for _, name := range names {
		path := filepath.Join(dir, name)
		err := readIndexSegment(path, func(id uint64, labels []Label) error {
			s.registry.LoadEntries(map[uint64][]Label{id: labels})
			return nil
		})
		if err != nil {
			return fmt.Errorf("cannot replay %q: %w", path, err)
		}
	}
	return nil
}

// saveRegistrySegment appends pending registry entries as one new
// indexdb segment. A no-op when nothing was registered since last flush.
func (s *Storage) saveRegistrySegment(pending map[uint64][]Label) error {
	dir := filepath.Join(s.path, seriesDir, indexdbDir)
	_, err := appendIndexSegment(dir, pending)
	return err
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
