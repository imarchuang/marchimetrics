package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

const dayMs = int64(86_400_000)

// StartRetentionLoop drops expired day partitions once at startup and
// then every interval until Close. Retention is disabled when
// RetentionDays <= 0.
func (s *Storage) StartRetentionLoop(interval time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runRetentionPass()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.runRetentionPass()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *Storage) runRetentionPass() {
	if err := s.ApplyRetention(s.RetentionDays, time.Now().UnixMilli()); err != nil {
		log.Printf("retention pass failed: %s", err)
	}
}

// ApplyRetention drops day partitions whose entire day predates the
// retention cutoff: a partition YYYYMMDD is dropped when
// day < dayString(nowMs - retentionDays days). Retention is therefore
// day-granular with up to ~24h of slack — the same trade-off VM makes
// with month-sized partitions. retentionDays <= 0 keeps everything.
//
// nowMs is a parameter so tests control the clock.
//
// Lock ordering: flushMu → compactMu → p.mu. Holding flushMu and
// compactMu means no flush or merge can be touching the partition while
// it is being dropped; queries holding the partition read lock finish
// their scan first, and queries arriving afterwards see dropped=true and
// skip — expired data is legitimately gone, so skipping is correct.
func (s *Storage) ApplyRetention(retentionDays int, nowMs int64) error {
	if retentionDays <= 0 {
		return nil
	}
	cutoff := dayString(nowMs - int64(retentionDays)*dayMs)

	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	s.pmu.Lock()
	var expired []*partition
	for day, p := range s.partitions {
		if day < cutoff {
			expired = append(expired, p)
		}
	}
	s.pmu.Unlock()

	for _, p := range expired {
		p.mu.Lock()
		p.dropped = true
		err := os.RemoveAll(p.dir)
		s.pmu.Lock()
		delete(s.partitions, p.day)
		s.pmu.Unlock()
		p.mu.Unlock()
		if err != nil {
			return fmt.Errorf("cannot drop partition %s: %w", p.day, err)
		}
		// The day's inverted index expires with its data: series only
		// ever seen that day become unfindable by label (their identity
		// in the forward registry is kept — LEARNING.md "Index
		// retention").
		if ierr := os.RemoveAll(filepath.Join(s.path, seriesDir, invertedDir, p.day)); ierr != nil {
			return fmt.Errorf("cannot drop inverted index for %s: %w", p.day, ierr)
		}
		s.registry.DropDay(p.day)
		log.Printf("dropped partition %s and its inverted index (retention %dd, cutoff day %s)", p.day, retentionDays, cutoff)
	}
	return nil
}
