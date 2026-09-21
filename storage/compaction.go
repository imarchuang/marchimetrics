package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Compaction merges a partition's small parts into one tier:big part —
// the LSM small→big rung (VM: lib/storage/merge.go). The MVP policy is
// deliberately simple: after each flush, a partition with at least
// SmallPartsMergeThreshold small parts merges ALL of them into one big
// part. Big parts are not re-merged (VM does; noted as a follow-up).

// ForceMerge merges every partition that has at least two small parts,
// regardless of the configured threshold.
func (s *Storage) ForceMerge() error {
	s.pmu.Lock()
	ps := make([]*partition, 0, len(s.partitions))
	for _, p := range s.partitions {
		ps = append(ps, p)
	}
	s.pmu.Unlock()
	for _, p := range ps {
		if err := s.compactPartition(p, 2); err != nil {
			return fmt.Errorf("cannot merge partition %s: %w", p.day, err)
		}
	}
	return nil
}

// maybeCompact merges p's small parts when their count reaches the
// configured threshold. Called after a flush lands a new small part.
func (s *Storage) maybeCompact(p *partition) error {
	return s.compactPartition(p, s.SmallPartsMergeThreshold)
}

// compactPartition merges p's small parts if there are at least min of
// them. compactMu serializes all merges so two compactions can never
// race over the same parts.
func (s *Storage) compactPartition(p *partition, min int) error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	small, err := p.smallParts()
	if err != nil {
		return err
	}
	if len(small) < min {
		return nil
	}
	return p.mergeParts(small)
}

// smallParts returns the names of p's parts with tier "small".
func (p *partition) smallParts() ([]string, error) {
	p.mu.RLock()
	names := append([]string(nil), p.parts...)
	p.mu.RUnlock()

	var small []string
	for _, name := range names {
		meta, err := readPartMeta(filepath.Join(p.dir, "parts", name))
		if err != nil {
			return nil, fmt.Errorf("cannot read meta of part %s: %w", name, err)
		}
		if meta.Tier == tierSmall {
			small = append(small, name)
		}
	}
	return small, nil
}

// mergeParts merges the named parts of p into a single tier:big part and
// atomically swaps the manifest. Samples of the same series are
// concatenated and time-sorted; duplicate (series, timestamp) pairs are
// kept as-is — dedup is a non-goal (LEARNING.md).
//
// Concurrency: the read+write phase runs lock-free (parts are immutable;
// the staging dir is invisible). The swap phase holds the partition write
// lock: rename, manifest update and old-dir deletion are one critical
// section, so a query holding the read lock always sees either the old
// small parts or the new big part — never a mix, never a deleted file.
func (p *partition) mergeParts(names []string) error {
	merged := make(map[uint64][]Sample)
	for _, name := range names {
		data, err := readPartAll(filepath.Join(p.dir, "parts", name))
		if err != nil {
			return fmt.Errorf("cannot read part %s: %w", name, err)
		}
		for id, samples := range data {
			merged[id] = append(merged[id], samples...)
		}
	}

	p.mu.Lock()
	id := p.nextID
	p.nextID++
	p.mu.Unlock()

	name := fmt.Sprintf("%06d", id)
	staging := filepath.Join(p.dir, "parts", publishingPrefix+name)
	final := filepath.Join(p.dir, "parts", name)
	if _, err := writePart(staging, merged, tierBig); err != nil {
		os.RemoveAll(staging)
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := os.Rename(staging, final); err != nil {
		os.RemoveAll(staging)
		return fmt.Errorf("cannot publish merged part %q: %w", name, err)
	}

	removed := make(map[string]struct{}, len(names))
	for _, n := range names {
		removed[n] = struct{}{}
	}
	kept := make([]string, 0, len(p.parts)-len(names)+1)
	for _, n := range p.parts {
		if _, ok := removed[n]; !ok {
			kept = append(kept, n)
		}
	}
	kept = append(kept, name)
	sort.Strings(kept)
	if err := (&Manifest{Parts: kept}).save(filepath.Join(p.dir, "manifest.json")); err != nil {
		// The big part is already renamed in; if the manifest save fails
		// it stays an ignored orphan and the small parts remain live —
		// reads stay correct, disk is wasted until retention (PR5).
		return fmt.Errorf("cannot update manifest: %w", err)
	}
	p.parts = kept

	// Delete merged dirs only after the manifest swap, still under the
	// write lock: no reader can hold references to them.
	for _, n := range names {
		os.RemoveAll(filepath.Join(p.dir, "parts", n))
	}
	return nil
}
