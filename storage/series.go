package storage

import (
	"sort"
	"strings"
	"sync"
)

// Label is a single Prometheus-style name/value pair.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// MetricNameLabel is the reserved label that holds the metric name —
// the same model Prometheus and VictoriaMetrics use internally.
const MetricNameLabel = "__name__"

// Separators for canonical keys and inverted-index keys, chosen from the
// control range so they cannot collide with typical label text. Label
// values containing these exact bytes would break uniqueness — accepted
// for the MVP (docs/TSID.md).
const (
	nameValueSep = '\x01'
	labelPairSep = '\x02'
)

// sortedLabels returns a copy of labels sorted by (Name, Value).
func sortedLabels(labels []Label) []Label {
	out := make([]Label, len(labels))
	copy(out, labels)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Value < out[j].Value
	})
	return out
}

func labelKey(name, value string) string {
	return name + string(nameValueSep) + value
}

// CanonicalKey returns the unique string form of a label set: labels
// sorted by (name, value) and joined with control-byte separators.
// Two label sets identify the same series iff their canonical keys are
// equal — see docs/TSID.md.
func CanonicalKey(labels []Label) string {
	var b strings.Builder
	for _, l := range sortedLabels(labels) {
		b.WriteString(l.Name)
		b.WriteByte(nameValueSep)
		b.WriteString(l.Value)
		b.WriteByte(labelPairSep)
	}
	return b.String()
}

// Registry maps label sets to SeriesIDs and back. Persistence is
// append-only and split in two (LEARNING.md "Index retention"):
//
//   - forward (SeriesID -> labels): global, permanent — series/indexdb/
//     segments, appended once per newly registered series at flush.
//   - inverted (label -> SeriesID): day-partitioned — series/inverted/
//     YYYYMMDD/ segments, written at flush for every series that has
//     data in that day, and dropped together with the day's data
//     partition by retention. A series is findable by label exactly for
//     the days whose data still exists; its identity (Labels(id)) never
//     expires.
type Registry struct {
	mu      sync.RWMutex
	nextID  uint64
	byKey   map[string]uint64  // canonical key -> SeriesID
	names   map[uint64][]Label // SeriesID -> sorted label set
	pending map[uint64][]Label // registered since last DrainPending

	// invertedByDay is the queryable inverted index: day -> labelKey ->
	// SeriesID set. Match unions the per-day sets. persisted tracks which
	// (day, SeriesID) pairs are durably on disk, so a long-lived series
	// is re-indexed into each new day but not rewritten into a day it is
	// already persisted in. (In-memory membership in invertedByDay is
	// separate: Append adds entries there immediately so unflushed data
	// is findable.)
	invertedByDay map[string]map[string]map[uint64]struct{}
	persisted     map[string]map[uint64]struct{}
}

// NewRegistry returns an empty registry. IDs start at 1 so that 0 can
// mean "no series" in later code.
func NewRegistry() *Registry {
	return &Registry{
		nextID:        1,
		byKey:         make(map[string]uint64),
		names:         make(map[uint64][]Label),
		pending:       make(map[uint64][]Label),
		invertedByDay: make(map[string]map[string]map[uint64]struct{}),
		persisted:     make(map[string]map[uint64]struct{}),
	}
}

// Resolve returns the SeriesID for a label set, registering it on first
// sight. The same label set always yields the same ID; collisions are
// impossible because identity is decided by full string equality
// (docs/TSID.md section 3).
func (r *Registry) Resolve(labels []Label) uint64 {
	key := CanonicalKey(labels)

	r.mu.RLock()
	id, ok := r.byKey[key]
	r.mu.RUnlock()
	if ok {
		return id
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.byKey[key]; ok { // registered while we upgraded the lock
		return id
	}
	id = r.nextID
	r.nextID++
	r.byKey[key] = id
	sorted := sortedLabels(labels)
	r.names[id] = sorted
	r.pending[id] = sorted
	return id
}

// Labels returns the sorted label set registered for id.
func (r *Registry) Labels(id uint64) ([]Label, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	labels, ok := r.names[id]
	return labels, ok
}

// Len reports how many series are registered.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.names)
}

// DrainPending returns the series registered since the last DrainPending
// call and clears the pending set. Flush persists exactly this delta as
// one indexdb segment — the append-only replacement for rewriting all of
// names.json every flush.
func (r *Registry) DrainPending() map[uint64][]Label {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return nil
	}
	out := r.pending
	r.pending = make(map[uint64][]Label)
	return out
}

// TrackPending returns entries to the pending set after a failed flush,
// so the next flush retries them. Entries already known to the registry
// are kept pending (idempotent — re-appending the same ID to the indexdb
// is harmless because replay is idempotent).
func (r *Registry) TrackPending(entries map[uint64][]Label) {
	if len(entries) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, labels := range entries {
		r.pending[id] = labels
	}
}

// LoadEntries replays forward indexdb records (SeriesID -> label set),
// rebuilding the canonical index and continuing IDs past the maximum
// seen. It is idempotent: replaying the same record twice converges to
// the same state, which is what makes a torn final segment harmless.
// The inverted index is NOT derived here — it is rebuilt separately from
// the per-day inverted segments (LoadInvertedEntry), so that expired
// days stay forgotten across a restart.
func (r *Registry) LoadEntries(entries map[uint64][]Label) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var maxID uint64
	for id, labels := range entries {
		sorted := sortedLabels(labels)
		key := CanonicalKey(sorted)
		if existing, ok := r.byKey[key]; ok && existing == id {
			continue // replay of an already-known series
		}
		r.names[id] = sorted
		r.byKey[key] = id
		if id > maxID {
			maxID = id
		}
	}
	if maxID >= r.nextID {
		r.nextID = maxID + 1
	}
}

// IndexForDay adds id to day's in-memory inverted index (using the
// registered labels) without marking it persisted. Append uses this to
// keep the mem buffer findable; Flush later persists the delta via
// UnindexedIDs + MarkIndexed.
func (r *Registry) IndexForDay(day string, id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addInvertedLocked(day, id, r.names[id])
}

// UnindexedIDs returns the subset of ids not yet persisted in day's
// inverted index. Flush writes exactly these as an inverted segment.
func (r *Registry) UnindexedIDs(day string, ids []uint64) []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	done := r.persisted[day]
	var out []uint64
	for _, id := range ids {
		if _, ok := done[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// MarkIndexed records that ids are now persisted in day's inverted
// index. Called only after the segment write succeeded. (Append already
// made them visible in memory.)
func (r *Registry) MarkIndexed(day string, ids []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		r.markPersistedLocked(day, id)
	}
}

// LoadInvertedEntry replays one inverted segment record at open: the
// entry is both visible in memory and known to be on disk.
func (r *Registry) LoadInvertedEntry(day string, id uint64, labels []Label) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addInvertedLocked(day, id, labels)
	r.markPersistedLocked(day, id)
}

// addInvertedLocked updates the in-memory inverted index only.
func (r *Registry) addInvertedLocked(day string, id uint64, labels []Label) {
	inv := r.invertedByDay[day]
	if inv == nil {
		inv = make(map[string]map[uint64]struct{})
		r.invertedByDay[day] = inv
	}
	for _, l := range labels {
		k := labelKey(l.Name, l.Value)
		set := inv[k]
		if set == nil {
			set = make(map[uint64]struct{})
			inv[k] = set
		}
		set[id] = struct{}{}
	}
}

// markPersistedLocked records (day, id) as durably on disk.
func (r *Registry) markPersistedLocked(day string, id uint64) {
	done := r.persisted[day]
	if done == nil {
		done = make(map[uint64]struct{})
		r.persisted[day] = done
	}
	done[id] = struct{}{}
}

// DropDay forgets day's inverted index — retention drops it together
// with the day's data partition. Series identities (names/byKey) are
// unaffected.
func (r *Registry) DropDay(day string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.invertedByDay, day)
	delete(r.persisted, day)
}

// Match returns the sorted SeriesIDs whose label sets satisfy every
// equality matcher. Each matcher's candidate set is the union of the
// per-day inverted sets (a series indexed in any live day is a
// candidate); the per-matcher sets are then intersected as before.
func (r *Registry) Match(matchers []Matcher) []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var ids []uint64
	for i, m := range matchers {
		key := labelKey(m.Name, m.Value)
		set := make(map[uint64]struct{})
		for _, inv := range r.invertedByDay {
			for id := range inv[key] {
				set[id] = struct{}{}
			}
		}
		if len(set) == 0 {
			return nil
		}
		if i == 0 {
			ids = make([]uint64, 0, len(set))
			for id := range set {
				ids = append(ids, id)
			}
			continue
		}
		kept := ids[:0]
		for _, id := range ids {
			if _, ok := set[id]; ok {
				kept = append(kept, id)
			}
		}
		ids = kept
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
