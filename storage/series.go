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

// Registry maps label sets to SeriesIDs and back. It is the in-memory
// counterpart of the indexdb segments under series/indexdb/ (forward)
// plus the derived inverted index; persistence is append-only — each
// flush writes only the series registered since the last flush.
type Registry struct {
	mu       sync.RWMutex
	nextID   uint64
	byKey    map[string]uint64              // canonical key -> SeriesID
	names    map[uint64][]Label             // SeriesID -> sorted label set
	inverted map[string]map[uint64]struct{} // labelKey -> SeriesID set
	pending  map[uint64][]Label             // registered since last DrainPending
}

// NewRegistry returns an empty registry. IDs start at 1 so that 0 can
// mean "no series" in later code.
func NewRegistry() *Registry {
	return &Registry{
		nextID:   1,
		byKey:    make(map[string]uint64),
		names:    make(map[uint64][]Label),
		inverted: make(map[string]map[uint64]struct{}),
		pending:  make(map[uint64][]Label),
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
	for _, l := range sorted {
		k := labelKey(l.Name, l.Value)
		set := r.inverted[k]
		if set == nil {
			set = make(map[uint64]struct{})
			r.inverted[k] = set
		}
		set[id] = struct{}{}
	}
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

// LoadEntries replays indexdb records (SeriesID -> label set), rebuilding
// the canonical and inverted indexes and continuing IDs past the maximum
// seen. It is idempotent: replaying the same record twice converges to
// the same state, which is what makes a torn final segment harmless.
// The inverted index is deliberately not persisted: the segments are the
// single source of truth and the index is derived from them at load time.
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
		for _, l := range sorted {
			k := labelKey(l.Name, l.Value)
			set := r.inverted[k]
			if set == nil {
				set = make(map[uint64]struct{})
				r.inverted[k] = set
			}
			set[id] = struct{}{}
		}
		if id > maxID {
			maxID = id
		}
	}
	if maxID >= r.nextID {
		r.nextID = maxID + 1
	}
}

// Match returns the sorted SeriesIDs whose label sets satisfy every
// equality matcher, intersecting inverted-index sets.
func (r *Registry) Match(matchers []Matcher) []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var ids []uint64
	for i, m := range matchers {
		set := r.inverted[labelKey(m.Name, m.Value)]
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
