package storage

import (
	"sort"
	"strings"
	"sync"
)

// Label is a single Prometheus-style name/value pair.
type Label struct {
	Name  string
	Value string
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
// counterpart of series/names.json (forward) and series/inverted.json
// (inverted); persistence arrives with flushing in PR2.
type Registry struct {
	mu       sync.RWMutex
	nextID   uint64
	byKey    map[string]uint64              // canonical key -> SeriesID
	names    map[uint64][]Label             // SeriesID -> sorted label set
	inverted map[string]map[uint64]struct{} // labelKey -> SeriesID set
}

// NewRegistry returns an empty registry. IDs start at 1 so that 0 can
// mean "no series" in later code.
func NewRegistry() *Registry {
	return &Registry{
		nextID:   1,
		byKey:    make(map[string]uint64),
		names:    make(map[uint64][]Label),
		inverted: make(map[string]map[uint64]struct{}),
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
