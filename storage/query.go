package storage

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Matcher is an equality matcher Name=Value against series labels.
type Matcher struct {
	Name  string
	Value string
}

// SeriesResult carries one matching series' labels and in-window samples.
type SeriesResult struct {
	ID      uint64
	Labels  []Label
	Samples []Sample
}

// QueryStats reports what a query touched (PLAN.md pass bar: "query
// stats show parts/blocks/points scanned"). Only the disk tier is
// counted as "scanned" — the stats exist to make disk IO cost visible;
// scanning the in-memory buffer is cheap by comparison.
type QueryStats struct {
	PartsScanned   int // disk parts opened and read (time range overlapped)
	BlocksScanned  int // series blocks decoded from those parts
	PointsScanned  int // samples decoded from disk blocks, before time filtering
	PointsReturned int // samples in the final merged result (disk + mem)
}

// QueryRange returns raw samples for all series matching every matcher,
// keeping timestamps in [startMs, endMs]. It merges two source tiers:
// the in-memory buffer (data appended since the last flush) and the
// immutable disk parts of every overlapping day partition — so data is
// queryable both before and after a flush.
func (s *Storage) QueryRange(matchers []Matcher, startMs, endMs int64) ([]SeriesResult, *QueryStats, error) {
	stats := &QueryStats{}
	ids := s.registry.Match(matchers)
	if len(ids) == 0 {
		return nil, stats, nil
	}
	want := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}

	merged := make(map[uint64][]Sample, len(ids))

	// Disk tier: immutable parts of overlapping day partitions. The
	// partition read lock is held for the whole scan so that compaction
	// cannot swap the manifest and delete merged parts mid-query — a
	// query always sees a consistent part set.
	for _, p := range s.partitionsInRange(startMs, endMs) {
		p.mu.RLock()
		if p.dropped { // retention deleted the dir; expired data is legitimately gone
			p.mu.RUnlock()
			continue
		}
		for _, name := range p.parts {
			dir := filepath.Join(p.dir, "parts", name)
			blocks, err := readPart(dir, want, startMs, endMs, stats)
			if err != nil {
				p.mu.RUnlock()
				return nil, stats, fmt.Errorf("cannot read part %s/parts/%s: %w", p.day, name, err)
			}
			for id, samples := range blocks {
				merged[id] = append(merged[id], samples...)
			}
		}
		p.mu.RUnlock()
	}

	// Memory tier: the not-yet-flushed buffer.
	s.mu.RLock()
	for _, id := range ids {
		for _, sm := range s.mem[id] {
			if sm.Timestamp >= startMs && sm.Timestamp <= endMs {
				merged[id] = append(merged[id], sm)
			}
		}
	}
	s.mu.RUnlock()

	results := make([]SeriesResult, 0, len(merged))
	for _, id := range ids {
		samples := merged[id]
		if len(samples) == 0 {
			continue
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i].Timestamp < samples[j].Timestamp })
		labels, _ := s.registry.Labels(id)
		stats.PointsReturned += len(samples)
		results = append(results, SeriesResult{ID: id, Labels: labels, Samples: samples})
	}
	return results, stats, nil
}

var metricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// ParseSelector parses the MVP query subset (PLAN.md section 3):
//
//	http_requests_total
//	http_requests_total{job="api", instance="h1:9090"}
//	{job="api"}
//
// Only equality matchers are supported.
func ParseSelector(q string) ([]Matcher, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, fmt.Errorf("empty selector")
	}

	var matchers []Matcher
	rest := q
	if i := strings.IndexByte(q, '{'); i >= 0 {
		if name := strings.TrimSpace(q[:i]); name != "" {
			if !metricNameRE.MatchString(name) {
				return nil, fmt.Errorf("invalid metric name %q", name)
			}
			matchers = append(matchers, Matcher{Name: MetricNameLabel, Value: name})
		}
		rest = q[i:]
	} else {
		if !metricNameRE.MatchString(q) {
			return nil, fmt.Errorf("invalid metric name %q", q)
		}
		return []Matcher{{Name: MetricNameLabel, Value: q}}, nil
	}

	if len(rest) < 2 || rest[0] != '{' || rest[len(rest)-1] != '}' {
		return nil, fmt.Errorf("malformed selector %q", q)
	}
	for _, part := range splitLabelPairs(rest[1 : len(rest)-1]) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("malformed matcher %q, want label=\"value\"", part)
		}
		name := strings.TrimSpace(part[:eq])
		if !metricNameRE.MatchString(name) {
			return nil, fmt.Errorf("invalid label name %q", name)
		}
		value, err := strconv.Unquote(strings.TrimSpace(part[eq+1:]))
		if err != nil {
			return nil, fmt.Errorf("malformed value in matcher %q: %s", part, err)
		}
		matchers = append(matchers, Matcher{Name: name, Value: value})
	}
	if len(matchers) == 0 {
		return nil, fmt.Errorf("selector must contain at least one matcher")
	}
	return matchers, nil
}

// splitLabelPairs splits on commas that are not inside double quotes.
func splitLabelPairs(s string) []string {
	var parts []string
	start := 0
	inQuote := false
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case escaped:
			escaped = false
		case inQuote && c == '\\':
			escaped = true
		case c == '"':
			inQuote = !inQuote
		case c == ',' && !inQuote:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}
