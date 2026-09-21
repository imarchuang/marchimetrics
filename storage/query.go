package storage

import (
	"fmt"
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

// QueryRange returns raw samples for all series matching every matcher,
// keeping timestamps in [startMs, endMs]. It scans the in-memory buffer,
// so freshly appended data is queryable before any flush; scanning
// flushed disk parts arrives with PR2/PR3.
func (s *Storage) QueryRange(matchers []Matcher, startMs, endMs int64) []SeriesResult {
	ids := s.registry.Match(matchers)
	results := make([]SeriesResult, 0, len(ids))

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, id := range ids {
		samples := samplesInRange(s.mem[id], startMs, endMs)
		if len(samples) == 0 {
			continue
		}
		labels, _ := s.registry.Labels(id)
		results = append(results, SeriesResult{ID: id, Labels: labels, Samples: samples})
	}
	return results
}

// samplesInRange filters and time-sorts samples. Disk parts will store
// pre-sorted blocks (PR2); the mem buffer sorts at query time.
func samplesInRange(samples []Sample, startMs, endMs int64) []Sample {
	out := make([]Sample, 0, len(samples))
	for _, s := range samples {
		if s.Timestamp >= startMs && s.Timestamp <= endMs {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
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
