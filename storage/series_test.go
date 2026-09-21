package storage

import "testing"

func TestCanonicalKeyOrderIndependent(t *testing.T) {
	a := []Label{{Name: "job", Value: "api"}, {Name: MetricNameLabel, Value: "http_requests_total"}}
	b := []Label{{Name: MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}}
	if CanonicalKey(a) != CanonicalKey(b) {
		t.Fatalf("label order must not change the canonical key:\n%q\n%q", CanonicalKey(a), CanonicalKey(b))
	}
}

func TestCanonicalKeyDistinct(t *testing.T) {
	a := []Label{{Name: "job", Value: "api"}}
	b := []Label{{Name: "job", Value: "web"}}
	if CanonicalKey(a) == CanonicalKey(b) {
		t.Fatalf("different label sets must not share canonical key %q", CanonicalKey(a))
	}
}

func TestCanonicalKeyDoesNotMutateInput(t *testing.T) {
	in := []Label{{Name: "z", Value: "1"}, {Name: "a", Value: "2"}}
	CanonicalKey(in)
	if in[0].Name != "z" {
		t.Fatalf("CanonicalKey mutated its input: %+v", in)
	}
}

func TestRegistryResolveStableIDs(t *testing.T) {
	r := NewRegistry()
	labels := []Label{{Name: MetricNameLabel, Value: "m"}, {Name: "job", Value: "api"}}

	id1 := r.Resolve(labels)
	id2 := r.Resolve([]Label{{Name: "job", Value: "api"}, {Name: MetricNameLabel, Value: "m"}}) // reordered
	if id1 != id2 {
		t.Fatalf("same label set resolved to different IDs: %d vs %d", id1, id2)
	}

	other := r.Resolve([]Label{{Name: MetricNameLabel, Value: "m"}, {Name: "job", Value: "web"}})
	if other == id1 {
		t.Fatalf("different label sets share ID %d", id1)
	}
	if r.Len() != 2 {
		t.Fatalf("registry Len = %d, want 2", r.Len())
	}
}

func TestRegistryLabelsRoundTrip(t *testing.T) {
	r := NewRegistry()
	id := r.Resolve([]Label{{Name: "b", Value: "2"}, {Name: "a", Value: "1"}})
	labels, ok := r.Labels(id)
	if !ok {
		t.Fatalf("no labels for id %d", id)
	}
	if len(labels) != 2 || labels[0].Name != "a" || labels[1].Name != "b" {
		t.Fatalf("labels did not round-trip sorted: %+v", labels)
	}
}

func TestRegistryMatchIntersects(t *testing.T) {
	r := NewRegistry()
	api1 := r.Resolve([]Label{{Name: MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}, {Name: "instance", Value: "h1"}})
	api2 := r.Resolve([]Label{{Name: MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}, {Name: "instance", Value: "h2"}})
	web := r.Resolve([]Label{{Name: MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "web"}})

	ids := r.Match([]Matcher{{Name: MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}})
	if len(ids) != 2 || ids[0] != api1 || ids[1] != api2 {
		t.Fatalf("match = %v, want [%d %d]", ids, api1, api2)
	}

	ids = r.Match([]Matcher{{Name: "job", Value: "web"}})
	if len(ids) != 1 || ids[0] != web {
		t.Fatalf("match = %v, want [%d]", ids, web)
	}

	if ids := r.Match([]Matcher{{Name: "job", Value: "nope"}}); len(ids) != 0 {
		t.Fatalf("match = %v, want empty", ids)
	}
}
