package storage

import "testing"

func TestParseSelectorMetricOnly(t *testing.T) {
	m, err := ParseSelector("http_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m[0] != (Matcher{Name: MetricNameLabel, Value: "http_requests_total"}) {
		t.Fatalf("unexpected matchers: %+v", m)
	}
}

func TestParseSelectorWithLabels(t *testing.T) {
	m, err := ParseSelector(`http_requests_total{job="api", instance="h1:9090"}`)
	if err != nil {
		t.Fatal(err)
	}
	want := []Matcher{
		{Name: MetricNameLabel, Value: "http_requests_total"},
		{Name: "job", Value: "api"},
		{Name: "instance", Value: "h1:9090"},
	}
	if len(m) != len(want) {
		t.Fatalf("got %+v, want %+v", m, want)
	}
	for i := range want {
		if m[i] != want[i] {
			t.Fatalf("matcher %d = %+v, want %+v", i, m[i], want[i])
		}
	}
}

func TestParseSelectorLabelsOnly(t *testing.T) {
	m, err := ParseSelector(`{job="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m[0].Name != "job" {
		t.Fatalf("unexpected matchers: %+v", m)
	}
}

func TestParseSelectorCommaInsideValue(t *testing.T) {
	m, err := ParseSelector(`m{path="/a,b"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 || m[1].Value != "/a,b" {
		t.Fatalf("unexpected matchers: %+v", m)
	}
}

func TestParseSelectorErrors(t *testing.T) {
	for _, q := range []string{"", "{}", "m{job=api}", "9bad", `m{job="api"`} {
		if _, err := ParseSelector(q); err == nil {
			t.Fatalf("ParseSelector(%q) succeeded, want error", q)
		}
	}
}
