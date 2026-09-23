package storage

import (
	"math"
	"math/rand"
	"testing"
)

func TestTimestampsRoundTrip(t *testing.T) {
	cases := map[string][]int64{
		"empty":            {},
		"single":           {1000},
		"regular 15s":      regularTs(1000000, 15000, 100),
		"irregular":        {1000, 1001, 5000, 5001, 5002, 90000},
		"same timestamp":   {1000, 1000, 1000},
		"large gaps":       {0, 1 << 40, 1 << 50},
		"epoch millis now": {1790000000000, 1790000015000, 1790000030000},
	}
	for name, ts := range cases {
		got, err := decodeTimestamps(encodeTimestamps(ts))
		if err != nil {
			t.Errorf("%s: decode: %s", name, err)
			continue
		}
		if len(got) != len(ts) {
			t.Errorf("%s: decoded %d, want %d", name, len(got), len(ts))
			continue
		}
		for i := range ts {
			if got[i] != ts[i] {
				t.Errorf("%s: point %d = %d, want %d", name, i, got[i], ts[i])
				break
			}
		}
	}
}

func TestTimestampsCompressionRatio(t *testing.T) {
	ts := regularTs(1790000000000, 15000, 1000)
	enc := encodeTimestamps(ts)
	raw := len(ts) * 8
	// Regular intervals: ~1 byte per point (dod = 0) + header.
	if len(enc) > raw/5 {
		t.Fatalf("encoded %d bytes for %d points (raw %d), want >5x compression", len(enc), len(ts), raw)
	}
	t.Logf("timestamps: %d points, raw %d B -> encoded %d B (%.1fx, %.2f B/pt)",
		len(ts), raw, len(enc), float64(raw)/float64(len(enc)), float64(len(enc))/float64(len(ts)))
}

func regularTs(start, step int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = start + int64(i)*step
	}
	return out
}

func TestValuesRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	random := make([]float64, 500)
	for i := range random {
		random[i] = rng.NormFloat64() * 1000
	}
	cases := map[string][]float64{
		"empty":         {},
		"single":        {3.14},
		"constant":      {7, 7, 7, 7, 7, 7, 7, 7},
		"integers":      {0, 1, 2, 3, 4, 5, 100, 101, 102},
		"random":        random,
		"special":       {0, math.Inf(1), math.Inf(-1), math.NaN(), -0.0, math.SmallestNonzeroFloat64, math.MaxFloat64},
		"window switch": {1.5, 1.6, 1e300, 1.5, 1.6, -1e-300, 1.5},
	}
	for name, vs := range cases {
		got, err := decodeValues(encodeValues(vs))
		if err != nil {
			t.Errorf("%s: decode: %s", name, err)
			continue
		}
		if len(got) != len(vs) {
			t.Errorf("%s: decoded %d, want %d", name, len(got), len(vs))
			continue
		}
		for i := range vs {
			// Bit-exact comparison (NaN == NaN at the bit level).
			if math.Float64bits(got[i]) != math.Float64bits(vs[i]) {
				t.Errorf("%s: point %d bits = %x, want %x", name, i,
					math.Float64bits(got[i]), math.Float64bits(vs[i]))
				break
			}
		}
	}
}

func TestValuesCompressionRatio(t *testing.T) {
	// A slowly drifting gauge: the common monitoring shape.
	vs := make([]float64, 1000)
	for i := range vs {
		vs[i] = 42.0 + float64(i%7)*0.1
	}
	enc := encodeValues(vs)
	raw := len(vs) * 8
	t.Logf("values: %d points, raw %d B -> encoded %d B (%.1fx, %.2f B/pt)",
		len(vs), raw, len(enc), float64(raw)/float64(len(enc)), float64(len(enc))/float64(len(vs)))

	// Constant gauge: 1 bit per point after the first.
	constant := make([]float64, 1000)
	for i := range constant {
		constant[i] = 42
	}
	encC := encodeValues(constant)
	if len(encC) > 200 { // 8B header + 999 bits ≈ 134 B
		t.Fatalf("constant gauge encoded to %d B, want ≈134", len(encC))
	}
	t.Logf("constant gauge: %d points -> %d B (%.2f B/pt)", len(constant), len(encC), float64(len(encC))/float64(len(constant)))
}

// Parts written by older marchimetrics versions (raw 8-byte cells, no
// encoding field) must stay readable.
func TestReadPartRawBackwardCompat(t *testing.T) {
	dir := t.TempDir()
	data := map[uint64][]Sample{
		1: {{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: 2.5}},
		2: {{Timestamp: 1000, Value: 7}},
	}
	if _, err := writePartEncoded(dir, data, tierSmall, encodingRaw); err != nil {
		t.Fatalf("writePartEncoded raw: %s", err)
	}

	st := &QueryStats{}
	got, err := readPart(dir, map[uint64]struct{}{1: {}, 2: {}}, 0, 3000, st)
	if err != nil {
		t.Fatalf("readPart raw: %s", err)
	}
	if len(got[1]) != 2 || got[1][0].Value != 1.5 || got[1][1].Value != 2.5 {
		t.Fatalf("series 1 = %+v", got[1])
	}
	if len(got[2]) != 1 || got[2][0].Value != 7 {
		t.Fatalf("series 2 = %+v", got[2])
	}
}

// Gorilla parts round-trip through writePart/readPart with bit-exact
// values (covered end-to-end by the storage tests, asserted directly
// here for the part layer).
func TestWriteReadPartGorilla(t *testing.T) {
	dir := t.TempDir()
	data := map[uint64][]Sample{
		1: {{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: 1.5}, {Timestamp: 3000, Value: 2.75}},
	}
	meta, err := writePart(dir, data, tierSmall)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Encoding != encodingGorilla {
		t.Fatalf("encoding = %q, want gorilla", meta.Encoding)
	}
	st := &QueryStats{}
	got, err := readPart(dir, map[uint64]struct{}{1: {}}, 0, 10000, st)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range data[1] {
		if got[1][i] != want {
			t.Fatalf("point %d = %+v, want %+v", i, got[1][i], want)
		}
	}
}
