package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Part file names inside partitions/YYYYMMDD/parts/NNNNNN/ (PLAN.md §4).
const (
	partMetaFile      = "meta.json"
	partSeriesIndex   = "series.index"
	partTimestampsBin = "timestamps.bin"
	partValuesBin     = "values.bin"
)

// Part tiers: flushes produce "small" parts; compaction (PR4) merges
// them into "big" ones.
const (
	tierSmall = "small"
	tierBig   = "big"
)

// Block encodings recorded in meta.json. Parts written before
// compression was added have no encoding field and are read as raw.
const (
	encodingRaw     = "raw"     // fixed 8-byte cells (pre-compression parts)
	encodingGorilla = "gorilla" // dod+varint timestamps, XOR values
)

// PartMeta is meta.json inside each part directory.
type PartMeta struct {
	MinTime      int64  `json:"minTime"` // milliseconds
	MaxTime      int64  `json:"maxTime"`
	SeriesCount  int    `json:"seriesCount"`
	SamplesCount int    `json:"samplesCount"`
	Tier         string `json:"tier"`
	Encoding     string `json:"encoding,omitempty"` // empty = raw (legacy parts)
}

// seriesIndexEntry maps a series to its block inside the shared bin
// files. Raw parts use Offset+Count (fixed 8-byte cells, one pair
// addressing both files). Gorilla parts use byte ranges per file.
type seriesIndexEntry struct {
	Offset uint64 `json:"offset,omitempty"` // raw: in samples
	Count  uint64 `json:"count"`

	TSOffset  uint64 `json:"tsOffset,omitempty"`  // gorilla: byte offset in timestamps.bin
	TSLength  uint64 `json:"tsLength,omitempty"`  // gorilla: block length in bytes
	ValOffset uint64 `json:"valOffset,omitempty"` // gorilla: byte offset in values.bin
	ValLength uint64 `json:"valLength,omitempty"` // gorilla: block length in bytes
}

// writePart writes data as the contents of dir (already named
// .publishing-* by the caller, who renames it into place afterwards),
// gorilla-encoded. tier is tierSmall for flushes, tierBig for
// compaction output.
func writePart(dir string, data map[uint64][]Sample, tier string) (*PartMeta, error) {
	return writePartEncoded(dir, data, tier, encodingGorilla)
}

// writePartEncoded is writePart with an explicit encoding — tests use
// it to produce legacy raw parts and verify the read path stays
// backward compatible.
func writePartEncoded(dir string, data map[uint64][]Sample, tier, encoding string) (*PartMeta, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	ids := make([]uint64, 0, len(data))
	for id := range data {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var tsBuf, valBuf bytes.Buffer
	index := make(map[string]seriesIndexEntry, len(ids))
	meta := &PartMeta{MinTime: math.MaxInt64, MaxTime: math.MinInt64, Tier: tier, Encoding: encoding}
	var b [8]byte
	var offset uint64
	for _, id := range ids {
		samples := data[id]
		sort.Slice(samples, func(i, j int) bool { return samples[i].Timestamp < samples[j].Timestamp })

		entry := seriesIndexEntry{Count: uint64(len(samples))}
		switch encoding {
		case encodingGorilla:
			tsBlock := encodeTimestamps(sampleTimestamps(samples))
			valBlock := encodeValues(sampleValues(samples))
			entry.TSOffset = uint64(tsBuf.Len())
			entry.TSLength = uint64(len(tsBlock))
			entry.ValOffset = uint64(valBuf.Len())
			entry.ValLength = uint64(len(valBlock))
			tsBuf.Write(tsBlock)
			valBuf.Write(valBlock)
		default: // raw
			entry.Offset = offset
			for _, sm := range samples {
				binary.LittleEndian.PutUint64(b[:], uint64(sm.Timestamp))
				tsBuf.Write(b[:])
				binary.LittleEndian.PutUint64(b[:], math.Float64bits(sm.Value))
				valBuf.Write(b[:])
			}
			offset += uint64(len(samples))
		}

		for _, sm := range samples {
			if sm.Timestamp < meta.MinTime {
				meta.MinTime = sm.Timestamp
			}
			if sm.Timestamp > meta.MaxTime {
				meta.MaxTime = sm.Timestamp
			}
		}
		index[strconv.FormatUint(id, 10)] = entry
		meta.SamplesCount += len(samples)
	}
	meta.SeriesCount = len(ids)

	files := []struct {
		name string
		data []byte
	}{
		{partTimestampsBin, tsBuf.Bytes()},
		{partValuesBin, valBuf.Bytes()},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, 0o644); err != nil {
			return nil, fmt.Errorf("cannot write %s: %w", f.name, err)
		}
	}
	if err := writeJSON(filepath.Join(dir, partSeriesIndex), index); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(dir, partMetaFile), meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// readPartMeta reads just meta.json — used for time-range pruning and
// by compaction to find small parts.
func readPartMeta(dir string) (*PartMeta, error) {
	data, err := os.ReadFile(filepath.Join(dir, partMetaFile))
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", partMetaFile, err)
	}
	var meta PartMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", partMetaFile, err)
	}
	return &meta, nil
}

// readPart returns samples for the wanted series within [startMs, endMs],
// accumulating IO cost into st. A part whose time range does not overlap
// is skipped without touching st — skipping via meta.json is exactly the
// cheap read the stats are meant to make visible.
func readPart(dir string, want map[uint64]struct{}, startMs, endMs int64, st *QueryStats) (map[uint64][]Sample, error) {
	meta, err := readPartMeta(dir)
	if err != nil {
		return nil, err
	}
	if meta.MaxTime < startMs || meta.MinTime > endMs {
		return nil, nil
	}
	st.PartsScanned++

	idxData, err := os.ReadFile(filepath.Join(dir, partSeriesIndex))
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", partSeriesIndex, err)
	}
	var index map[string]seriesIndexEntry
	if err := json.Unmarshal(idxData, &index); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", partSeriesIndex, err)
	}

	tsFile, err := os.Open(filepath.Join(dir, partTimestampsBin))
	if err != nil {
		return nil, err
	}
	defer tsFile.Close()
	valFile, err := os.Open(filepath.Join(dir, partValuesBin))
	if err != nil {
		return nil, err
	}
	defer valFile.Close()

	out := make(map[uint64][]Sample)
	for idStr, e := range index {
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad series id %q in %s: %w", idStr, partSeriesIndex, err)
		}
		if _, ok := want[id]; !ok {
			continue
		}
		st.BlocksScanned++
		st.PointsScanned += int(e.Count)
		samples, err := readSeriesBlock(tsFile, valFile, e, meta.Encoding)
		if err != nil {
			return nil, fmt.Errorf("cannot decode series %d: %w", id, err)
		}
		for _, sm := range samples {
			if sm.Timestamp < startMs || sm.Timestamp > endMs {
				continue
			}
			out[id] = append(out[id], sm)
		}
	}
	return out, nil
}

// readSeriesBlock decodes one series' timestamps+values block pair,
// dispatching on the part's encoding.
func readSeriesBlock(tsFile, valFile *os.File, e seriesIndexEntry, encoding string) ([]Sample, error) {
	if encoding == encodingGorilla {
		tsBuf := make([]byte, e.TSLength)
		if _, err := tsFile.ReadAt(tsBuf, int64(e.TSOffset)); err != nil {
			return nil, err
		}
		valBuf := make([]byte, e.ValLength)
		if _, err := valFile.ReadAt(valBuf, int64(e.ValOffset)); err != nil {
			return nil, err
		}
		ts, err := decodeTimestamps(tsBuf)
		if err != nil {
			return nil, err
		}
		vs, err := decodeValues(valBuf)
		if err != nil {
			return nil, err
		}
		if uint64(len(ts)) != e.Count || uint64(len(vs)) != e.Count {
			return nil, fmt.Errorf("decoded %d timestamps / %d values, want %d", len(ts), len(vs), e.Count)
		}
		samples := make([]Sample, len(ts))
		for i := range ts {
			samples[i] = Sample{Timestamp: ts[i], Value: vs[i]}
		}
		return samples, nil
	}

	// Raw: fixed 8-byte cells addressed by one {offset, count} pair.
	tsBuf := make([]byte, e.Count*8)
	if _, err := tsFile.ReadAt(tsBuf, int64(e.Offset*8)); err != nil {
		return nil, err
	}
	valBuf := make([]byte, e.Count*8)
	if _, err := valFile.ReadAt(valBuf, int64(e.Offset*8)); err != nil {
		return nil, err
	}
	samples := make([]Sample, 0, e.Count)
	for i := uint64(0); i < e.Count; i++ {
		t := int64(binary.LittleEndian.Uint64(tsBuf[i*8:]))
		v := math.Float64frombits(binary.LittleEndian.Uint64(valBuf[i*8:]))
		samples = append(samples, Sample{Timestamp: t, Value: v})
	}
	return samples, nil
}

// readPartAll reads every series block in the part without any time
// filtering — compaction by definition needs all the data.
func readPartAll(dir string) (map[uint64][]Sample, error) {
	idxData, err := os.ReadFile(filepath.Join(dir, partSeriesIndex))
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", partSeriesIndex, err)
	}
	var index map[string]seriesIndexEntry
	if err := json.Unmarshal(idxData, &index); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", partSeriesIndex, err)
	}

	tsFile, err := os.Open(filepath.Join(dir, partTimestampsBin))
	if err != nil {
		return nil, err
	}
	defer tsFile.Close()
	valFile, err := os.Open(filepath.Join(dir, partValuesBin))
	if err != nil {
		return nil, err
	}
	defer valFile.Close()

	meta, err := readPartMeta(dir)
	if err != nil {
		return nil, err
	}

	out := make(map[uint64][]Sample, len(index))
	for idStr, e := range index {
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad series id %q in %s: %w", idStr, partSeriesIndex, err)
		}
		samples, err := readSeriesBlock(tsFile, valFile, e, meta.Encoding)
		if err != nil {
			return nil, fmt.Errorf("cannot decode series %d: %w", id, err)
		}
		out[id] = samples
	}
	return out, nil
}

// sampleTimestamps / sampleValues split a sample slice for the encoders.
func sampleTimestamps(samples []Sample) []int64 {
	out := make([]int64, len(samples))
	for i, sm := range samples {
		out[i] = sm.Timestamp
	}
	return out
}

func sampleValues(samples []Sample) []float64 {
	out := make([]float64, len(samples))
	for i, sm := range samples {
		out[i] = sm.Value
	}
	return out
}

// writeJSON writes v as indented JSON via a temp file + rename, so a
// crash never leaves a half-written file behind.
func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
