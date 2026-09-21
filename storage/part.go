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

// partMeta is meta.json inside each part directory.
type partMeta struct {
	MinTime      int64  `json:"minTime"` // milliseconds
	MaxTime      int64  `json:"maxTime"`
	SeriesCount  int    `json:"seriesCount"`
	SamplesCount int    `json:"samplesCount"`
	Tier         string `json:"tier"`
}

// seriesIndexEntry maps a series to its block inside the shared bin
// files. Both files store fixed-size 8-byte cells, so a block occupies
// bytes [offset*8, (offset+count)*8) in timestamps.bin and values.bin
// alike.
type seriesIndexEntry struct {
	Offset uint64 `json:"offset"` // in samples
	Count  uint64 `json:"count"`
}

// writePart writes data as the contents of dir (already named
// .publishing-* by the caller, who renames it into place afterwards).
// Samples are stored raw little-endian — VM does delta/varint encoding
// and compression here, which the MVP deliberately skips (LEARNING.md).
func writePart(dir string, data map[uint64][]Sample) (*partMeta, error) {
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
	meta := &partMeta{MinTime: math.MaxInt64, MaxTime: math.MinInt64, Tier: tierSmall}
	var b [8]byte
	var offset uint64
	for _, id := range ids {
		samples := data[id]
		sort.Slice(samples, func(i, j int) bool { return samples[i].Timestamp < samples[j].Timestamp })
		for _, sm := range samples {
			binary.LittleEndian.PutUint64(b[:], uint64(sm.Timestamp))
			tsBuf.Write(b[:])
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(sm.Value))
			valBuf.Write(b[:])
			if sm.Timestamp < meta.MinTime {
				meta.MinTime = sm.Timestamp
			}
			if sm.Timestamp > meta.MaxTime {
				meta.MaxTime = sm.Timestamp
			}
		}
		index[strconv.FormatUint(id, 10)] = seriesIndexEntry{Offset: offset, Count: uint64(len(samples))}
		offset += uint64(len(samples))
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

// readPart returns samples for the wanted series within [startMs, endMs],
// accumulating IO cost into st. A part whose time range does not overlap
// is skipped without touching st — skipping via meta.json is exactly the
// cheap read the stats are meant to make visible.
func readPart(dir string, want map[uint64]struct{}, startMs, endMs int64, st *QueryStats) (map[uint64][]Sample, error) {
	metaData, err := os.ReadFile(filepath.Join(dir, partMetaFile))
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", partMetaFile, err)
	}
	var meta partMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", partMetaFile, err)
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
		tsBuf := make([]byte, e.Count*8)
		if _, err := tsFile.ReadAt(tsBuf, int64(e.Offset*8)); err != nil {
			return nil, fmt.Errorf("cannot read timestamps for series %d: %w", id, err)
		}
		valBuf := make([]byte, e.Count*8)
		if _, err := valFile.ReadAt(valBuf, int64(e.Offset*8)); err != nil {
			return nil, fmt.Errorf("cannot read values for series %d: %w", id, err)
		}
		for i := uint64(0); i < e.Count; i++ {
			t := int64(binary.LittleEndian.Uint64(tsBuf[i*8:]))
			if t < startMs || t > endMs {
				continue
			}
			v := math.Float64frombits(binary.LittleEndian.Uint64(valBuf[i*8:]))
			out[id] = append(out[id], Sample{Timestamp: t, Value: v})
		}
	}
	return out, nil
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
