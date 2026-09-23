package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// indexdb is the persistent form of the series registry: instead of
// rewriting one growing names.json on every flush, each flush appends
// only the series registered since the last flush as one immutable
// segment file. Open replays the segments in order to rebuild the
// registry. This is the LSM idea applied to the index itself — the
// thin stand-in for VictoriaMetrics' mergeset IndexDB (LEARNING.md).
//
// Layout: <path>/series/indexdb/NNNNNN.seg
//
// Segment binary format (all varints via encoding/binary):
//
//	"MMIDX001"                       magic + version (8 bytes)
//	record*                          until EOF
//
//	record = uvarint(deltaID) uvarint(nLabels) label*
//	label  = uvarint(len(name)) name uvarint(len(value)) value
//
// deltaID is relative to the previous record's ID (first record stores
// its absolute ID). Records are written in ascending ID order, so deltas
// are small positive integers — one byte each in the common case.
const (
	indexdbDir      = "indexdb"
	indexdbSegMagic = "MMIDX001"
)

// indexSegmentName formats a segment sequence number.
func indexSegmentName(seq int) string {
	return fmt.Sprintf("%06d.seg", seq)
}

// appendIndexSegment writes entries as a new segment inside
// <path>/series/indexdb/ and returns the segment's file name. Entries
// are sorted by ID before encoding. An empty entries slice is a no-op
// and returns "".
//
// Crash safety mirrors part publishing: the segment is written to a
// .tmp file and renamed into place, so a crash mid-write leaves a
// complete previous generation of segments plus an ignorable .tmp.
func appendIndexSegment(dir string, entries map[uint64][]Label) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	ids := make([]uint64, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	seq, err := nextIndexSegmentSeq(dir)
	if err != nil {
		return "", err
	}
	name := indexSegmentName(seq)
	final := filepath.Join(dir, name)
	tmp := final + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	w := bufio.NewWriter(f)
	if _, err := w.WriteString(indexdbSegMagic); err != nil {
		f.Close()
		return "", err
	}
	var scratch [binary.MaxVarintLen64]byte
	putUvarint := func(v uint64) error {
		n := binary.PutUvarint(scratch[:], v)
		_, err := w.Write(scratch[:n])
		return err
	}
	putString := func(s string) error {
		if err := putUvarint(uint64(len(s))); err != nil {
			return err
		}
		_, err := w.WriteString(s)
		return err
	}

	var prev uint64
	for i, id := range ids {
		delta := id
		if i > 0 {
			delta = id - prev
		}
		prev = id
		if err := putUvarint(delta); err != nil {
			f.Close()
			return "", err
		}
		labels := entries[id]
		if err := putUvarint(uint64(len(labels))); err != nil {
			f.Close()
			return "", err
		}
		for _, l := range labels {
			if err := putString(l.Name); err != nil {
				f.Close()
				return "", err
			}
			if err := putString(l.Value); err != nil {
				f.Close()
				return "", err
			}
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		return "", err
	}
	return name, nil
}

// nextIndexSegmentSeq returns one past the highest existing segment
// sequence number in dir.
func nextIndexSegmentSeq(dir string) (int, error) {
	names, err := listIndexSegments(dir)
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		return 1, nil
	}
	last := names[len(names)-1]
	n, err := strconv.Atoi(last[:len(last)-len(".seg")])
	if err != nil {
		return 0, fmt.Errorf("bad segment name %q: %w", last, err)
	}
	return n + 1, nil
}

// listIndexSegments returns the segment file names in dir, sorted by
// sequence number. Leftover .tmp files from a crashed write are removed.
func listIndexSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) == ".tmp" {
			os.Remove(filepath.Join(dir, name)) // crashed mid-publish; safe to drop
			continue
		}
		if filepath.Ext(name) != ".seg" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names) // zero-padded names sort by sequence
	return names, nil
}

// readIndexSegment replays one segment, calling fn for each record in
// file order. A truncated tail record (crash between segment rename and
// the data part it accompanied) is tolerated: replay stops at the last
// complete record, matching the at-most-one-flush durability window.
func readIndexSegment(path string, fn func(id uint64, labels []Label) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < len(indexdbSegMagic) || string(data[:len(indexdbSegMagic)]) != indexdbSegMagic {
		return fmt.Errorf("%s: bad magic", path)
	}
	r := &byteReader{b: data[len(indexdbSegMagic):]}

	var prev uint64
	for r.len() > 0 {
		id, labels, err := readIndexRecord(r, prev)
		if err != nil {
			// Tolerate a torn tail: stop at the last complete record.
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("%s: %w", path, err)
		}
		prev = id
		if err := fn(id, labels); err != nil {
			return err
		}
	}
	return nil
}

func readIndexRecord(r *byteReader, prev uint64) (uint64, []Label, error) {
	delta, err := r.uvarint()
	if err != nil {
		return 0, nil, err
	}
	id := prev + delta
	n, err := r.uvarint()
	if err != nil {
		return 0, nil, err
	}
	labels := make([]Label, 0, n)
	for i := uint64(0); i < n; i++ {
		name, err := r.str()
		if err != nil {
			return 0, nil, err
		}
		value, err := r.str()
		if err != nil {
			return 0, nil, err
		}
		labels = append(labels, Label{Name: name, Value: value})
	}
	return id, labels, nil
}

// byteReader is a minimal cursor over a byte slice with varint helpers
// that report io.ErrUnexpectedEOF on torn tails.
type byteReader struct {
	b []byte
}

func (r *byteReader) len() int { return len(r.b) }

func (r *byteReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b)
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, fmt.Errorf("varint overflow")
	}
	r.b = r.b[n:]
	return v, nil
}

func (r *byteReader) str() (string, error) {
	l, err := r.uvarint()
	if err != nil {
		return "", err
	}
	if uint64(len(r.b)) < l {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.b[:l])
	r.b = r.b[l:]
	return s, nil
}
