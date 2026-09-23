package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
)

// Block encodings for the shared bin files (LEARNING.md "Block
// encoding"). Timestamps use delta-of-delta + zigzag varint — byte
// aligned, one byte per point for regular scrape intervals. Values use
// the Gorilla XOR bitstream — one bit per point for constant gauges.
//
// Timestamp block:
//
//	uvarint count
//	fixed64 first timestamp (LE)
//	zigzag-varint first delta (t1 - t0)        [if count >= 2]
//	zigzag-varint dod_i = d_i - d_{i-1} × (count-2)
//
// Value block:
//
//	uvarint count
//	fixed64 first value bits (LE)
//	bitstream for the rest, per point:
//	  '0'              → identical to previous
//	  '10'             → XOR fits the previous leading/trailing-zero window
//	  '11' + 5b leading + 6b significant + bits → new window

// encodeTimestamps compresses sorted (non-decreasing) timestamps.
func encodeTimestamps(ts []int64) []byte {
	out := make([]byte, 0, len(ts)+16)
	out = binary.AppendUvarint(out, uint64(len(ts)))
	if len(ts) == 0 {
		return out
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(ts[0]))
	out = append(out, b[:]...)
	if len(ts) == 1 {
		return out
	}
	prevDelta := ts[1] - ts[0]
	out = binary.AppendVarint(out, prevDelta)
	for i := 2; i < len(ts); i++ {
		d := ts[i] - ts[i-1]
		out = binary.AppendVarint(out, d-prevDelta)
		prevDelta = d
	}
	return out
}

// decodeTimestamps reverses encodeTimestamps.
func decodeTimestamps(data []byte) ([]int64, error) {
	r := &byteReader{b: data}
	count, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	if len(r.b) < 8 {
		return nil, io.ErrUnexpectedEOF
	}
	t0 := int64(binary.LittleEndian.Uint64(r.b[:8]))
	r.b = r.b[8:]
	out := make([]int64, 0, count)
	out = append(out, t0)
	if count == 1 {
		return out, nil
	}
	d, err := r.varint()
	if err != nil {
		return nil, err
	}
	out = append(out, t0+d)
	for i := uint64(2); i < count; i++ {
		dod, err := r.varint()
		if err != nil {
			return nil, err
		}
		d += dod
		out = append(out, out[i-1]+d)
	}
	return out, nil
}

// varint reads a zigzag-encoded signed varint.
func (r *byteReader) varint() (int64, error) {
	v, n := binary.Varint(r.b)
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, fmt.Errorf("varint overflow")
	}
	r.b = r.b[n:]
	return v, nil
}

// encodeValues compresses values with the Gorilla XOR scheme.
func encodeValues(vs []float64) []byte {
	out := make([]byte, 0, len(vs)*2+16)
	out = binary.AppendUvarint(out, uint64(len(vs)))
	if len(vs) == 0 {
		return out
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(vs[0]))
	out = append(out, b[:]...)

	w := &bitWriter{}
	prev := math.Float64bits(vs[0])
	var prevLz, prevTz int
	for i := 1; i < len(vs); i++ {
		cur := math.Float64bits(vs[i])
		xor := cur ^ prev
		if xor == 0 {
			w.writeBit(0)
		} else {
			lz := bits.LeadingZeros64(xor)
			tz := bits.TrailingZeros64(xor)
			if lz > 31 { // 5-bit field; clamp like the Gorilla paper
				lz = 31
			}
			sigLen := 64 - lz - tz
			if prevLz != 0 && lz >= prevLz && tz >= prevTz {
				// Fits the previous window: '10' + significant bits.
				w.writeBit(1)
				w.writeBit(0)
				w.writeBits(xor>>prevTz, 64-prevLz-prevTz)
			} else {
				// New window: '11' + 5b lz + 6b sigLen + significant bits.
				w.writeBit(1)
				w.writeBit(1)
				w.writeBits(uint64(lz), 5)
				if sigLen == 64 {
					w.writeBits(0, 6) // 64 does not fit in 6 bits; 0 means 64
				} else {
					w.writeBits(uint64(sigLen), 6)
				}
				w.writeBits(xor>>tz, sigLen)
				prevLz, prevTz = lz, tz
			}
		}
		prev = cur
	}
	return append(out, w.bytes()...)
}

// decodeValues reverses encodeValues.
func decodeValues(data []byte) ([]float64, error) {
	r := &byteReader{b: data}
	count, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	if len(r.b) < 8 {
		return nil, io.ErrUnexpectedEOF
	}
	prev := binary.LittleEndian.Uint64(r.b[:8])
	out := make([]float64, 0, count)
	out = append(out, math.Float64frombits(prev))

	br := &bitReader{b: r.b[8:]}
	var lz, tz int
	for i := uint64(1); i < count; i++ {
		bit, err := br.readBit()
		if err != nil {
			return nil, err
		}
		if bit == 0 {
			out = append(out, math.Float64frombits(prev))
			continue
		}
		bit, err = br.readBit()
		if err != nil {
			return nil, err
		}
		if bit == 1 { // new window
			l, err := br.readBits(5)
			if err != nil {
				return nil, err
			}
			s, err := br.readBits(6)
			if err != nil {
				return nil, err
			}
			lz = int(l)
			sigLen := int(s)
			if sigLen == 0 {
				sigLen = 64
			}
			tz = 64 - lz - sigLen
		}
		sig, err := br.readBits(64 - lz - tz)
		if err != nil {
			return nil, err
		}
		prev ^= sig << tz
		out = append(out, math.Float64frombits(prev))
	}
	return out, nil
}

// bitWriter appends bits MSB-first.
type bitWriter struct {
	buf []byte
	cur byte
	n   uint // bits used in cur
}

func (w *bitWriter) writeBit(b byte) {
	w.cur |= (b & 1) << (7 - w.n)
	w.n++
	if w.n == 8 {
		w.buf = append(w.buf, w.cur)
		w.cur, w.n = 0, 0
	}
}

func (w *bitWriter) writeBits(v uint64, n int) {
	for i := n - 1; i >= 0; i-- {
		w.writeBit(byte(v >> i))
	}
}

func (w *bitWriter) bytes() []byte {
	if w.n > 0 {
		return append(w.buf, w.cur)
	}
	return w.buf
}

// bitReader reads bits MSB-first.
type bitReader struct {
	b   []byte
	pos uint // bit position
}

func (r *bitReader) readBit() (byte, error) {
	if r.pos/8 >= uint(len(r.b)) {
		return 0, io.ErrUnexpectedEOF
	}
	bit := (r.b[r.pos/8] >> (7 - r.pos%8)) & 1
	r.pos++
	return bit, nil
}

func (r *bitReader) readBits(n int) (uint64, error) {
	var v uint64
	for i := 0; i < n; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		v = (v << 1) | uint64(bit)
	}
	return v, nil
}
