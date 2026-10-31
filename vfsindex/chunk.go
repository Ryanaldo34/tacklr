package vfsindex

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/bits"
	"unicode/utf8"
)

// ChunkSizes bounds the content-defined chunks a file is split into, in
// bytes. A chunk ends at the first line end after a content-defined cut
// point, so boundaries depend only on nearby bytes: an edit changes the
// chunks around it, and the chunks before and after it keep their text
// (and so their brain ids and vectors). Size them for the embedding model:
// about 4 bytes of English text make one token.
type ChunkSizes struct {
	// Min is the smallest chunk, except the last one of a file. Default 1 KiB.
	Min int
	// Avg is the target mean chunk size. Default 2 KiB.
	Avg int
	// Max is the largest chunk. A chunk that reaches Max ends at its last
	// line end, or at Max when it has no line end past Min. Default 8 KiB.
	Max int
}

// DefaultChunkSizes suits current embedding models (about 500 tokens a chunk).
var DefaultChunkSizes = ChunkSizes{Min: 1 << 10, Avg: 2 << 10, Max: 8 << 10}

// withDefaults fills zero sizes from DefaultChunkSizes and checks the order.
func (c ChunkSizes) withDefaults() (ChunkSizes, error) {
	d := DefaultChunkSizes
	c = ChunkSizes{Min: cmp.Or(c.Min, d.Min), Avg: cmp.Or(c.Avg, d.Avg), Max: cmp.Or(c.Max, d.Max)}
	if c.Min <= 0 || c.Min >= c.Avg || c.Avg > c.Max || c.Max < utf8.UTFMax {
		return ChunkSizes{}, fmt.Errorf("vfsindex: chunk sizes need Min < Avg <= Max and Max >= %d, got %d, %d, %d", utf8.UTFMax, c.Min, c.Avg, c.Max)
	}
	return c, nil
}

// chunk is one span of a file: its text without the final line end, 1-based
// inclusive lines, and the half-open byte range.
type chunk struct {
	Text      string
	StartLine int
	EndLine   int
	ByteStart int64
	ByteEnd   int64
}

// chunks splits r into content-defined chunks as it reads, holding at most
// one chunk and the read buffer. It stops after maxBytes (maxBytes <= 0
// means no limit).
func chunks(ctx context.Context, r io.Reader, sizes ChunkSizes, maxBytes int64) iter.Seq2[chunk, error] {
	return func(yield func(chunk, error) bool) {
		br := bufio.NewReaderSize(r, 64<<10)
		c := cutter{sizes: sizes, mask: cutMask(sizes), line: 1, buf: make([]byte, 0, sizes.Max)}
		for read := int64(0); maxBytes <= 0 || read < maxBytes; read++ {
			if read%(64<<10) == 0 {
				if err := ctx.Err(); err != nil {
					yield(chunk{}, err)
					return
				}
			}
			b, err := br.ReadByte()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				yield(chunk{}, err)
				return
			}
			if ch, ok := c.push(b); ok && !yield(ch, nil) {
				return
			}
		}
		if ch, ok := c.flush(len(c.buf)); ok {
			yield(ch, nil)
		}
	}
}

// cutter is the chunking state machine: a Gear rolling hash over the last
// 64 bytes picks cut points; a cut waits for the end of its line.
type cutter struct {
	sizes   ChunkSizes
	mask    uint64
	hash    uint64
	pending bool // a cut point was seen; cut at the next line end
	buf     []byte
	lastNL  int // index after the last '\n' in buf, 0 when none
	line    int // line number of buf[0]
	offset  int64
}

func (c *cutter) push(b byte) (chunk, bool) {
	c.buf = append(c.buf, b)
	c.hash = c.hash<<1 + gearTable[b]
	if len(c.buf) >= c.sizes.Min && c.hash&c.mask == 0 {
		c.pending = true
	}
	if b == '\n' {
		c.lastNL = len(c.buf)
		if c.pending {
			return c.flush(len(c.buf))
		}
	}
	if len(c.buf) < c.sizes.Max {
		return chunk{}, false
	}
	if c.lastNL >= c.sizes.Min {
		return c.flush(c.lastNL)
	}
	return c.flush(runeStart(c.buf, len(c.buf)))
}

// flush emits buf[:n] as a chunk and keeps buf[n:] as the next one's start.
func (c *cutter) flush(n int) (chunk, bool) {
	if n == 0 {
		return chunk{}, false
	}
	span := c.buf[:n]
	lines := bytes.Count(span, []byte{'\n'})
	end := c.line + lines
	if span[n-1] == '\n' {
		end-- // the next chunk starts on a new line
	}
	text := bytes.TrimSuffix(bytes.TrimSuffix(span, []byte{'\n'}), []byte{'\r'})
	ch := chunk{Text: string(text), StartLine: c.line, EndLine: end, ByteStart: c.offset, ByteEnd: c.offset + int64(n)}
	c.line += lines
	c.offset += int64(n)
	rest := copy(c.buf, c.buf[n:])
	c.buf = c.buf[:rest]
	c.lastNL = bytes.LastIndexByte(c.buf, '\n') + 1
	c.pending = false
	return ch, true
}

// cutMask makes a cut point appear on average every Avg-Min bytes past Min.
// It tests the high bits: bit k of a Gear hash depends on the last k+1
// bytes only, so low bits repeat on repetitive text.
func cutMask(s ChunkSizes) uint64 {
	return ^uint64(0) << (64 - bits.Len(uint(s.Avg-s.Min)-1))
}

// runeStart backs n off to the start of a UTF-8 rune in b.
func runeStart(b []byte, n int) int {
	for i := n; i > 0 && n-i < utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i-1]) {
			if utf8.FullRune(b[i-1 : n]) {
				return n
			}
			return i - 1
		}
	}
	return n
}

// isBinarySample reports whether the first bytes of a file are not text.
func isBinarySample(sample []byte) bool {
	// A sample cut mid-rune is still text: only check whole runes.
	return len(sample) > 0 && (bytes.IndexByte(sample, 0) >= 0 || !utf8.Valid(sample[:runeStart(sample, len(sample))]))
}

// gearTable is the Gear hash table. Chunk ids depend on where cuts fall, so
// this table must never change: a new table re-embeds every indexed file.
var gearTable = func() (t [256]uint64) {
	// splitmix64 from a fixed seed.
	x := uint64(0x9e3779b97f4a7c15)
	for i := range t {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
		z = (z ^ z>>27) * 0x94d049bb133111eb
		t[i] = z ^ z>>31
	}
	return t
}()
