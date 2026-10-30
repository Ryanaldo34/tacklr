package vfsindex

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

// split chunks text with sizes and fails the test on an error.
func split(t *testing.T, text string, sizes ChunkSizes) []chunk {
	t.Helper()
	var out []chunk
	for ch, err := range chunks(t.Context(), strings.NewReader(text), sizes, 0) {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ch)
	}
	return out
}

func lines(n int) string {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "line %04d holds a fact about topic %d\n", i, i*7)
	}
	return b.String()
}

func TestChunks_coverTheInputInOrderWithinTheSizeBounds(t *testing.T) {
	text := lines(500)

	got := split(t, text, DefaultChunkSizes)

	var b strings.Builder
	line, offset := 1, int64(0)
	for i, ch := range got {
		b.WriteString(text[ch.ByteStart:ch.ByteEnd])
		if ch.StartLine != line || ch.ByteStart != offset {
			t.Fatalf("chunk %d starts at line %d byte %d, want line %d byte %d", i, ch.StartLine, ch.ByteStart, line, offset)
		}
		if n := ch.ByteEnd - ch.ByteStart; n > int64(DefaultChunkSizes.Max) || (i < len(got)-1 && n < int64(DefaultChunkSizes.Min)) {
			t.Fatalf("chunk %d is %d bytes, outside [%d, %d]", i, n, DefaultChunkSizes.Min, DefaultChunkSizes.Max)
		}
		line, offset = ch.EndLine+1, ch.ByteEnd
	}
	if b.String() != text || line != 501 {
		t.Fatalf("chunks rebuild %d bytes ending before line %d, want the %d-byte input and line 501", b.Len(), line, len(text))
	}
}

func TestChunks_anInsertChangesOnlyTheChunksAroundIt(t *testing.T) {
	text := lines(2000)
	mid := strings.Index(text, "line 1000")
	edited := text[:mid] + "an inserted line\n" + text[mid:]
	before := map[string]bool{}
	for _, ch := range split(t, text, DefaultChunkSizes) {
		before[ch.Text] = true
	}

	after := split(t, edited, DefaultChunkSizes)

	changed := 0
	for _, ch := range after {
		if !before[ch.Text] {
			changed++
		}
	}
	if changed == 0 || changed > 2 {
		t.Fatalf("%d of %d chunks changed after a one-line insert, want 1 or 2", changed, len(after))
	}
}

func TestChunks_aLineLongerThanMaxSplitsOnRuneBoundaries(t *testing.T) {
	// Three-byte runes: a cut at Max lands inside a rune and backs off.
	line := strings.Repeat("€", 7_000)

	got := split(t, line, DefaultChunkSizes)

	var b strings.Builder
	for _, ch := range got {
		if !utf8.ValidString(ch.Text) || ch.StartLine != 1 || ch.EndLine != 1 {
			t.Fatalf("chunk of %d bytes: valid UTF-8 %v, lines %d-%d", len(ch.Text), utf8.ValidString(ch.Text), ch.StartLine, ch.EndLine)
		}
		b.WriteString(ch.Text)
	}
	if len(got) < 3 || b.String() != line {
		t.Fatalf("%d chunks rebuild the line: %v", len(got), b.String() == line)
	}
}
