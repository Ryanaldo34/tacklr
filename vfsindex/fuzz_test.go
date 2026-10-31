package vfsindex

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzChunks(f *testing.F) {
	f.Add("alpha line\nbeta line\ngamma line\n", 8, 16, 32)
	f.Add(strings.Repeat("€", 200), 8, 16, 32)
	f.Add("top\n"+strings.Repeat("\n", 60)+"bottom\n", 8, 16, 32)
	f.Add(lines(40), 0, 0, 0)
	f.Add("a\n"+strings.Repeat("x", 100), 8, 16, 32)
	f.Add("𝄞𝄞𝄞", 1, 2, 4)
	f.Fuzz(func(t *testing.T, text string, minSize, avg, maxSize int) {
		sizes, err := ChunkSizes{Min: minSize, Avg: avg, Max: maxSize}.withDefaults()
		if err != nil || sizes.Max > 1<<16 {
			t.Skip()
		}

		got := split(t, text, sizes)

		var b strings.Builder
		line, offset := 1, int64(0)
		for i, ch := range got {
			span := text[ch.ByteStart:ch.ByteEnd]
			b.WriteString(span)
			if ch.StartLine != line || ch.ByteStart != offset || ch.EndLine < ch.StartLine {
				t.Fatalf("chunk %d starts at line %d byte %d (ends line %d), want line %d byte %d", i, ch.StartLine, ch.ByteStart, ch.EndLine, line, offset)
			}
			if n := len(span); n > sizes.Max || (i < len(got)-1 && n < sizes.Min) {
				t.Fatalf("chunk %d is %d bytes, outside [%d, %d]", i, n, sizes.Min, sizes.Max)
			}
			if utf8.ValidString(text) && !utf8.ValidString(ch.Text) {
				t.Fatalf("chunk %d of valid UTF-8 is not valid UTF-8: %q", i, ch.Text)
			}
			line, offset = ch.EndLine+1, ch.ByteEnd
			if strings.HasSuffix(span, "\n") {
				continue
			}
			line = ch.EndLine // the next chunk continues this line
		}
		if b.String() != text {
			t.Fatalf("chunks rebuild %q, want %q", b.String(), text)
		}
	})
}
