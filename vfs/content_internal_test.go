package vfs

import (
	"strings"
	"testing"
)

// TestMediaTypeOf_takesTheDeclaredTypeElseTheNameElseTheBytes: a type a
// backend declares wins without its parameters, and octet-stream or no
// type falls back to the extension, then to at most 512 sniffed bytes:
// UTF-8 text, even with a control byte, is plain text, and sniffed text
// that is not UTF-8 is bytes.
func TestMediaTypeOf_takesTheDeclaredTypeElseTheNameElseTheBytes(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		declared, file string
		sample         []byte
		want           string
	}{
		"a declared type with parameters":         {"Text/Markdown; charset=utf-8", "notes", nil, "text/markdown"},
		"a declared type over the extension":      {"image/png", "blob.txt", nil, "image/png"},
		"a Google type":                           {"application/vnd.google-apps.document", "Spec", nil, "application/vnd.google-apps.document"},
		"octet-stream falls back to the name":     {"application/octet-stream", "main.go", nil, "text/x-go"},
		"S3's binary type falls back too":         {"binary/octet-stream", "a.md", nil, "text/markdown"},
		"no type and no extension is bytes":       {"", "blob", nil, "application/octet-stream"},
		"a long sample sniffs its start":          {"", "README", []byte(strings.Repeat("a", 600) + "\x00"), "text/plain"},
		"text with a control byte is text":        {"", "README", []byte("hello\x01world"), "text/plain"},
		"sniffed HTML that is not UTF-8 is bytes": {"", "page", []byte("<html>\xff"), "application/octet-stream"},
	} {
		got := mediaTypeOf(c.declared, c.file, c.sample)

		if got != c.want {
			t.Errorf("%s: mediaTypeOf(%q, %q) = %q, want %q", name, c.declared, c.file, got, c.want)
		}
	}
}

// TestFIFO_dropsTheOldestKeyWhenFull: a full cache drops its oldest key
// for a new one, and a key that takes a new value keeps its place.
func TestFIFO_dropsTheOldestKeyWhenFull(t *testing.T) {
	t.Parallel()
	c := newFIFO[int](2)
	c.put("a", 1)
	c.put("b", 2)
	c.put("a", 3)

	c.put("c", 4)

	_, hasA := c.get("a")
	b, hasB := c.get("b")
	if hasA || !hasB || b != 2 {
		t.Fatalf("after a third key: a kept = %v, b = %d %v, want a dropped and b kept", hasA, b, hasB)
	}
}
