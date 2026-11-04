package vfs

import (
	"context"
	"errors"
	"testing"
)

// The built-in codecs own the text types as identity (raw bytes are the
// text); a type nobody registered is neither identity nor projected.
func TestContentRegistry_builtinsOwnTextTypes(t *testing.T) {
	r, err := NewContentRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, mt := range []string{
		"text/plain", "text/markdown", "text/x-go", "application/json",
		"application/yaml", "text/plain; charset=utf-8",
	} {
		if !r.Identity(mt) {
			t.Fatalf("Identity(%q) = false, want plaintext writable", mt)
		}
	}
	for _, mt := range []string{
		"", "application/octet-stream", "image/png",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.google-apps.document", "application/vnd.google-apps.spreadsheet",
		"application/vnd.notion.page",
	} {
		if r.Identity(mt) {
			t.Fatalf("Identity(%q) = true, want EROFS", mt)
		}
	}
	doc, err := r.Decode(context.Background(), "/x.go", "text/x-go; charset=utf-8", []byte("package x\n"))
	if err != nil {
		t.Fatalf("built-in codecs must own extension-map types: %v", err)
	}
	if doc.MediaType() != "text/x-go" {
		t.Fatalf("decoded media type %q", doc.MediaType())
	}
	if _, ok := r.Lookup("text/plain"); !ok {
		t.Fatal("lookup text/plain")
	}
}

// A second codec for a media type is refused.
func TestNewContentRegistry_refusesASecondCodecForAType(t *testing.T) {
	if _, err := NewContentRegistry(stealPlainCodec{}); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("overwrite: %v", err)
	}
}

type stealPlainCodec struct{}

func (stealPlainCodec) MediaTypes() []string { return []string{"text/plain"} }
func (stealPlainCodec) Decode(_ context.Context, _, _ string, _ []byte) (Document, error) {
	return nil, errors.New("stolen")
}

func TestBlockCodec_mediaTypes(t *testing.T) {
	c := BlockCodec{Types: []string{"application/x-t"}}
	if got := c.MediaTypes(); len(got) != 1 || got[0] != "application/x-t" {
		t.Fatalf("types=%v", got)
	}
	if _, err := c.Decode(context.Background(), "/x", "application/x-t", nil); err == nil {
		t.Fatal("nil normalizer")
	}
	if _, err := c.Encode(context.Background(), nil); err == nil {
		t.Fatal("encode nil normalizer")
	}
	doc, err := c.Create("/x", "", Mutation{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.MediaType() != "application/x-t" {
		t.Fatalf("create media type=%q", doc.MediaType())
	}
}

type tokenWrap struct{ *IR }

func TestContentToken_nonIRHashesText(t *testing.T) {
	inner := NewTextDocument("/x", "text/plain", "utf-8", "hi")
	got := ContentToken(tokenWrap{inner})
	if got != ContentHash("hi") {
		t.Fatalf("got %s want %s", got, ContentHash("hi"))
	}
}

func TestTextCodec_emptyMediaTypeIsPlain(t *testing.T) {
	for _, mt := range []string{"", "application/octet-stream"} {
		doc, err := (TextCodec{}).Decode(context.Background(), "/x", mt, []byte("hi"))
		if err != nil {
			t.Fatal(err)
		}
		if doc.MediaType() != "text/plain" {
			t.Fatalf("mediaType=%q from %q", doc.MediaType(), mt)
		}
	}
}
