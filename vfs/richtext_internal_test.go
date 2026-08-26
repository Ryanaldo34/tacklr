package vfs

import (
	"context"
	"testing"
)

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
