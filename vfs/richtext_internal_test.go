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
}
