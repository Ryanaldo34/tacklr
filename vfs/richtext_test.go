package vfs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestBlockCodec_errorsAndEmpty(t *testing.T) {
	ctx := context.Background()
	empty := vfs.BlockCodec{Types: []string{"application/x-test-rich"}}
	if _, err := empty.Decode(ctx, "/x", "application/x-test-rich", nil); err == nil {
		t.Fatal("nil normalizer decode")
	}
	if _, err := empty.Encode(ctx, vfs.NewRichDocument("/x", "application/x-test-rich", nil)); err == nil {
		t.Fatal("nil normalizer encode")
	}
	codec := vfs.BlockCodec{Types: []string{"application/x-test-rich"}, Normalizer: failRich{}}
	if _, err := codec.Decode(ctx, "/x", "application/x-test-rich", nil); err == nil {
		t.Fatal("decode fail")
	}
	if _, err := codec.Encode(ctx, vfs.NewTextDocument("/t", "text/plain", "utf-8", "not-json")); err == nil {
		t.Fatal("non-rich encode")
	}
	if _, err := codec.Encode(ctx, onlyDoc{}); err == nil {
		t.Fatal("non-textual encode")
	}
}

type onlyDoc struct{}

func (onlyDoc) Path() string      { return "/x" }
func (onlyDoc) MediaType() string { return "x" }

type failRich struct{}

func (failRich) DecodeBlocks(context.Context, string, string, []byte) ([]vfs.Block, error) {
	return nil, errors.New("nope")
}
func (failRich) EncodeBlocks(context.Context, []vfs.Block) ([]byte, error) {
	return nil, errors.New("nope")
}
