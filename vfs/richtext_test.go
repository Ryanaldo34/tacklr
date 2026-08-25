package vfs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

type richNormalizer struct {
	encoded []vfs.Block
}

func (n *richNormalizer) DecodeBlocks(context.Context, string, string, []byte) ([]vfs.Block, error) {
	return []vfs.Block{{ID: "intro", Kind: vfs.BlockKindParagraph, Text: "original"}}, nil
}

func (n *richNormalizer) EncodeBlocks(_ context.Context, blocks []vfs.Block) ([]byte, error) {
	n.encoded = blocks
	return []byte("source-format"), nil
}

func TestBlockCodecProjectsAndEncodesEditedCanonicalDocument(t *testing.T) {
	ctx := context.Background()
	n := &richNormalizer{}
	codec := vfs.BlockCodec{Types: []string{"application/x-test-rich"}, Normalizer: n}
	reg := vfs.NewContentRegistry()
	if err := reg.Register(codec); err != nil {
		t.Fatal(err)
	}

	doc, err := codec.Decode(ctx, "/work/example.rich", "application/x-test-rich", []byte("source"))
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("decoded document = %#v, want rich body", doc)
	}
	if err := doc.(vfs.Textual).SetText("<p>edited</p>"); err != nil {
		t.Fatalf("SetText HTML: %v", err)
	}
	if got := rd.Blocks(); len(got) != 1 || got[0].Kind != vfs.BlockKindParagraph || got[0].Text != "edited" {
		t.Fatalf("blocks after SetText = %#v", rd.Blocks())
	}

	rd.SetBlocks([]vfs.Block{{ID: "intro", Kind: vfs.BlockKindParagraph, Text: "edited"}})
	encoded, err := codec.Encode(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "source-format" || n.encoded[0].Text != "edited" {
		t.Fatalf("encoded=%q doc=%#v", encoded, n.encoded)
	}
}

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
