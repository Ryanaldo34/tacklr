package vfs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

type richNormalizer struct {
	encoded *vfs.RichTextDocument
}

func (n *richNormalizer) DecodeRich(context.Context, string, string, []byte) (*vfs.RichTextDocument, error) {
	return &vfs.RichTextDocument{Blocks: []vfs.RichTextBlock{{ID: "intro", Kind: "paragraph", Text: "original"}}}, nil
}

func (n *richNormalizer) EncodeRich(_ context.Context, doc *vfs.RichTextDocument) ([]byte, error) {
	n.encoded = doc
	return []byte("source-format"), nil
}

func TestRichTextCodecProjectsAndEncodesEditedCanonicalDocument(t *testing.T) {
	ctx := context.Background()
	n := &richNormalizer{}
	codec := vfs.RichTextCodec{Types: []string{"application/x-test-rich"}, Normalizer: n}
	reg := vfs.NewContentRegistry()
	if err := reg.Register(codec); err != nil {
		t.Fatal(err)
	}

	doc, err := codec.Decode(ctx, "/work/example.rich", "application/x-test-rich", []byte("source"))
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := doc.(*vfs.RichDocument)
	if !ok {
		t.Fatalf("decoded document = %#v, want *RichDocument", doc)
	}
	if err := rd.SetText("nope"); !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("SetText = %v, want ErrProjected", err)
	}
	if got := rd.Blocks(); len(got) != 1 || got[0].Kind != vfs.BlockKindParagraph || got[0].Text != "original" {
		t.Fatalf("blocks = %#v", got)
	}

	rd.SetBlocks([]vfs.Block{{ID: "intro", Kind: vfs.BlockKindParagraph, Text: "edited"}})
	encoded, err := codec.Encode(ctx, rd)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "source-format" || n.encoded.Blocks[0].Text != "edited" {
		t.Fatalf("encoded=%q doc=%#v", encoded, n.encoded)
	}
}

func TestRichTextCodec_errorsAndEmpty(t *testing.T) {
	ctx := context.Background()
	empty := vfs.RichTextCodec{Types: []string{"application/x-test-rich"}}
	if _, err := empty.Decode(ctx, "/x", "application/x-test-rich", nil); err == nil {
		t.Fatal("nil normalizer decode")
	}
	if _, err := empty.Encode(ctx, &vfs.RichDocument{}); err == nil {
		t.Fatal("nil normalizer encode")
	}
	bad := &richNormalizer{}
	codec := vfs.RichTextCodec{Types: []string{"application/x-test-rich"}, Normalizer: failRich{}}
	if _, err := codec.Decode(ctx, "/x", "application/x-test-rich", nil); err == nil {
		t.Fatal("decode fail")
	}
	if _, err := codec.Encode(ctx, vfs.NewTextDocument("/t", "text/plain", "utf-8", "not-json")); err == nil {
		t.Fatal("bad json encode")
	}
	if _, err := codec.Encode(ctx, onlyDoc{}); err == nil {
		t.Fatal("non-textual encode")
	}
	_ = bad
}

type onlyDoc struct{}

func (onlyDoc) Path() string      { return "/x" }
func (onlyDoc) MediaType() string { return "x" }

type failRich struct{}

func (failRich) DecodeRich(context.Context, string, string, []byte) (*vfs.RichTextDocument, error) {
	return nil, errors.New("nope")
}
func (failRich) EncodeRich(context.Context, *vfs.RichTextDocument) ([]byte, error) {
	return nil, errors.New("nope")
}
