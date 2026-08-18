package vfs_test

import (
	"context"
	"encoding/json"
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
	text, ok := doc.(*vfs.TextDocument)
	if !ok || text.Text() == "" {
		t.Fatalf("decoded document = %#v", doc)
	}
	if got := text.Blocks(); len(got) != 1 || got[0].ID != "intro" {
		t.Fatalf("blocks = %#v", got)
	}

	var canonical vfs.RichTextDocument
	if err := json.Unmarshal([]byte(text.Text()), &canonical); err != nil {
		t.Fatal(err)
	}
	canonical.Blocks[0].Text = "edited"
	body, err := json.MarshalIndent(canonical, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	text.SetText(string(body))
	encoded, err := codec.Encode(ctx, text)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "source-format" || n.encoded.Blocks[0].Text != "edited" {
		t.Fatalf("encoded=%q doc=%#v", encoded, n.encoded)
	}
}
