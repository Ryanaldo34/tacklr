package vfs

import (
	"context"
	"testing"
)

type staticRich struct {
	doc *RichTextDocument
	err error
}

func (s staticRich) DecodeRich(context.Context, string, string, []byte) (*RichTextDocument, error) {
	return s.doc, s.err
}
func (s staticRich) EncodeRich(context.Context, *RichTextDocument) ([]byte, error) {
	return []byte("ok"), nil
}

func TestRichTextValidateAndChildren(t *testing.T) {
	ctx := context.Background()
	try := func(doc *RichTextDocument) error {
		c := RichTextCodec{Types: []string{"application/x-t"}, Normalizer: staticRich{doc: doc}}
		_, err := c.Decode(ctx, "/x", "application/x-t", nil)
		return err
	}
	if err := try(nil); err == nil {
		t.Fatal("nil doc")
	}
	if err := try(&RichTextDocument{Schema: "nope", Blocks: []RichTextBlock{{ID: "a", Kind: "paragraph"}}}); err == nil {
		t.Fatal("schema")
	}
	if err := try(&RichTextDocument{Blocks: []RichTextBlock{{ID: "", Kind: "paragraph"}}}); err == nil {
		t.Fatal("empty id")
	}
	if err := try(&RichTextDocument{Blocks: []RichTextBlock{
		{ID: "a", Kind: "paragraph"}, {ID: "a", Kind: "paragraph"},
	}}); err == nil {
		t.Fatal("dup id")
	}
	if err := try(&RichTextDocument{Blocks: []RichTextBlock{{
		ID: "a", Kind: "paragraph", Runs: []RichTextRun{{Text: "a\nb"}},
	}}}); err == nil {
		t.Fatal("newline run")
	}
	if err := try(&RichTextDocument{Blocks: []RichTextBlock{{
		ID: "a", Kind: "paragraph",
		Children: []RichTextBlock{{ID: "b", Kind: "list-item", Text: "c", Attributes: map[string]string{"k": "v"}}},
	}}}); err != nil {
		t.Fatalf("children: %v", err)
	}
	// Encode via JSON TextDocument + registry Encoder
	c := RichTextCodec{Types: []string{"application/x-t"}, Normalizer: staticRich{doc: &RichTextDocument{
		Schema: RichTextSchema,
		Blocks: []RichTextBlock{{ID: "a", Kind: "paragraph", Text: "x"}},
	}}}
	if _, err := c.Encode(ctx, NewTextDocument("/t", "text/plain", "utf-8", `{"schema":"`+RichTextSchema+`","blocks":[{"id":"a","kind":"paragraph","text":"x"}]}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeDocument(ctx, nil); err == nil {
		t.Fatal("encode nil")
	}
	if runs := adapterRunsToIR(RichTextBlock{Text: "plain"}); len(runs) != 1 || runs[0].Text != "plain" {
		t.Fatalf("plain runs=%v", runs)
	}
	if adapterRunsToIR(RichTextBlock{}) != nil {
		t.Fatal("empty")
	}
	runs := adapterRunsToIR(RichTextBlock{Runs: []RichTextRun{{
		Text: "x",
		Attributes: map[string]string{
			"b": "true", "bold": "false", "i": "true", "strike": "true",
			"href": "https://e", "s": "false", "nope": "1",
		},
	}}})
	if len(runs) != 1 || runs[0].Marks[MarkBold] != "true" || runs[0].Marks[MarkHref] != "https://e" {
		t.Fatalf("marks=%v", runs)
	}
	if adapterRichKind(BlockKindListItem) != "list-item" || adapterRichKind("paragraph") != "paragraph" {
		t.Fatal("kind")
	}
}
