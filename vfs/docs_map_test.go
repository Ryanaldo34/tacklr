package vfs

import (
	"errors"
	"testing"
)

func TestMapReplace_imageAndEmptyAndTableMarks(t *testing.T) {
	if _, err := mapReplaceBlock(blockLocation{startIndex: 1, endIndex: 2}, Block{Kind: BlockKindImage}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("image: %v", err)
	}
	reqs, err := mapReplaceBlock(blockLocation{startIndex: 5, endIndex: 6, tabID: "t"}, Block{Kind: BlockKindParagraph, Text: ""})
	if err != nil || reqs != nil {
		t.Fatalf("empty para: %v %v", reqs, err)
	}
	tbl := Block{
		Kind: BlockKindTable, Text: "A\t**B**",
		Style: StyleMeta{Attributes: map[string]string{"rows": "1", "cols": "2"}},
	}
	normalizeInline(&tbl)
	reqs, err = mapReplaceBlock(blockLocation{
		startIndex: 1, endIndex: 10, tabID: "t",
		cells: []cellLocation{{0, 0, 1, 3}, {0, 1, 3, 6}},
	}, tbl)
	if err != nil {
		t.Fatal(err)
	}
	var styles int
	for _, r := range reqs {
		if r.UpdateTextStyle != nil {
			styles++
		}
	}
	if styles == 0 {
		t.Fatalf("table marks missing styles: %+v", reqs)
	}
	emoji := Block{Kind: BlockKindParagraph, Text: "😀**x**"}
	normalizeInline(&emoji)
	if _, err := mapReplaceBlock(blockLocation{startIndex: 1, endIndex: 8}, emoji); err != nil {
		t.Fatal(err)
	}
	if _, err := mapReplaceTable(blockLocation{}, Block{Kind: BlockKindTable, Text: "x", Style: StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "2"}}}); err == nil {
		t.Fatal("shape")
	}
}

func TestParagraphInsertIndex_skipsSectionBreak(t *testing.T) {
	spans := []DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 3, Kind: "paragraph"},
	}
	if got := paragraphInsertIndex(spans, ""); got != 2 {
		t.Fatalf("got %d want 2", got)
	}
	if got := paragraphInsertIndex([]DocsSpan{{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"}}, ""); got != 1 {
		t.Fatalf("fallback %d", got)
	}
	after := []DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 10, Kind: "heading"},
		{StartIndex: 10, EndIndex: 40, Kind: "table"},
		{StartIndex: 40, EndIndex: 41, Kind: "paragraph"},
	}
	if got := paragraphAppendIndex(after, ""); got != 40 {
		t.Fatalf("append after table %d", got)
	}
}

// Only an image that came from the document keeps its object: an image
// decoded from HTML has no Docs object to keep.
func TestKeepImageObjectIDs_keepsOnlyDocumentImages(t *testing.T) {
	got := keepImageObjectIDs([]Block{
		{Kind: BlockKindParagraph},
		{Kind: BlockKindImage, Style: StyleMeta{Attributes: map[string]string{"object_id": "kix.z"}}},
		{Kind: BlockKindImage, Style: StyleMeta{Attributes: map[string]string{"object_id": "img-html-x"}}},
	})
	if _, ok := got["kix.z"]; len(got) != 1 || !ok {
		t.Fatalf("kept = %v, want only kix.z", got)
	}
}
