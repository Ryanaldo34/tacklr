package vfs

import (
	"errors"
	"strings"
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

func TestEncodeDocument_richFallsBackToHTML(t *testing.T) {
	rd := NewRichDocument("/Spec", mimeGoogleDocument, []Block{{Kind: BlockKindParagraph, Text: "**Hi**"}})
	data, err := EncodeDocument(t.Context(), rd)
	if err != nil || !strings.Contains(string(data), "<strong>Hi</strong>") {
		t.Fatalf("encode = %q err=%v", data, err)
	}
}
