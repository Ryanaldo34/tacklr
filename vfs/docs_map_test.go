package vfs

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/api/docs/v1"
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

func TestSplitInsertHead_tableBoundary(t *testing.T) {
	blocks := []Block{
		{Kind: BlockKindHeading, Text: "H"},
		{Kind: BlockKindParagraph, Text: "P"},
		{Kind: BlockKindTable, Text: "A\tB"},
		{Kind: BlockKindParagraph, Text: "After"},
	}
	head, rest := splitInsertHead(blocks)
	if len(head) != 2 || head[0].Text != "H" || len(rest) != 2 || rest[0].Kind != BlockKindTable {
		t.Fatalf("prefix head=%+v rest=%+v", head, rest)
	}
	head, rest = splitInsertHead(rest)
	if len(head) != 1 || head[0].Kind != BlockKindTable || len(rest) != 1 || rest[0].Text != "After" {
		t.Fatalf("table head=%+v rest=%+v", head, rest)
	}
	head, rest = splitInsertHead(rest)
	if len(head) != 1 || head[0].Text != "After" || rest != nil {
		t.Fatalf("tail head=%+v rest=%+v", head, rest)
	}
}

func TestMapInsertBlocks_stripsMarksAndEmitsTextStyle(t *testing.T) {
	chunks, _, err := mapInsertBlocks([]Block{
		{Kind: BlockKindParagraph, Text: "See **x** and [Maya](mailto:maya)"},
		{Kind: BlockKindHeading, Text: "**Title**", Style: StyleMeta{Level: 1}},
		{Kind: BlockKindListItem, Text: "**item**", Style: StyleMeta{Level: 1}},
	}, 1, "t")
	if err != nil || len(chunks) != 1 {
		t.Fatalf("chunks=%d err=%v", len(chunks), err)
	}
	var inserted []string
	var bold, link int
	for _, r := range chunks[0].reqs {
		if r.InsertText != nil {
			inserted = append(inserted, r.InsertText.Text)
		}
		if st := r.UpdateTextStyle; st != nil && st.TextStyle != nil {
			if st.TextStyle.Bold {
				bold++
			}
			if st.TextStyle.Link != nil && st.TextStyle.Link.Url == "mailto:maya" {
				link++
			}
		}
	}
	if strings.Contains(strings.Join(inserted, ""), "**") {
		t.Fatalf("insert kept markdown: %q", inserted)
	}
	if bold < 3 || link < 1 {
		t.Fatalf("styles bold=%d link=%d inserts=%q reqs=%+v", bold, link, inserted, chunks[0].reqs)
	}
}

func TestEncodeDocument_richFallsBackToHTML(t *testing.T) {
	rd := NewRichDocument("/Spec", mimeGoogleDocument, []Block{{Kind: BlockKindParagraph, Text: "**Hi**"}})
	data, err := EncodeDocument(t.Context(), rd)
	if err != nil || !strings.Contains(string(data), "<strong>Hi</strong>") {
		t.Fatalf("encode = %q err=%v", data, err)
	}
}

func TestDocsWalkHelpers(t *testing.T) {
	if clampHeading(0) != 1 || clampHeading(9) != 6 || clampHeading(3) != 3 {
		t.Fatal("clamp")
	}
	if _, _, err := tableShape(Block{}); err == nil {
		t.Fatal("empty table")
	}
	if _, _, err := tableShape(Block{Text: "a\tb\nc"}); err == nil {
		t.Fatal("ragged")
	}
	if r, c, err := tableShape(Block{Style: StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "3"}}}); err != nil || r != 2 || c != 3 {
		t.Fatalf("attrs %d %d %v", r, c, err)
	}
	_ = keepImageObjectIDs([]Block{
		{Kind: BlockKindParagraph},
		{Kind: BlockKindImage, Style: StyleMeta{Attributes: map[string]string{"object_id": "kix.z"}}},
		{Kind: BlockKindImage, Style: StyleMeta{Attributes: map[string]string{"object_id": "img-html-x"}}},
	})
	if _, err := unzipHTML(nil); err == nil {
		t.Fatal("unzip empty")
	}
	if b, err := unzipHTML([]byte("<p>x</p>")); err != nil || string(b) != "<p>x</p>" {
		t.Fatalf("html %q %v", b, err)
	}
	if walkBodySpans(tabBody{}) != nil {
		t.Fatal("empty body")
	}
	dst := map[string]DocsListProps{}
	mergeListProps(dst, nil)
	mergeListProps(dst, map[string]docs.List{"x": {}})
	if !glyphOrdered("DECIMAL") || !glyphOrdered("weirdDECIMAL") || glyphOrdered("bullet") {
		t.Fatal("glyph")
	}
	el := &docs.StructuralElement{StartIndex: 1, EndIndex: 2}
	if structuralToSpans(el, tabBody{}) != nil {
		t.Fatal("default kind")
	}
	spans := paragraphSpans(&docs.StructuralElement{
		StartIndex: 1, EndIndex: 4,
		Paragraph: &docs.Paragraph{
			Bullet:   &docs.Bullet{ListId: "l", NestingLevel: 1},
			Elements: []*docs.ParagraphElement{nil, {TextRun: &docs.TextRun{Content: "x\n"}}},
		},
	}, tabBody{ID: "t"})
	if len(spans) == 0 {
		t.Fatal("bullet para")
	}
	_ = paragraphText(&docs.Paragraph{Elements: []*docs.ParagraphElement{nil}})
	if kernelWritable("") || IsProjected("") || !kernelCreateOK("readme") {
		t.Fatal("registry helpers")
	}
	if _, err := EncodeDocument(t.Context(), NewTextDocument("/a.txt", "text/plain", "utf-8", "z")); err != nil {
		t.Fatal(err)
	}
	if DetectMediaType("n.docx", nil) == "" {
		t.Fatal("docx mime")
	}
	rd := snapshotToRich("/x", DocsSnapshot{Body: []DocsSpan{
		{Kind: "sectionBreak"},
		{Kind: "tableOfContents"},
		{Kind: "heading", Text: "H", Level: 0},
		{Kind: "heading", Text: "H2", Level: 9},
		{Kind: "list_item", Text: "i", Level: 1, ListID: "l1"},
		{Kind: "table", Text: "A\tB", Cells: []DocsCell{{Text: "A"}}},
		{Kind: "image", ObjectID: "kix.z", NamedStyle: "https://e"},
		{Kind: "paragraph", Text: "p"},
		{Kind: "nope"},
	}})
	if len(rd.Blocks()) < 4 {
		t.Fatalf("blocks=%d", len(rd.Blocks()))
	}
	if allDocumentTabs(nil) != nil || allDocumentTabs(&docs.Document{}) != nil {
		t.Fatal("empty tabs")
	}
	if len(allDocumentTabs(&docs.Document{Body: &docs.Body{}})) != 1 {
		t.Fatal("legacy body")
	}
	_ = allDocumentTabs(&docs.Document{Tabs: []*docs.Tab{
		nil,
		{ChildTabs: []*docs.Tab{{DocumentTab: &docs.DocumentTab{Body: &docs.Body{}}}}},
	}})
	_ = snapshotFromDocument(nil)
	_ = paragraphSpans(&docs.StructuralElement{
		StartIndex: 1, EndIndex: 3,
		Paragraph: &docs.Paragraph{
			ParagraphStyle: &docs.ParagraphStyle{NamedStyleType: "HEADING_7"},
			Elements:       []*docs.ParagraphElement{{TextRun: &docs.TextRun{Content: "H\n"}}},
		},
	}, tabBody{})
	_ = paragraphSpans(&docs.StructuralElement{
		StartIndex: 1, EndIndex: 3,
		Paragraph: &docs.Paragraph{
			ParagraphStyle: &docs.ParagraphStyle{NamedStyleType: "HEADING_"},
			Elements:       []*docs.ParagraphElement{{TextRun: &docs.TextRun{Content: "H\n"}}},
		},
	}, tabBody{})
	_ = paragraphSpans(&docs.StructuralElement{
		StartIndex: 1, EndIndex: 2,
		Paragraph: &docs.Paragraph{Elements: []*docs.ParagraphElement{{
			InlineObjectElement: &docs.InlineObjectElement{InlineObjectId: "kix.z"},
		}}},
	}, tabBody{Objs: map[string]docs.InlineObject{
		"kix.z": {InlineObjectProperties: &docs.InlineObjectProperties{
			EmbeddedObject: &docs.EmbeddedObject{Title: "t", ImageProperties: &docs.ImageProperties{ContentUri: "https://e"}},
		}},
	}})
}
