package vfs

import (
	"strings"
	"testing"
)

func TestProjectHTMLSpans_tabsListsTablesMarks(t *testing.T) {
	doc := NewRichDocument("/Spec", mimeGoogleDocument, []Block{
		{Kind: BlockKindHeading, Text: "H & **x**", Style: StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.a"}}},
		{Kind: BlockKindParagraph, Text: `See _i_ and [y](https://e) and ~~s~~`, Style: StyleMeta{Attributes: map[string]string{"tab_id": "t.a"}}},
		{Kind: BlockKindListItem, Text: "one", Style: StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.a", "list_type": "ul", "list_id": "l1"}}},
		{Kind: BlockKindListItem, Text: "nest", Style: StyleMeta{Level: 2, Attributes: map[string]string{"tab_id": "t.a", "list_type": "ul", "list_id": "l1"}}},
		{Kind: BlockKindListItem, Text: "two", Style: StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.a", "list_type": "ol", "list_id": "l2"}}},
		{Kind: BlockKindTable, Text: "A\tB\nC\t**D**", Style: StyleMeta{Attributes: map[string]string{"tab_id": "t.a", "rows": "2", "cols": "2"}}},
		{Kind: BlockKindImage, Text: `alt "q"`, Style: StyleMeta{Attributes: map[string]string{"tab_id": "t.b", "object_id": "kix.1", "content_uri": "https://img"}}},
		{Kind: "quote", Text: "aside", Style: StyleMeta{Attributes: map[string]string{"tab_id": "t.b"}}},
	})
	// tabs so projection emits sections
	rb, ok := asRichBody(doc)
	if !ok {
		t.Fatal("rich body")
	}
	rb.tabs = []DocTab{{ID: "t.a", Title: "A", Index: 0}, {ID: "t.b", Title: "B", Index: 1}}
	rb.reproject()
	html := doc.Text()
	for _, want := range []string{"<strong>x</strong>", "<em>i</em>", "<a href=", "<s>s</s>", "<ul>", "<ol>", "<table>", "<section", "data-object-id", "&amp;", "&#34;"} {
		if !strings.Contains(html, want) {
			t.Fatalf("missing %q in %s", want, html)
		}
	}
	blocks, err := decodeDocsHTML([]byte(html))
	if err != nil {
		t.Fatal(err)
	}
	var sawBold, sawLink bool
	for _, b := range blocks {
		if strings.Contains(b.Text, "**x**") {
			sawBold = true
		}
		if strings.Contains(b.Text, "](https://") {
			sawLink = true
		}
	}
	if !sawBold || !sawLink {
		t.Fatalf("decode lost marks: %+v", blocks)
	}
}

func TestDecodeDocsHTML_bareTextAndEmpty(t *testing.T) {
	got, err := decodeDocsHTML([]byte("<html><body>hello from CRE</body></html>"))
	if err != nil || len(got) != 1 || got[0].Kind != BlockKindParagraph || got[0].Text != "hello from CRE" {
		t.Fatalf("bare text: %+v err=%v", got, err)
	}
	empty, err := decodeDocsHTML([]byte("<html><body><div></div></body></html>"))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty: %+v err=%v", empty, err)
	}
}
