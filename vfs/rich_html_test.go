package vfs_test

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// decodeDocsHTML is the blocks the Docs codec reads from html.
func decodeDocsHTML(t *testing.T, html string) []vfs.Block {
	t.Helper()
	doc, err := (vfs.DocsCodec{}).Decode(t.Context(), "/Spec", googleDocMIME, []byte(html))
	if err != nil {
		t.Fatal(err)
	}
	return doc.(vfs.Structured).Blocks()
}

// A rich document projects its blocks as HTML: headings at levels 1 to 6,
// inline marks, nested and ordered lists (a list item at level 0 is at
// level 1), tables, images and escaped text. The Docs codec reads the
// projection back with its marks.
func TestProjectHTMLSpans_tabsListsTablesMarks(t *testing.T) {
	t.Parallel()
	doc := vfs.NewRichDocument("/Spec", googleDocMIME, []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "H & **x**", Style: vfs.StyleMeta{Level: 0}},
		{Kind: vfs.BlockKindHeading, Text: "Deep", Style: vfs.StyleMeta{Level: 9}},
		{Kind: vfs.BlockKindParagraph, Text: `See _i_ and [y](https://e) and ~~s~~`},
		{Kind: vfs.BlockKindListItem, Text: "one", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindListItem, Text: "nest", Style: vfs.StyleMeta{Level: 2, Attributes: map[string]string{"list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindListItem, Text: "two", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"list_type": "ol", "list_id": "l2"}}},
		{Kind: vfs.BlockKindListItem, Text: "zero", Style: vfs.StyleMeta{Attributes: map[string]string{"list_id": "l3"}}},
		{Kind: vfs.BlockKindTable, Text: "A\tB\nC\t**D**", Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "2"}}},
		{Kind: vfs.BlockKindImage, Text: `alt "q"`, Style: vfs.StyleMeta{Attributes: map[string]string{"object_id": "kix.1", "content_uri": "https://img"}}},
		{Kind: "quote", Text: "aside"},
	})

	html := doc.Text()
	blocks := decodeDocsHTML(t, html)

	for _, want := range []string{"<h1>H &amp; <strong>x</strong></h1>", "<h6>Deep</h6>", "<em>i</em>", "<a href=", "<s>s</s>", "<ul>", "<ol>", "<ul>\n<li>zero</li>\n</ul>", "<table>", "data-object-id", "&#34;"} {
		if !strings.Contains(html, want) {
			t.Fatalf("missing %q in %s", want, html)
		}
	}
	var sawBold, sawLink bool
	for _, b := range blocks {
		sawBold = sawBold || strings.Contains(b.Text, "**x**")
		sawLink = sawLink || strings.Contains(b.Text, "](https://")
	}
	if !sawBold || !sawLink {
		t.Fatalf("decode lost marks: %+v", blocks)
	}
}

// The Docs codec reads bare body text as a paragraph. It skips empty
// elements, styles, scripts, images outside a figure and the tab headings
// it projects. It keeps inline marks, the tab of each section, figures,
// nested lists and table headers.
func TestDocsCodec_decodesHTMLBlocksAndMarks(t *testing.T) {
	t.Parallel()
	bare := decodeDocsHTML(t, "<html><body>hello from CRE</body></html>")
	empty := decodeDocsHTML(t, "<html><body><div></div></body></html>")
	marks := decodeDocsHTML(t, `<html><body><p>See <strong>x</strong><em></em> and <a href="https://e">y</a><br><em>i</em> <s>s</s></p></body></html>`)
	tabs := decodeDocsHTML(t, `<html><body><section data-tab-id="t.a"><h1 class="tacklr-tab">A</h1><p>in a</p></section><section><p>no tab</p></section></body></html>`)
	sink := decodeDocsHTML(t, `<html><head><style>x</style><script>y</script></head><body>
<h2></h2><div></div>
<figure data-object-id="kix.z"><img alt="pic" src="https://e"></figure>
<img alt="bare">
<ol><li>A<ul><li>B</li></ul></li></ol>
<table><tr><th>H</th></tr></table>
</body></html>`)

	if len(bare) != 1 || bare[0].Kind != vfs.BlockKindParagraph || bare[0].Text != "hello from CRE" {
		t.Fatalf("bare text: %+v", bare)
	}
	if len(empty) != 0 {
		t.Fatalf("empty: %+v", empty)
	}
	if len(marks) != 1 || !strings.Contains(marks[0].Text, "See **x** and [y](https://e)") || !strings.Contains(marks[0].Text, "_i_ ~~s~~") {
		t.Fatalf("marks: %+v", marks)
	}
	if len(tabs) != 2 || tabs[0].Style.Attributes["tab_id"] != "t.a" || tabs[1].Style.Attributes["tab_id"] != "" || tabs[1].Text != "no tab" {
		t.Fatalf("tabs: %+v", tabs)
	}
	var kinds []string
	for _, b := range sink {
		kinds = append(kinds, b.Kind)
	}
	if got := strings.Join(kinds, ","); got != "image,list_item,list_item,table" {
		t.Fatalf("kinds = %s: %+v", got, sink)
	}
}
