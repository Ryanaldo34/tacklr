package vfs

import (
	"strings"
	"testing"
)

func TestNormalizeInline_setsCanonicalText(t *testing.T) {
	b := Block{Kind: BlockKindParagraph, Text: "Hello *world*"}
	normalizeInline(&b)
	if b.Text != "Hello _world_" || len(b.Runs) != 2 {
		t.Fatalf("block = %+v", b)
	}
	if b.PlainText() != "Hello world" {
		t.Fatalf("plain = %q", b.PlainText())
	}
}

func TestParseInline_unclosedAndEscapes(t *testing.T) {
	if got := FormatInline(ParseInline("[nope")); got != `\[nope` {
		t.Fatalf("unclosed link = %q", got)
	}
	if got := FormatInline(ParseInline("[x](nope")); got != `\[x\](nope` {
		t.Fatalf("unclosed href = %q", got)
	}
	if got := PlainInline("a * b"); got != "a * b" {
		t.Fatalf("lone star = %q", got)
	}
	img := Block{Kind: BlockKindImage, Text: "alt"}
	if img.PlainText() != "alt" || img.inlineRuns() != nil {
		t.Fatalf("image = %+v", img)
	}
	tbl := Block{Kind: BlockKindTable, Text: "A\t**B**"}
	normalizeInline(&tbl)
	if !strings.Contains(tbl.Text, "**B**") || tbl.PlainText() != tbl.Text {
		t.Fatalf("table = %+v", tbl)
	}
	if marksEqual(map[string]string{"bold": "true"}, map[string]string{"italic": "true"}) {
		t.Fatal("marks should differ")
	}
	if mergeRuns([]Run{{Text: ""}, {Text: "a"}, {Text: "b"}})[0].Text != "ab" {
		t.Fatal("merge empty+same")
	}
	if _, _, _, ok := parseLink("x", 0); ok {
		t.Fatal("not a link")
	}
	if got := PlainInline("[a [b]](u)"); got != "a [b]" {
		t.Fatalf("nested = %q", got)
	}
	if got := PlainInline(`[a\]b](u)`); got != `a\]b` && got != `a]b` {
		t.Fatalf("escaped = %q", got)
	}
	if got := FormatInline(ParseInline("[x]")); !strings.Contains(got, "x") {
		t.Fatalf("no href = %q", got)
	}
	if got := FormatInline(ParseInline(`**a\*b**`)); !strings.Contains(got, "a") {
		t.Fatalf("escaped close = %q", got)
	}
}

func TestDocsCodec_htmlInlineMarks(t *testing.T) {
	raw := []byte(`<html><body><p>See <strong>x</strong> and <a href="https://e">y</a></p></body></html>`)
	blocks, err := decodeDocsHTML(raw)
	if err != nil || len(blocks) != 1 {
		t.Fatalf("blocks=%v err=%v", blocks, err)
	}
	if blocks[0].Text != "See **x** and [y](https://e)" {
		t.Fatalf("text=%q", blocks[0].Text)
	}
	sink, err := decodeDocsHTML([]byte(`<html><head><style>x</style><script>y</script></head><body>
<h2></h2><div></div>
<figure data-object-id="kix.z"><img alt="pic" src="https://e"></figure>
<img alt="bare">
<ol><li>A<ul><li>B</li></ul></li></ol>
<table><tr><th>H</th></tr></table>
</body></html>`))
	if err != nil || len(sink) < 3 {
		t.Fatalf("sink=%d err=%v", len(sink), err)
	}
	more, err := decodeDocsHTML([]byte(`<html><body><h1 class="tacklr-tab">Skip</h1><p>a<br>b <em>i</em> <s>s</s></p></body></html>`))
	if err != nil || len(more) != 1 || !strings.Contains(more[0].Text, "_i_") || !strings.Contains(more[0].Text, "~~s~~") {
		t.Fatalf("more=%+v err=%v", more, err)
	}
}
