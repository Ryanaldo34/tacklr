package vfs

import (
	"testing"
)

func TestParseFormatInline_roundTrip(t *testing.T) {
	cases := []struct {
		in, plain, out string
	}{
		{"hello", "hello", "hello"},
		{"**bold**", "bold", "**bold**"},
		{"_italic_", "italic", "_italic_"},
		{"*also*", "also", "_also_"},
		{"~~x~~", "x", "~~x~~"},
		{"[Maya](mailto:maya)", "Maya", "[Maya](mailto:maya)"},
		{"See **Priorities** and [Maya](mailto:maya).", "See Priorities and Maya.", "See **Priorities** and [Maya](mailto:maya)."},
		{"[**Maya**](mailto:maya)", "Maya", "[**Maya**](mailto:maya)"},
		{`2 \* 3`, "2 * 3", `2 \* 3`},
	}
	for _, tc := range cases {
		runs := ParseInline(tc.in)
		if got := runsPlain(runs); got != tc.plain {
			t.Fatalf("%q plain = %q, want %q", tc.in, got, tc.plain)
		}
		if got := FormatInline(runs); got != tc.out {
			t.Fatalf("%q format = %q, want %q", tc.in, got, tc.out)
		}
		if got := FormatInline(ParseInline(FormatInline(runs))); got != tc.out {
			t.Fatalf("%q not stable: %q", tc.in, got)
		}
	}
}

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

func TestMapReplaceBlock_emitsTextStyle(t *testing.T) {
	b := Block{Kind: BlockKindParagraph, Text: "See **x**"}
	normalizeInline(&b)
	reqs, err := mapReplaceBlock(blockLocation{tabID: "t0", startIndex: 1, endIndex: 10}, b)
	if err != nil {
		t.Fatal(err)
	}
	var sawInsert, sawBold bool
	for _, r := range reqs {
		if r.InsertText != nil && r.InsertText.Text == "See x" {
			sawInsert = true
		}
		if st := r.UpdateTextStyle; st != nil && st.TextStyle != nil && st.TextStyle.Bold {
			if st.Range != nil && st.Range.StartIndex == 5 && st.Range.EndIndex == 6 {
				sawBold = true
			}
		}
	}
	if !sawInsert || !sawBold {
		t.Fatalf("reqs=%+v insert=%v bold=%v", reqs, sawInsert, sawBold)
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
}
