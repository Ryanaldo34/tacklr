package vfs_test

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// A rich document stores inline marks in their canonical form, merges
// given runs (empty ones dropped, equal marks joined), and its tables keep
// their cell marks as text.
func TestNormalizeInline_setsCanonicalText(t *testing.T) {
	doc := vfs.NewRichDocument("/Spec", googleDocMIME, []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "Hello *world*"},
		{Kind: vfs.BlockKindTable, Text: "A\t**B**"},
		{Kind: vfs.BlockKindImage, Text: "alt"},
		{Kind: vfs.BlockKindParagraph, Runs: []vfs.Run{{Text: ""}, {Text: "a"}, {Text: "b"}}},
	})

	para, table, image, runs := doc.Blocks()[0], doc.Blocks()[1], doc.Blocks()[2], doc.Blocks()[3]

	if para.Text != "Hello _world_" || len(para.Runs) != 2 || para.PlainText() != "Hello world" {
		t.Fatalf("paragraph = %+v, plain %q", para, para.PlainText())
	}
	if !strings.Contains(table.Text, "**B**") || table.PlainText() != table.Text {
		t.Fatalf("table = %+v", table)
	}
	if image.PlainText() != "alt" {
		t.Fatalf("image = %+v", image)
	}
	if runs.Text != "ab" || len(runs.Runs) != 1 {
		t.Fatalf("merged runs = %+v", runs)
	}
}

// Unclosed links and marks stay literal (escaped when formatted), nested
// and escaped brackets stay in the label, and adjacent runs with the same
// marks merge.
func TestParseInline_unclosedAndEscapes(t *testing.T) {
	for in, want := range map[string]string{
		"[nope":       `\[nope`,
		"[x](nope":    `\[x\](nope`,
		"**a****b**":  "**ab**",
		"**a**_b_":    "**a**_b_",
		`**a\*b**`:    `**a\*b**`,
		"[x]":         `\[x\]`,
		"a [b](u) c":  "a [b](u) c",
		"~~s~~ plain": "~~s~~ plain",
	} {
		if got := vfs.FormatInline(vfs.ParseInline(in)); got != want {
			t.Fatalf("FormatInline(ParseInline(%q)) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"a * b":      "a * b",
		"[a [b]](u)": "a [b]",
		`[a\]b](u)`:  "a]b",
	} {
		if got := vfs.PlainInline(in); got != want {
			t.Fatalf("PlainInline(%q) = %q, want %q", in, got, want)
		}
	}
}
