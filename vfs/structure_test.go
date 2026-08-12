package vfs_test

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestTextDocument_markdownBlocks projects ATX outline as Structured blocks.
func TestTextDocument_markdownBlocks(t *testing.T) {
	const md = `Preamble here.

# Title

Intro.

## Install

Use go get.

## API

### Errors

Boom.

` + "```\n# not a heading\n```\n" + `
## Install

Dup title.
`
	doc := vfs.NewTextDocument("/work/README.md", "text/markdown", "utf-8", md)
	blocks := doc.Blocks()

	// Positive outline identity: preamble, hierarchy, fence-ignored heading, collision ids.
	wantIDs := []string{"preamble", "title", "title/install", "title/api", "title/api/errors", "title/install-2"}
	gotIDs := blockIDs(blocks)
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("outline ids=%v want %v", gotIDs, wantIDs)
	}
	for i, id := range wantIDs {
		if gotIDs[i] != id {
			t.Fatalf("outline ids=%v want %v", gotIDs, wantIDs)
		}
	}
	if blocks[0].Kind != vfs.BlockKindPreamble || blocks[0].ID != "preamble" {
		t.Fatalf("preamble: %+v", blocks[0])
	}
	if blocks[1].ID != "title" || blocks[1].Style.Level != 1 || blocks[1].Text != "Title" {
		t.Fatalf("title: %+v", blocks[1])
	}
	errBlock, ok := vfs.FindBlock(blocks, "title/api/errors")
	if !ok || errBlock.Text != "Errors" {
		t.Fatalf("title/api/errors: ok=%v blocks ids=%v", ok, gotIDs)
	}
	// heading_path attribute matches ID (FindBlock secondary key)
	if _, ok := vfs.FindBlock(blocks, strings.TrimSpace(" title/api/errors ")); !ok {
		t.Fatalf("FindBlock trim + heading_path: %v", gotIDs)
	}

	inst, ok := vfs.FindBlock(blocks, "title/install")
	if !ok || inst.Text != "Install" {
		t.Fatalf("title/install: ok=%v text=%q ids=%v", ok, inst.Text, gotIDs)
	}
	dup, ok := vfs.FindBlock(blocks, "title/install-2")
	if !ok || dup.Text != "Install" {
		t.Fatalf("title/install-2: ok=%v text=%q ids=%v", ok, dup.Text, gotIDs)
	}

	start, end, err := vfs.BlockReplaceSpan(inst, false)
	if err != nil || start != inst.Style.Span.StartLine+1 || end != inst.Style.Span.EndLine {
		t.Fatalf("body span: start=%d end=%d want start=%d end=%d err=%v",
			start, end, inst.Style.Span.StartLine+1, inst.Style.Span.EndLine, err)
	}
	hs, he, err := vfs.BlockReplaceSpan(inst, true)
	if err != nil || hs != inst.Style.Span.StartLine || he != inst.Style.Span.EndLine {
		t.Fatalf("full span: %d %d err=%v", hs, he, err)
	}

	// edit body via ReplaceLines; Blocks() refreshes
	if err := doc.ReplaceLines(start, end, []string{"New install body."}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Text(), "New install body.") {
		t.Fatal("body not updated")
	}
	if _, ok := vfs.FindBlock(doc.Blocks(), "title/install"); !ok {
		t.Fatalf("outline after edit: %v", blockIDs(doc.Blocks()))
	}
}

func blockIDs(blocks []vfs.Block) []string {
	ids := make([]string, len(blocks))
	for i, b := range blocks {
		ids[i] = b.ID
	}
	return ids
}

// TestTextDocument_markdownStructureEdges: empty/non-md, ATX quirks, fences,
// empty titles → section, root-level id collision, FindBlock/ReplaceSpan edges.
func TestTextDocument_markdownStructureEdges(t *testing.T) {
	// Non-markdown → no structure
	goDoc := vfs.NewTextDocument("/work/a.go", "text/x-go", "utf-8", "package a\n")
	if len(goDoc.Blocks()) != 0 {
		t.Fatalf("go blocks: %v", blockIDs(goDoc.Blocks()))
	}
	// Empty markdown
	empty := vfs.NewTextDocument("/work/e.md", "text/markdown", "utf-8", "")
	if len(empty.Blocks()) != 0 {
		t.Fatalf("empty md blocks: %v", blockIDs(empty.Blocks()))
	}

	// Leading spaces (≤3), trailing ###, empty title → section, ~~~ fence,
	// not-a-heading (#no space), too many #, root collision uniquify.
	md := "   # Root\n\n" +
		"####### not heading\n" +
		"#nospace\n\n" +
		"~~~\n# fenced\n~~~\n\n" +
		"#   \n\n" + // empty title after strip → section
		"## Child\n\n" +
		"# Root\n\n" + // collision at root → root-2
		"body under second root\n"
	doc := vfs.NewTextDocument("/work/edges.md", "text/markdown", "utf-8", md)
	blocks := doc.Blocks()
	ids := blockIDs(blocks)
	// Expect: root, section (empty title), section/child, root-2
	want := []string{"root", "section", "section/child", "root-2"}
	if len(ids) != len(want) {
		t.Fatalf("ids=%v want %v\ntext:\n%s", ids, want, md)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids=%v want %v", ids, want)
		}
	}
	// Fenced heading must not appear
	for _, id := range ids {
		if strings.Contains(id, "fenced") {
			t.Fatalf("fenced heading leaked: %v", ids)
		}
	}
	root2, ok := vfs.FindBlock(blocks, "root-2")
	if !ok || root2.Style.Level != 1 {
		t.Fatalf("root-2: ok=%v %+v", ok, root2)
	}
	// Empty body under heading: start after heading line can equal end
	// (BlockReplaceSpan body-only on empty section).
	sec, ok := vfs.FindBlock(blocks, "section")
	if !ok {
		t.Fatalf("section missing: %v", ids)
	}
	start, end, err := vfs.BlockReplaceSpan(sec, false)
	if err != nil {
		t.Fatal(err)
	}
	if start < sec.Style.Span.StartLine {
		t.Fatalf("body start %d < heading %d", start, sec.Style.Span.StartLine)
	}
	_ = end

	// FindBlock empty / miss
	if _, ok := vfs.FindBlock(blocks, ""); ok {
		t.Fatal("empty id should miss")
	}
	if _, ok := vfs.FindBlock(blocks, "nope"); ok {
		t.Fatal("missing id")
	}
	// Invalid span on ReplaceSpan
	bad := vfs.Block{Kind: vfs.BlockKindHeading, Style: vfs.StyleMeta{Span: vfs.Span{StartLine: 0, EndLine: 1}}}
	if _, _, err := vfs.BlockReplaceSpan(bad, true); err == nil {
		t.Fatal("want ErrLineOutOfRange for bad span")
	}
}
