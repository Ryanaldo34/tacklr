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
