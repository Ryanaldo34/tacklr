package vfs_test

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestTextDocument_markdownBlocks projects ATX outline as Structured blocks.
func TestTextDocument_markdownBlocks(t *testing.T) {
	t.Parallel()
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
	if errBlock := blocks[4]; errBlock.Text != "Errors" || errBlock.Style.Attributes["heading_path"] != "title/api/errors" {
		t.Fatalf("title/api/errors: %+v", errBlock)
	}
	if inst, dup := blocks[2], blocks[5]; inst.Text != "Install" || dup.Text != "Install" {
		t.Fatalf("install headings: %+v %+v", inst, dup)
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
// empty titles → section, root-level id collision.
func TestTextDocument_markdownStructureEdges(t *testing.T) {
	t.Parallel()
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
		"# Closed ##\n\n" + // ATX closing hashes with space are stripped
		"# Tight##\n\n" + // no space before closing hashes: kept
		"body under second root\n"
	doc := vfs.NewTextDocument("/work/edges.md", "text/markdown", "utf-8", md)
	blocks := doc.Blocks()
	ids := blockIDs(blocks)
	// Expect: root, section (empty title), section/child, root-2, closed, tight
	want := []string{"root", "section", "section/child", "root-2", "closed", "tight"}
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
	if root2 := blocks[3]; root2.Style.Level != 1 {
		t.Fatalf("root-2: %+v", root2)
	}
}
