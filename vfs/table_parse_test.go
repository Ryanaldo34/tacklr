package vfs

import (
	"strings"
	"testing"
)

func TestParseTableText_pipeMarkdownBecomesGrid(t *testing.T) {
	raw := strings.TrimSpace(`
| Option | Description | Planning run-rate | Pros | Cons |
| --- | --- | --- | --- | --- |
| A. Public-only | Census/BLS/DOT | $1,000–$15,000/month | Lowest licensing risk | Lagged |
| B. Marketplace | Snowflake listings | $3,000–$25,000/month | Faster coverage | License cost |
`)
	grid, err := parseTableText(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(grid) != 3 || len(grid[0]) != 5 {
		t.Fatalf("shape %dx%d, want 3x5: %+v", len(grid), len(grid[0]), grid)
	}
	if grid[0][0] != "Option" || grid[1][0] != "A. Public-only" || grid[2][1] != "Snowflake listings" {
		t.Fatalf("cells = %+v", grid)
	}
	for _, row := range grid {
		for _, cell := range row {
			if strings.HasPrefix(cell, "|") || strings.Contains(cell, " | ") {
				t.Fatalf("pipe leftover in cell %q", cell)
			}
		}
	}
}

func TestTableShape_pipeOverridesOneColumnGuess(t *testing.T) {
	b := Block{Kind: BlockKindTable, Text: "| A | B |\n| 1 | 2 |"}
	r, c, err := tableShape(b)
	if err != nil || r != 2 || c != 2 {
		t.Fatalf("shape %d %d %v", r, c, err)
	}
}

func TestLiftPlaintext_pipeTableIsTableBlock(t *testing.T) {
	blocks := liftPlaintext("Intro\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n\nOutro")
	if len(blocks) != 3 {
		t.Fatalf("blocks=%d %+v", len(blocks), blocks)
	}
	if blocks[0].Kind != BlockKindParagraph || blocks[0].Text != "Intro" {
		t.Fatalf("intro %+v", blocks[0])
	}
	if blocks[1].Kind != BlockKindTable {
		t.Fatalf("want table, got %+v", blocks[1])
	}
	grid, err := parseTableText(blocks[1].Text)
	if err != nil || len(grid) != 2 || grid[0][0] != "A" || grid[1][1] != "2" {
		t.Fatalf("lifted grid %+v %v", grid, err)
	}
	if blocks[2].Kind != BlockKindParagraph || blocks[2].Text != "Outro" {
		t.Fatalf("outro %+v", blocks[2])
	}
}
