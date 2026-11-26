package vfsindex

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// tinyChunks makes a few short lines span several chunks.
var tinyChunks = ChunkSizes{Min: 8, Avg: 16, Max: 32}

// part is what a test reads of one Chunk part.
type part struct {
	text, block    string
	start, endLine float64
}

func collect(t *testing.T, parts func(func(brain.Object, error) bool)) []part {
	t.Helper()
	var out []part
	for p, err := range parts {
		if err != nil {
			t.Fatal(err)
		}
		block, _ := p.Properties[PropBlockID].(string)
		out = append(out, part{p.Content, block, p.Properties[PropStartLine].(float64), p.Properties[PropEndLine].(float64)})
	}
	return out
}

func texts(parts []part) string {
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = p.text
	}
	return strings.Join(out, "\n")
}

// TestStreamParts_chunkPlainTextIntoLineAlignedParts: plain text splits
// into chunks that cover it in order, a run of blank lines longer than a
// chunk makes no blank part, and a chunk that ends at a CRLF line end drops
// the CR.
func TestStreamParts_chunkPlainTextIntoLineAlignedParts(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		body  string
		check func([]part) bool
	}{
		"short lines cover the text in order": {
			"alpha line\nbeta TODO findme\ngamma line\ndelta line\n",
			func(ps []part) bool {
				return len(ps) > 1 && texts(ps) == "alpha line\nbeta TODO findme\ngamma line\ndelta line" && ps[0].start == 1
			},
		},
		"a run of blank lines makes no blank part": {
			"top-word\n" + strings.Repeat("\n", 100) + "bottom-word\n",
			func(ps []part) bool {
				for _, p := range ps {
					if strings.TrimSpace(p.text) == "" {
						return false
					}
				}
				return strings.Contains(texts(ps), "top-word") && strings.Contains(texts(ps), "bottom-word")
			},
		},
		"a CRLF line end drops its CR": {
			"alpha stream-unique\r\nbeta line\r\n",
			func(ps []part) bool {
				return !strings.HasSuffix(ps[len(ps)-1].text, "\r") && !strings.HasSuffix(ps[0].text, "\r")
			},
		},
	} {
		got := collect(t, streamParts(chunks(t.Context(), strings.NewReader(c.body), tinyChunks, 0), DefaultChunkKind))

		if !c.check(got) {
			t.Errorf("%s: parts = %+v", name, got)
		}
	}
}

// TestBlockParts_chunkMarkdownBySection: Markdown splits into one part per
// heading section, which holds its subsections, and its preamble, with each
// block's id and half-open line span. A document with no heading is one
// preamble part, and an empty one has no parts.
func TestBlockParts_chunkMarkdownBySection(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		body  string
		sizes ChunkSizes
		want  []part
	}{
		"sections and a preamble": {
			body:  "intro line\n\n# Title\n\n## Install\n\npip install x\n\n## API\n\ncall me\n",
			sizes: DefaultChunkSizes,
			want: []part{
				{"intro line\n", "preamble", 1, 3},
				{"# Title\n\n## Install\n\npip install x\n\n## API\n\ncall me\n", "title", 3, 13},
				{"## Install\n\npip install x\n", "title/install", 5, 9},
				{"## API\n\ncall me\n", "title/api", 9, 13},
			},
		},
		"no heading is one preamble": {
			body:  "line one\nline two\nline three\n",
			sizes: DefaultChunkSizes,
			want:  []part{{"line one\nline two\nline three\n", "preamble", 1, 5}},
		},
		"an empty document has no parts": {sizes: DefaultChunkSizes},
	} {
		doc := vfs.NewTextDocument("/w/guide.md", "text/markdown", "utf-8", c.body)

		got := collect(t, blockParts(t.Context(), "guide.md", DefaultChunkKind, doc, c.sizes, false))

		if len(got) != len(c.want) {
			t.Errorf("%s: parts = %+v, want %+v", name, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: part %d = %#v, want %#v", name, i, got[i], c.want[i])
			}
		}
	}
}

// TestBlockParts_aSectionOverMaxSplitsIntoChunksOfItsBlock: every part of a
// big section keeps the section's block id, and the last ends on its last
// line.
func TestBlockParts_aSectionOverMaxSplitsIntoChunksOfItsBlock(t *testing.T) {
	t.Parallel()
	doc := vfs.NewTextDocument("/w/big.md", "text/markdown", "utf-8", "# Big\n\n"+strings.Repeat("section body words\n", 20))

	got := collect(t, blockParts(t.Context(), "big.md", DefaultChunkKind, doc, tinyChunks, false))

	if len(got) < 2 || got[len(got)-1].endLine != 22 {
		t.Fatalf("parts = %+v, want several ending on line 22", got)
	}
	for _, p := range got {
		if p.block != "big" {
			t.Fatalf("part %+v is not of block big", p)
		}
	}
}
