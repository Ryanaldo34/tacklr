package vfs

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

// structureFor attaches a block view for known media types (internal projectors).
// Uses the document line index so large files are not re-split into []string.
func structureFor(d *TextDocument) []Block {
	if d.encoder != nil {
		var rich RichTextDocument
		if err := json.Unmarshal([]byte(d.text), &rich); err == nil && validateRichText(&rich) == nil {
			return projectRichBlocks(rich.Blocks, 1)
		}
	}
	if d.richBlocks != nil {
		return slices.Clone(d.richBlocks)
	}
	switch d.mediaType {
	case "text/markdown":
		return blocksFromMarkdown(d)
	default:
		return nil
	}
}

// blocksFromMarkdown projects ATX headings into the shared Block schema.
// Headings inside fenced code are ignored. Content before the first heading
// is a preamble block. IDs are hierarchical slugs (e.g. api/errors).
func blocksFromMarkdown(d *TextDocument) []Block {
	nLines := d.LineCount()
	if nLines == 0 {
		return nil
	}

	type head struct {
		line  int // 1-based
		level int
		title string
	}
	var heads []head
	inFence := false
	fenceMark := ""

	for i := 0; i < nLines; i++ {
		line := d.lineSlice(i)
		lineNo := i + 1
		trim := strings.TrimSpace(line)
		if !inFence {
			if mark, ok := fenceOpen(trim); ok {
				inFence = true
				fenceMark = mark
				continue
			}
			if level, title, ok := atxHeading(line); ok {
				heads = append(heads, head{line: lineNo, level: level, title: title})
			}
			continue
		}
		if fenceClose(trim, fenceMark) {
			inFence = false
			fenceMark = ""
		}
	}

	endEOF := nLines + 1
	blocks := make([]Block, 0, len(heads)+1)

	preEnd := endEOF
	if len(heads) > 0 {
		preEnd = heads[0].line
	}
	if preEnd > 1 {
		blocks = append(blocks, Block{
			ID:   "preamble",
			Kind: BlockKindPreamble,
			Text: "",
			Style: StyleMeta{
				Level: 0,
				Span:  Span{StartLine: 1, EndLine: preEnd},
				Attributes: map[string]string{
					"heading_path": "preamble",
				},
			},
		})
	}

	// path is the full hierarchical id for this heading (children nest under it).
	type frame struct {
		level int
		path  string
	}
	// ATX levels are 1..6, so nesting depth is bounded.
	stack := make([]frame, 0, 6)
	used := make(map[string]int, len(heads))

	for i, h := range heads {
		for len(stack) > 0 && stack[len(stack)-1].level >= h.level {
			stack = stack[:len(stack)-1]
		}
		seg := Slugify(h.title)
		if seg == "" {
			seg = "section"
		}
		// Build id from parent path + segment; uniquify full path on collision.
		var baseID string
		if len(stack) == 0 {
			baseID = seg
		} else {
			baseID = stack[len(stack)-1].path + "/" + seg
		}
		used[baseID]++
		id := baseID
		if n := used[baseID]; n > 1 {
			// Unique leaf segment so children nest under this occurrence.
			if len(stack) == 0 {
				id = seg + "-" + strconv.Itoa(n)
			} else {
				id = stack[len(stack)-1].path + "/" + seg + "-" + strconv.Itoa(n)
			}
		}
		stack = append(stack, frame{level: h.level, path: id})

		end := endEOF
		for j := i + 1; j < len(heads); j++ {
			if heads[j].level <= h.level {
				end = heads[j].line
				break
			}
		}
		blocks = append(blocks, Block{
			ID:   id,
			Kind: BlockKindHeading,
			Text: h.title,
			Style: StyleMeta{
				Level: h.level,
				Span:  Span{StartLine: h.line, EndLine: end},
				Attributes: map[string]string{
					"heading_path": id,
				},
			},
		})
	}
	return blocks
}

func atxHeading(line string) (level int, title string, ok bool) {
	i := 0
	for i < len(line) && i < 3 && line[i] == ' ' {
		i++
	}
	if i >= len(line) || line[i] != '#' {
		return 0, "", false
	}
	start := i
	for i < len(line) && line[i] == '#' {
		i++
	}
	level = i - start
	if level < 1 || level > 6 {
		return 0, "", false
	}
	if i < len(line) && line[i] != ' ' && line[i] != '\t' {
		return 0, "", false
	}
	title = strings.TrimSpace(line[i:])
	if strings.HasSuffix(title, "#") {
		j := len(title)
		for j > 0 && title[j-1] == '#' {
			j--
		}
		if j > 0 && (title[j-1] == ' ' || title[j-1] == '\t') {
			title = strings.TrimSpace(title[:j])
		}
	}
	return level, title, true
}

func fenceOpen(trim string) (mark string, ok bool) {
	if strings.HasPrefix(trim, "```") {
		return "```", true
	}
	if strings.HasPrefix(trim, "~~~") {
		return "~~~", true
	}
	return "", false
}

func fenceClose(trim, mark string) bool {
	return mark != "" && strings.HasPrefix(trim, mark)
}

// Slugify lowers, keeps alnum, collapses other runs to '-', and trims dashes.
func Slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	b.Grow(len(s))
	prevDash := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			b.WriteRune(r)
			prevDash = false
			continue
		}
		if !prevDash && b.Len() > 0 {
			b.WriteByte('-')
			prevDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}
