package vfs

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/api/docs/v1"
)

func docsRange(start, end int, tabID string) *docs.Range {
	return &docs.Range{StartIndex: int64(start), EndIndex: int64(end), TabId: tabID}
}

func docsLocation(index int, tabID string) *docs.Location {
	return &docs.Location{Index: int64(index), TabId: tabID}
}

func reqDelete(start, end int, tabID string) DocsRequest {
	return DocsRequest{DeleteContentRange: &docs.DeleteContentRangeRequest{
		Range: docsRange(start, end, tabID),
	}}
}

func reqInsert(index int, tabID, text string) DocsRequest {
	return DocsRequest{InsertText: &docs.InsertTextRequest{
		Location: docsLocation(index, tabID),
		Text:     text,
	}}
}

func reqStyle(start, end int, tabID, named string) DocsRequest {
	return DocsRequest{UpdateParagraphStyle: &docs.UpdateParagraphStyleRequest{
		Range:          docsRange(start, end, tabID),
		ParagraphStyle: &docs.ParagraphStyle{NamedStyleType: named},
		Fields:         "namedStyleType",
	}}
}

func reqBullets(start, end int, tabID, listType string) DocsRequest {
	preset := "BULLET_DISC_CIRCLE_SQUARE"
	if listType == "ol" {
		preset = "NUMBERED_DECIMAL_ALPHA_ROMAN"
	}
	return DocsRequest{CreateParagraphBullets: &docs.CreateParagraphBulletsRequest{
		Range:        docsRange(start, end, tabID),
		BulletPreset: preset,
	}}
}

func runHasMarks(r Run) bool {
	return r.Marks[MarkBold] == "true" || r.Marks[MarkItalic] == "true" ||
		r.Marks[MarkStrike] == "true" || r.Marks[MarkHref] != ""
}

func reqTextStyle(start, end int, tabID string, r Run) DocsRequest {
	st := &docs.TextStyle{
		Bold:          r.Marks[MarkBold] == "true",
		Italic:        r.Marks[MarkItalic] == "true",
		Strikethrough: r.Marks[MarkStrike] == "true",
	}
	st.ForceSendFields = []string{"Bold", "Italic", "Strikethrough"}
	if href := r.Marks[MarkHref]; href != "" {
		st.Link = &docs.Link{Url: href}
	} else {
		st.NullFields = []string{"Link"}
	}
	return DocsRequest{UpdateTextStyle: &docs.UpdateTextStyleRequest{
		Range:     docsRange(start, end, tabID),
		TextStyle: st,
		Fields:    "bold,italic,strikethrough,link",
	}}
}

func mapInlineStyles(start int, tabID string, runs []Run) []DocsRequest {
	var reqs []DocsRequest
	off := 0
	for _, r := range runs {
		n := docsIndexLen(r.Text)
		if n > 0 && runHasMarks(r) {
			reqs = append(reqs, reqTextStyle(start+off, start+off+n, tabID, r))
		}
		off += n
	}
	return reqs
}

func splitTextStyles(reqs []DocsRequest) (body, styles []DocsRequest) {
	for _, r := range reqs {
		if r.UpdateTextStyle != nil {
			styles = append(styles, r)
			continue
		}
		body = append(body, r)
	}
	return body, styles
}

func appendInsertStyled(reqs []DocsRequest, idx int, tabID, named, prefix string, b Block) ([]DocsRequest, int) {
	payload := prefix + b.PlainText() + "\n"
	n := docsIndexLen(payload)
	reqs = append(reqs, reqInsert(idx, tabID, payload), reqStyle(idx, idx+n, tabID, named))
	// Style start is the insert index. CreateParagraphBullets then strips
	// leading tabs, so the plaintext sits at idx when styles run in the next batch.
	return append(reqs, mapInlineStyles(idx, tabID, b.inlineRuns())...), idx + n
}

func reqInsertTable(index int, tabID string, rows, cols int) DocsRequest {
	return DocsRequest{InsertTable: &docs.InsertTableRequest{
		Location: docsLocation(index, tabID),
		Rows:     int64(rows),
		Columns:  int64(cols),
	}}
}

// mapReplaceBlock builds keep-newline text replace requests for one location.
// High startIndex first is the caller's job when replacing many.
func mapReplaceBlock(loc blockLocation, b Block) ([]DocsRequest, error) {
	tab := loc.tabID
	switch b.Kind {
	case BlockKindImage:
		return nil, fmt.Errorf("%w: write: cannot replace an image; omit it from blocks to delete, or leave it", ErrNotSupported)
	case BlockKindTable:
		return mapReplaceTable(loc, b)
	default:
		plain := b.PlainText()
		var reqs []DocsRequest
		if loc.endIndex-loc.startIndex <= 1 {
			if plain == "" {
				return nil, nil
			}
			reqs = append(reqs, reqInsert(loc.startIndex, tab, plain))
		} else {
			reqs = append(reqs, reqDelete(loc.startIndex, loc.endIndex-1, tab))
			if plain != "" {
				reqs = append(reqs, reqInsert(loc.startIndex, tab, plain))
			}
		}
		return append(reqs, mapInlineStyles(loc.startIndex, tab, b.inlineRuns())...), nil
	}
}

func mapReplaceTable(loc blockLocation, b Block) ([]DocsRequest, error) {
	grid, err := parseTSV(b.Text)
	if err != nil {
		return nil, err
	}
	rows, cols, err := tableShape(b)
	if err != nil {
		return nil, err
	}
	if len(grid) != rows {
		return nil, fmt.Errorf("%w: table shape must stay %dx%d", ErrNotSupported, rows, cols)
	}
	for _, row := range grid {
		if len(row) != cols {
			return nil, fmt.Errorf("%w: table shape must stay %dx%d", ErrNotSupported, rows, cols)
		}
	}
	if len(loc.cells) == 0 {
		return nil, fmt.Errorf("%w: table has no cell locations", ErrNotSupported)
	}
	type pair struct {
		cell cellLocation
		text string
	}
	pairs := make([]pair, 0, len(loc.cells))
	for _, c := range loc.cells {
		if c.row < 0 || c.row >= len(grid) || c.col < 0 || c.col >= len(grid[c.row]) {
			return nil, fmt.Errorf("%w: table shape must stay %dx%d", ErrNotSupported, rows, cols)
		}
		pairs = append(pairs, pair{cell: c, text: grid[c.row][c.col]})
	}
	slices.SortFunc(pairs, func(a, b pair) int { return cmp.Compare(b.cell.startIndex, a.cell.startIndex) })
	reqs := make([]DocsRequest, 0, len(pairs)*2)
	for _, p := range pairs {
		if p.cell.endIndex-p.cell.startIndex > 1 {
			reqs = append(reqs, reqDelete(p.cell.startIndex, p.cell.endIndex-1, loc.tabID))
		}
		plain := PlainInline(p.text)
		if plain != "" {
			reqs = append(reqs, reqInsert(p.cell.startIndex, loc.tabID, plain))
			reqs = append(reqs, mapInlineStyles(p.cell.startIndex, loc.tabID, ParseInline(p.text))...)
		}
	}
	return reqs, nil
}

func mapReplaceBlocks(hint persistHint, blocks []Block) ([]DocsRequest, error) {
	byID := make(map[string]Block, len(blocks))
	for _, b := range blocks {
		byID[b.ID] = b
	}
	type item struct {
		loc blockLocation
		b   Block
	}
	items := make([]item, 0, len(hint.locations))
	for _, loc := range hint.locations {
		b, ok := byID[loc.id]
		if !ok {
			continue
		}
		if loc.kind == BlockKindImage || loc.kind == "image" {
			continue
		}
		items = append(items, item{loc: loc, b: b})
	}
	slices.SortFunc(items, func(a, b item) int { return cmp.Compare(b.loc.startIndex, a.loc.startIndex) })
	reqs := make([]DocsRequest, 0, len(items)*2)
	for _, it := range items {
		part, err := mapReplaceBlock(it.loc, it.b)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, part...)
	}
	return reqs, nil
}

// mapSetBlocksDeletes deletes locations + structural in reverse except the
// undeletable first section break. Images in keepIDs are left in place.
func mapSetBlocksDeletes(hint persistHint, tabID string, keepObjectIDs map[string]struct{}) []DocsRequest {
	type span struct {
		start, end int
		kind       string
		objectID   string
	}
	spans := make([]span, 0, len(hint.structural)+len(hint.locations))
	lowestSection := -1
	for _, s := range hint.structural {
		if tabID != "" && s.tabID != tabID {
			continue
		}
		if s.kind == "sectionBreak" && (lowestSection < 0 || s.startIndex < lowestSection) {
			lowestSection = s.startIndex
		}
		spans = append(spans, span{s.startIndex, s.endIndex, s.kind, ""})
	}
	for _, loc := range hint.locations {
		if tabID != "" && loc.tabID != tabID {
			continue
		}
		if loc.objectID != "" {
			if _, keep := keepObjectIDs[loc.objectID]; keep {
				continue
			}
		}
		spans = append(spans, span{loc.startIndex, loc.endIndex, loc.kind, loc.objectID})
	}
	slices.SortFunc(spans, func(a, b span) int { return cmp.Compare(b.start, a.start) })
	// last remaining body paragraph keeps its terminator
	var lastPara *span
	for i := range spans {
		s := &spans[i]
		if s.kind == "sectionBreak" && s.start == lowestSection {
			continue
		}
		if s.kind == BlockKindParagraph || s.kind == "paragraph" ||
			s.kind == BlockKindHeading || s.kind == "heading" ||
			s.kind == BlockKindListItem || s.kind == "list_item" {
			if lastPara == nil || s.end > lastPara.end {
				lastPara = s
			}
		}
	}
	reqs := make([]DocsRequest, 0, len(spans))
	for _, s := range spans {
		if s.kind == "sectionBreak" && s.start == lowestSection {
			continue
		}
		end := s.end
		if lastPara != nil && s.start == lastPara.start && s.end == lastPara.end && end > s.start {
			end = s.end - 1
		}
		if end <= s.start {
			continue
		}
		reqs = append(reqs, reqDelete(s.start, end, tabID))
	}
	return reqs
}

type insertChunk struct {
	reqs []DocsRequest
	// tableFill is set after insertTable; caller must Get then fill cells.
	tableFill bool
	rows      int
	cols      int
	grid      [][]string
	tabID     string
}

func mapInsertBlocks(blocks []Block, startIdx int, tabID string) (chunks []insertChunk, endIdx int, err error) {
	idx := startIdx
	var cur insertChunk
	flush := func() {
		if len(cur.reqs) > 0 || cur.tableFill {
			chunks = append(chunks, cur)
			cur = insertChunk{}
		}
	}
	i := 0
	for i < len(blocks) {
		b := blocks[i]
		switch b.Kind {
		case BlockKindImage:
			oid := blockAttr(b, "object_id")
			if oid == "" || strings.HasPrefix(oid, "img-html-") {
				return nil, 0, fmt.Errorf("%w: write: cannot insert a new image", ErrNotSupported)
			}
			// existing image kept by not deleting it; skip insert
			i++
		case BlockKindTable:
			rows, cols, err := tableShape(b)
			if err != nil {
				return nil, 0, err
			}
			grid, err := parseTSV(b.Text)
			if err != nil {
				return nil, 0, err
			}
			flush()
			chunks = append(chunks, insertChunk{
				reqs:      []DocsRequest{reqInsertTable(idx, tabID, rows, cols)},
				tableFill: true,
				rows:      rows,
				cols:      cols,
				grid:      grid,
				tabID:     tabID,
			})
			// Docs inserts a newline before the table; indexes shift after Get.
			idx += 2
			i++
		case BlockKindListItem:
			runStart := idx
			listID := blockAttr(b, "list_id")
			listType := blockAttr(b, "list_type")
			tabUnits := 0
			for i < len(blocks) && blocks[i].Kind == BlockKindListItem && blockAttr(blocks[i], "list_id") == listID {
				item := blocks[i]
				level := item.Style.Level
				if level < 1 {
					level = 1
				}
				tabs := strings.Repeat("\t", level-1)
				cur.reqs, idx = appendInsertStyled(cur.reqs, idx, tabID, "NORMAL_TEXT", tabs, item)
				tabUnits += docsIndexLen(tabs)
				i++
			}
			cur.reqs = append(cur.reqs, reqBullets(runStart, idx, tabID, listType))
			// createParagraphBullets strips the leading tabs used for nesting.
			idx -= tabUnits
		case BlockKindHeading:
			named := "HEADING_" + strconv.Itoa(clampHeading(b.Style.Level))
			cur.reqs, idx = appendInsertStyled(cur.reqs, idx, tabID, named, "", b)
			i++
		default:
			cur.reqs, idx = appendInsertStyled(cur.reqs, idx, tabID, "NORMAL_TEXT", "", b)
			i++
		}
	}
	flush()
	return chunks, idx, nil
}

func clampHeading(level int) int {
	if level < 1 {
		return 1
	}
	if level > 6 {
		return 6
	}
	return level
}

func keepImageObjectIDs(blocks []Block) map[string]struct{} {
	out := map[string]struct{}{}
	for _, b := range blocks {
		if b.Kind != BlockKindImage {
			continue
		}
		if oid := blockAttr(b, "object_id"); oid != "" && !strings.HasPrefix(oid, "img-html-") {
			out[oid] = struct{}{}
		}
	}
	return out
}
