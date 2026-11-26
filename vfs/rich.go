package vfs

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

const (
	mimeGoogleDocument = "application/vnd.google-apps.document"
	mimeGoogleFolder   = "application/vnd.google-apps.folder"
	mimeGoogleShortcut = "application/vnd.google-apps.shortcut"
	mimeExportHTMLZip  = "application/zip"
	// MaxDocsExportBytes is the Drive files.export limit.
	MaxDocsExportBytes = 10 << 20
)

// DocTab is one document tab (Docs includeTabsContent).
type DocTab struct {
	ID    string
	Title string
	Index int
}

// richBody is the block-tree representation (Docs / Word).
type richBody struct {
	tree   []Block
	html   string
	starts []int
	tabs   []DocTab
}

func NewRichDocument(path, mediaType string, blocks []Block) *IR {
	return newRichDocument(path, mediaType, blocks, nil)
}

func newRichDocument(path, mediaType string, blocks []Block, tabs []DocTab) *IR {
	if mediaType == "" {
		mediaType = mimeGoogleDocument
	}
	b := &richBody{tabs: tabs}
	b.tree = assignBlockIDs(cloneBlocks(blocks), tabs)
	b.reproject()
	return newIR(path, mediaType, b)
}

func (d *richBody) text() string      { return d.html }
func (d *richBody) lineStarts() []int { return d.starts }

func (d *richBody) blocks(_ string) []Block { return d.tree }
func (d *richBody) Blocks() []Block         { return d.tree }
func (d *richBody) Tabs() []DocTab          { return d.tabs }

func (d *richBody) reproject() {
	htmlOut, spans := projectHTMLSpans(d.tree, d.tabs)
	d.html = htmlOut
	d.starts = lineStartsOf(d.html)
	for i := range d.tree {
		if i < len(spans) {
			d.tree[i].Style.Span = spans[i]
		}
	}
}

func cloneBlocks(in []Block) []Block {
	out := make([]Block, len(in))
	for i, b := range in {
		out[i] = b
		out[i].Style.Attributes = maps.Clone(b.Style.Attributes)
		out[i].Runs = cloneRuns(b.Runs)
		normalizeInline(&out[i])
	}
	return out
}

func assignBlockIDs(blocks []Block, tabs []DocTab) []Block {
	titleOf := map[string]string{}
	for _, t := range tabs {
		titleOf[t.ID] = t.Title
	}
	type tabState struct {
		p, table, imgHTML int
		listItem          map[string]int
		used              map[string]int
		stack             []idFrame
	}
	states := map[string]*tabState{}
	state := func(tabID string) *tabState {
		s := states[tabID]
		if s == nil {
			s = &tabState{listItem: map[string]int{}, used: map[string]int{}}
			states[tabID] = s
		}
		return s
	}
	prefix := func(tabID string) string {
		title := titleOf[tabID]
		if title == "" {
			return ""
		}
		seg := Slugify(title)
		if seg == "" {
			return ""
		}
		return seg + "/"
	}
	for i, b := range blocks {
		tabID := blockAttr(b, "tab_id")
		st := state(tabID)
		pre := prefix(tabID)
		switch b.Kind {
		case BlockKindHeading:
			for len(st.stack) > 0 && st.stack[len(st.stack)-1].level >= b.Style.Level {
				st.stack = st.stack[:len(st.stack)-1]
			}
			seg := Slugify(b.PlainText())
			if seg == "" {
				seg = "section"
			}
			base := pre + seg
			if len(st.stack) > 0 {
				base = st.stack[len(st.stack)-1].path + "/" + seg
			} else if pre != "" {
				base = strings.TrimSuffix(pre, "/") + "/" + seg
			}
			st.used[base]++
			id := base
			if n := st.used[base]; n > 1 {
				id = base + "-" + strconv.Itoa(n)
			}
			st.stack = append(st.stack, idFrame{level: b.Style.Level, path: id})
			blocks[i].ID = id
			setAttr(&blocks[i], "heading_path", id)
		case BlockKindParagraph:
			st.p++
			blocks[i].ID = pre + "p-" + strconv.Itoa(st.p)
		case BlockKindListItem:
			listID := blockAttr(b, "list_id")
			if listID == "" {
				listID = "list"
				setAttr(&blocks[i], "list_id", listID)
			}
			st.listItem[listID]++
			blocks[i].ID = listID + "/" + strconv.Itoa(st.listItem[listID])
		case BlockKindTable:
			st.table++
			blocks[i].ID = pre + "table-" + strconv.Itoa(st.table)
		case BlockKindImage:
			oid := blockAttr(b, "object_id")
			if oid != "" {
				blocks[i].ID = "img-" + oid
			} else {
				st.imgHTML++
				id := "img-html-" + strconv.Itoa(st.imgHTML)
				blocks[i].ID = id
				setAttr(&blocks[i], "object_id", id)
			}
		default:
			if blocks[i].ID == "" {
				st.p++
				blocks[i].ID = pre + "p-" + strconv.Itoa(st.p)
			}
		}
	}
	return blocks
}

type idFrame struct {
	level int
	path  string
}

func blockAttr(b Block, key string) string {
	if b.Style.Attributes == nil {
		return ""
	}
	return b.Style.Attributes[key]
}

func setAttr(b *Block, key, val string) {
	if b.Style.Attributes == nil {
		b.Style.Attributes = map[string]string{}
	}
	b.Style.Attributes[key] = val
}

func lineStartsOf(text string) []int {
	if text == "" {
		return nil
	}
	starts := make([]int, 1, strings.Count(text, "\n")+1)
	starts[0] = 0
	for i := range len(text) {
		if text[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

func parseTSV(text string) ([][]string, error) {
	if text == "" {
		return nil, nil
	}
	lines := strings.Split(text, "\n")
	out := make([][]string, len(lines))
	cols := -1
	for i, line := range lines {
		cells := strings.Split(line, "\t")
		if cols < 0 {
			cols = len(cells)
		} else if len(cells) != cols {
			return nil, fmt.Errorf("%w: ragged table row", ErrNotSupported)
		}
		out[i] = cells
	}
	return out, nil
}

func encodeTSV(rows [][]string) string {
	if len(rows) == 0 {
		return ""
	}
	n := 0
	for _, row := range rows {
		n += len(row)
		for _, cell := range row {
			n += len(cell)
		}
	}
	var b strings.Builder
	b.Grow(n)
	for i, row := range rows {
		if i > 0 {
			b.WriteByte('\n')
		}
		for j, cell := range row {
			if j > 0 {
				b.WriteByte('\t')
			}
			b.WriteString(cell)
		}
	}
	return b.String()
}

func sanitizeCell(s string) string {
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
