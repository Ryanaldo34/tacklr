package vfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
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

type persistHint struct {
	fileID     string
	revisionID string
	locations  []blockLocation
	structural []structuralSpan
}

type structuralSpan struct {
	tabID                string
	startIndex, endIndex int
	kind                 string // sectionBreak | tableOfContents
}

type blockLocation struct {
	id         string
	tabID      string
	startIndex int
	endIndex   int
	kind       string
	objectID   string
	cells      []cellLocation
}

type cellLocation struct {
	row, col             int
	startIndex, endIndex int
}

type richMutation int

const (
	richClean richMutation = iota
	richReplace
	richSet
)

// RichDocument is the office/cloud IR. Blocks are the source of truth;
// Text() is a derived HTML projection for FUSE / rg.
type RichDocument struct {
	path, mediaType, encoding string
	blocks                    []Block
	html                      string
	starts                    []int
	hint                      persistHint
	tabs                      []DocTab
	mut                       richMutation
}

// NewRichDocument builds a RichDocument from blocks. IDs and HTML spans are
// assigned here. persistHint stays empty until the provider attaches one.
func NewRichDocument(path, mediaType string, blocks []Block) *RichDocument {
	return newRichDocument(path, mediaType, blocks, nil)
}

func newRichDocument(path, mediaType string, blocks []Block, tabs []DocTab) *RichDocument {
	if mediaType == "" {
		mediaType = mimeGoogleDocument
	}
	d := &RichDocument{path: path, mediaType: mediaType, encoding: "utf-8", tabs: tabs}
	d.blocks = assignBlockIDs(cloneBlocks(blocks), tabs)
	d.reproject()
	return d
}

func (d *RichDocument) Path() string      { return d.path }
func (d *RichDocument) MediaType() string { return d.mediaType }
func (d *RichDocument) Encoding() string  { return d.encoding }
func (d *RichDocument) Text() string      { return d.html }
func (d *RichDocument) LineCount() int    { return len(d.starts) }
func (d *RichDocument) Blocks() []Block   { return d.blocks }
func (d *RichDocument) Tabs() []DocTab    { return d.tabs }

func (d *RichDocument) Line(n int) (string, error) {
	if n < 1 || n > len(d.starts) {
		return "", ErrLineOutOfRange
	}
	return d.lineSlice(n - 1), nil
}

func (d *RichDocument) Lines(start, end int) ([]string, error) {
	n := len(d.starts)
	if start < 1 || end < start || end > n+1 {
		return nil, ErrLineOutOfRange
	}
	if start == end {
		return []string{}, nil
	}
	out := make([]string, end-start)
	for i := range out {
		out[i] = d.lineSlice(start - 1 + i)
	}
	return out, nil
}

func (d *RichDocument) lineSlice(i int) string {
	start := d.starts[i]
	if i+1 < len(d.starts) {
		return d.html[start : d.starts[i+1]-1]
	}
	return d.html[start:]
}

func (d *RichDocument) SetText(string) error { return ErrProjected }
func (d *RichDocument) SetLine(int, string) error {
	return ErrProjected
}
func (d *RichDocument) ReplaceLines(int, int, []string) error {
	return ErrProjected
}

// ReplaceBlock mutates one IR block and re-projects HTML.
func (d *RichDocument) ReplaceBlock(id string, text string, includeHeading bool) error {
	i := -1
	for j, b := range d.blocks {
		if b.ID == id {
			i = j
			break
		}
	}
	if i < 0 {
		if b, ok := FindBlock(d.blocks, id); ok {
			for j, x := range d.blocks {
				if x.ID == b.ID {
					i = j
					break
				}
			}
		}
	}
	if i < 0 {
		return fmt.Errorf("unknown block_id %q", id)
	}
	b := d.blocks[i]
	switch b.Kind {
	case BlockKindImage:
		return fmt.Errorf("%w: write: cannot replace an image; omit it from blocks to delete, or leave it", ErrNotSupported)
	case BlockKindHeading:
		if !includeHeading {
			return fmt.Errorf("write: heading %q has no body; set include_heading or use the following block_id", id)
		}
		b.Text = text
	case BlockKindTable:
		rows, cols, err := tableShape(b)
		if err != nil {
			return err
		}
		got, err := parseTSV(text)
		if err != nil {
			return err
		}
		if len(got) != rows || (rows > 0 && len(got[0]) != cols) {
			return fmt.Errorf("%w: table shape must stay %dx%d", ErrNotSupported, rows, cols)
		}
		b.Text = encodeTSV(got)
	default:
		b.Text = text
	}
	d.blocks[i] = b
	d.mut = richReplace
	d.reproject()
	return nil
}

// SetBlocks replaces the IR and re-projects. Empty lists are allowed here;
// WriteDocument / the write tool reject an empty replace on an existing path.
func (d *RichDocument) SetBlocks(blocks []Block) {
	d.blocks = assignBlockIDs(cloneBlocks(blocks), d.tabs)
	d.mut = richSet
	d.reproject()
}

// ContentFingerprint is SHA-256 of kind, text, level, and sorted attributes.
// Span and ID are ignored.
func (d *RichDocument) ContentFingerprint() string {
	h := sha256.New()
	var level [12]byte
	for _, bl := range d.blocks {
		_, _ = h.Write(unsafeStringBytes(bl.Kind))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(unsafeStringBytes(bl.Text))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(strconv.AppendInt(level[:0], int64(bl.Style.Level), 10))
		_, _ = h.Write(fingerprintSep)
		if len(bl.Style.Attributes) > 0 {
			keys := make([]string, 0, len(bl.Style.Attributes))
			for k := range bl.Style.Attributes {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				_, _ = h.Write(unsafeStringBytes(k))
				_, _ = h.Write(fingerprintEq)
				_, _ = h.Write(unsafeStringBytes(bl.Style.Attributes[k]))
				_, _ = h.Write(fingerprintSep)
			}
		}
		_, _ = h.Write(fingerprintNL)
	}
	return hex.EncodeToString(h.Sum(nil))
}

var (
	fingerprintSep = []byte{0}
	fingerprintEq  = []byte{'='}
	fingerprintNL  = []byte{'\n'}
)

func (d *RichDocument) reproject() {
	htmlOut, spans := projectHTMLSpans(d.blocks, d.tabs)
	d.html = htmlOut
	d.starts = lineStartsOf(d.html)
	for i := range d.blocks {
		if i < len(spans) {
			d.blocks[i].Style.Span = spans[i]
		}
	}
}

func attachPersistHint(doc Document, hint persistHint) {
	if d, ok := doc.(*RichDocument); ok {
		d.hint = hint
	}
}

func cloneBlocks(in []Block) []Block {
	out := make([]Block, len(in))
	for i, b := range in {
		out[i] = b
		if b.Style.Attributes != nil {
			attrs := make(map[string]string, len(b.Style.Attributes))
			for k, v := range b.Style.Attributes {
				attrs[k] = v
			}
			out[i].Style.Attributes = attrs
		}
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
			seg := Slugify(b.Text)
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
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

func tableShape(b Block) (rows, cols int, err error) {
	if b.Style.Attributes != nil {
		rows, _ = strconv.Atoi(b.Style.Attributes["rows"])
		cols, _ = strconv.Atoi(b.Style.Attributes["cols"])
	}
	if rows == 0 || cols == 0 {
		grid, perr := parseTSV(b.Text)
		if perr != nil {
			return 0, 0, perr
		}
		rows = len(grid)
		if rows > 0 {
			cols = len(grid[0])
		}
	}
	if rows == 0 || cols == 0 {
		return 0, 0, fmt.Errorf("%w: table has no shape", ErrNotSupported)
	}
	return rows, cols, nil
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

func docsIndexLen(s string) int {
	n := 0
	for _, r := range s {
		if utf16.RuneLen(r) == 2 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

var (
	_ Document   = (*RichDocument)(nil)
	_ Textual    = (*RichDocument)(nil)
	_ Structured = (*RichDocument)(nil)
)
