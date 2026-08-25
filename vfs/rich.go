package vfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
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

// richBody is the block-tree representation (Docs / Word).
type richBody struct {
	tree   []Block
	html   string
	starts []int
	tabs   []DocTab
	mut    richMutation
}

// NewRichDocument builds a block-tree checkout. persistHint stays empty until
// the provider attaches one.
func liftPlaintext(s string) []Block {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	var out []Block
	for _, para := range strings.Split(s, "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		if grid, ok := parsePipeTable(para); ok {
			out = append(out, tableBlock(grid))
			continue
		}
		out = append(out, Block{Kind: BlockKindParagraph, Text: para})
	}
	if len(out) == 0 {
		out = append(out, Block{Kind: BlockKindParagraph, Text: s})
	}
	return out
}

func createRichDocument(path, mediaType string, mut Mutation) (Document, error) {
	if mut.Content != nil {
		if looksLikeHTML(*mut.Content) {
			blocks, err := decodeDocsHTML([]byte(*mut.Content))
			if err != nil {
				return nil, err
			}
			return NewRichDocument(path, mediaType, blocks), nil
		}
		return NewRichDocument(path, mediaType, liftPlaintext(*mut.Content)), nil
	}
	return NewRichDocument(path, mediaType, mut.Blocks), nil
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
	return newIR(path, mediaType, "utf-8", b)
}

func (d *richBody) text() string      { return d.html }
func (d *richBody) lineStarts() []int { return d.starts }

func (d *richBody) setText(text string) error {
	return d.setFromHTML(text)
}

func (d *richBody) setLine(n int, line string) error {
	return d.replaceLines(n, n+1, []string{line})
}

func (d *richBody) replaceLines(start, end int, replacement []string) error {
	s, err := spliceLines(d.html, d.starts, start, end, replacement)
	if err != nil {
		return err
	}
	return d.setFromHTML(s)
}

func (d *richBody) setFromHTML(html string) error {
	blocks, err := decodeDocsHTML([]byte(html))
	if err != nil {
		return err
	}
	if len(blocks) == 0 {
		return ErrEmptyReplace
	}
	d.SetBlocks(blocks)
	return nil
}
func (d *richBody) blocks(_ string) []Block { return d.tree }
func (d *richBody) Blocks() []Block         { return d.tree }
func (d *richBody) Tabs() []DocTab          { return d.tabs }

// ReplaceBlock mutates one IR block and re-projects HTML.
func (d *richBody) ReplaceBlock(id string, text string, includeHeading bool) error {
	i := -1
	for j, b := range d.tree {
		if b.ID == id {
			i = j
			break
		}
	}
	if i < 0 {
		if b, ok := FindBlock(d.tree, id); ok {
			for j, x := range d.tree {
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
	b := d.tree[i]
	switch b.Kind {
	case BlockKindImage:
		return fmt.Errorf("%w: write: cannot replace an image; omit it from blocks to delete, or leave it", ErrNotSupported)
	case BlockKindHeading:
		if !includeHeading {
			return fmt.Errorf("write: heading %q has no body; set include_heading or use the following block_id", id)
		}
		b.Text = text
		b.Runs = nil
	case BlockKindTable:
		rows, cols, err := tableShape(b)
		if err != nil {
			return err
		}
		got, err := parseTableText(text)
		if err != nil {
			return err
		}
		if len(got) != rows || (rows > 0 && len(got[0]) != cols) {
			return fmt.Errorf("%w: table shape must stay %dx%d", ErrNotSupported, rows, cols)
		}
		b.Text = encodeTSV(got)
		b.Runs = nil
	default:
		b.Text = text
		b.Runs = nil
	}
	normalizeInline(&b)
	d.tree[i] = b
	d.mut = richReplace
	d.reproject()
	return nil
}

// SetBlocks replaces the IR and re-projects. Empty lists are allowed here;
// WriteDocument / the write tool reject an empty replace on an existing path.
func (d *richBody) SetBlocks(blocks []Block) {
	d.tree = assignBlockIDs(cloneBlocks(blocks), d.tabs)
	d.mut = richSet
	d.reproject()
}

// ContentFingerprint is SHA-256 of kind, text, level, and sorted attributes.
// Span and ID are ignored.
func (d *richBody) fingerprint() string {
	h := sha256.New()
	var level [12]byte
	for _, bl := range d.tree {
		_, _ = h.Write(unsafeStringBytes(bl.Kind))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(unsafeStringBytes(bl.Text))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(strconv.AppendInt(level[:0], int64(bl.Style.Level), 10))
		_, _ = h.Write(fingerprintSep)
		if len(bl.Style.Attributes) > 0 {
			keys := slices.Sorted(maps.Keys(bl.Style.Attributes))
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
	fingerprintOne = []byte{'1'}
)

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

func attachPersistHint(doc Document, hint persistHint) {
	if d, ok := asIR(doc); ok {
		d.hint = hint
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
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

func tableShape(b Block) (rows, cols int, err error) {
	grid, perr := parseTableText(b.Text)
	if perr == nil && len(grid) > 0 && len(grid[0]) > 0 {
		return len(grid), len(grid[0]), nil
	}
	if b.Style.Attributes != nil {
		rows, _ = strconv.Atoi(b.Style.Attributes["rows"])
		cols, _ = strconv.Atoi(b.Style.Attributes["cols"])
	}
	if rows == 0 || cols == 0 {
		if perr != nil {
			return 0, 0, perr
		}
		return 0, 0, fmt.Errorf("%w: table has no shape", ErrNotSupported)
	}
	return rows, cols, nil
}

func tableBlock(grid [][]string) Block {
	rows, cols := len(grid), 0
	if rows > 0 {
		cols = len(grid[0])
	}
	return Block{
		Kind: BlockKindTable,
		Text: encodeTSV(grid),
		Style: StyleMeta{Attributes: map[string]string{
			"rows": strconv.Itoa(rows),
			"cols": strconv.Itoa(cols),
		}},
	}
}

// parseTableText accepts TSV (tabs) or a GFM pipe table. Agents often write
// kind=table with "| a | b |" markdown; treating that as TSV makes a 1-column
// Docs table with the pipes still in the cell.
func parseTableText(text string) ([][]string, error) {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.TrimRight(text, "\n")
	if text == "" {
		return nil, nil
	}
	if strings.Contains(text, "\t") {
		return parseTSV(text)
	}
	if grid, ok := parsePipeTable(text); ok {
		return grid, nil
	}
	return parseTSV(text)
}

func parsePipeTable(text string) ([][]string, bool) {
	var rows [][]string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.Contains(line, "|") {
			return nil, false
		}
		cells := parsePipeRow(line)
		if isPipeSeparatorRow(cells) {
			continue
		}
		if len(cells) == 0 {
			continue
		}
		rows = append(rows, cells)
	}
	if len(rows) == 0 {
		return nil, false
	}
	cols := 0
	for _, r := range rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	if cols < 2 {
		return nil, false
	}
	for i := range rows {
		for len(rows[i]) < cols {
			rows[i] = append(rows[i], "")
		}
	}
	return rows, true
}

func parsePipeRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	parts := strings.Split(line, "|")
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = strings.TrimSpace(p)
	}
	return out
}

func isPipeSeparatorRow(cells []string) bool {
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		t := strings.TrimSpace(c)
		if t == "" {
			return false
		}
		hasDash := false
		for _, r := range t {
			if r == '-' {
				hasDash = true
				continue
			}
			if r != ':' {
				return false
			}
		}
		if !hasDash {
			return false
		}
	}
	return true
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
