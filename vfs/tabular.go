package vfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	mimeGoogleSpreadsheet = "application/vnd.google-apps.spreadsheet"
	// MaxSheetCells is the hard cap on loaded cells in one workbook.
	MaxSheetCells = 200_000
	headerPreview = 80
)

// Cell is one grid value. Input is what the agent writes ("Acme", "42", "=A1+1").
// Value is the provider's formatted/computed text; empty when unknown.
type Cell struct {
	Input string
	Value string
}

func (c Cell) empty() bool { return c.Input == "" && c.Value == "" }

// Display is the agent-visible cell: formula if present, else formatted value.
func (c Cell) Display() string {
	if strings.HasPrefix(c.Input, "=") {
		return c.Input
	}
	if c.Value != "" {
		return c.Value
	}
	return c.Input
}

// NamedRange is a workbook named range.
type NamedRange struct {
	Name, SheetID, A1 string
}

type gridMerge struct {
	r1, c1, r2, c2 int // 0-based, end exclusive
}

// Sheet is one used rectangle. Cells[r][c] is 0-based; agent rows/cols are 1-based.
type Sheet struct {
	ID    string
	Title string
	Index int
	Rows  int
	Cols  int
	Cells [][]Cell

	merges                   []gridMerge
	persistRows, persistCols int
}

// TabularDocument is the spreadsheet IR. Sheets are the source of truth;
// Text() is a derived HTML projection for FUSE / rg.
type TabularDocument struct {
	path, mediaType, encoding string
	sheets                    []Sheet
	named                     []NamedRange
	hint                      persistHint
	html                      string
	starts                    []int
}

// NewTabularDocument builds a workbook, trims trailing empty rows/cols, and
// rejects grids larger than MaxSheetCells.
func NewTabularDocument(path, mediaType string, sheets []Sheet, named []NamedRange) (*TabularDocument, error) {
	if mediaType == "" {
		mediaType = mimeGoogleSpreadsheet
	}
	cloned := make([]Sheet, len(sheets))
	for i, sh := range sheets {
		cloned[i] = cloneSheet(sh)
	}
	return adoptTabularDocument(path, mediaType, cloned, slices.Clone(named))
}

// adoptTabularDocument takes ownership of sheets and named (no clone).
func adoptTabularDocument(path, mediaType string, sheets []Sheet, named []NamedRange) (*TabularDocument, error) {
	n := 0
	for i := range sheets {
		trimSheet(&sheets[i])
		n += sheets[i].Rows * sheets[i].Cols
	}
	if n > MaxSheetCells {
		return nil, fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
	}
	d := &TabularDocument{
		path: path, mediaType: mediaType, encoding: "utf-8",
		sheets: sheets, named: named,
	}
	d.reproject()
	return d, nil
}

func (d *TabularDocument) Path() string      { return d.path }
func (d *TabularDocument) MediaType() string { return d.mediaType }
func (d *TabularDocument) Encoding() string  { return d.encoding }
func (d *TabularDocument) Text() string      { return d.html }
func (d *TabularDocument) LineCount() int    { return len(d.starts) }
func (d *TabularDocument) Sheets() []Sheet   { return d.sheets }
func (d *TabularDocument) NamedRanges() []NamedRange {
	return d.named
}

func (d *TabularDocument) Line(n int) (string, error) {
	if n < 1 || n > len(d.starts) {
		return "", ErrLineOutOfRange
	}
	return d.lineSlice(n - 1), nil
}

func (d *TabularDocument) Lines(start, end int) ([]string, error) {
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

func (d *TabularDocument) lineSlice(i int) string {
	start := d.starts[i]
	if i+1 < len(d.starts) {
		return d.html[start : d.starts[i+1]-1]
	}
	return d.html[start:]
}

func (d *TabularDocument) SetText(string) error { return ErrProjected }
func (d *TabularDocument) SetLine(int, string) error {
	return ErrProjected
}
func (d *TabularDocument) ReplaceLines(int, int, []string) error {
	return ErrProjected
}

func (d *TabularDocument) reproject() {
	d.html = projectTabularHTML(d.sheets)
	d.starts = lineStartsOf(d.html)
}

func (d *TabularDocument) cellCount() int {
	n := 0
	for _, sh := range d.sheets {
		n += sh.Rows * sh.Cols
	}
	return n
}

func (d *TabularDocument) checkCap() error {
	if d.cellCount() > MaxSheetCells {
		return fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
	}
	return nil
}

// Blocks implements Structured: one kind=sheet block per sheet.
func (d *TabularDocument) Blocks() []Block {
	used := make(map[string]int, len(d.sheets))
	out := make([]Block, len(d.sheets))
	for i, sh := range d.sheets {
		out[i] = Block{
			ID:   sheetBlockID(sh.Title, used),
			Kind: BlockKindSheet,
			Text: sheetPreview(sh),
			Style: StyleMeta{
				Level: i,
				Attributes: map[string]string{
					"sheet_id": sh.ID,
					"title":    sh.Title,
					"rows":     strconv.Itoa(sh.Rows),
					"cols":     strconv.Itoa(sh.Cols),
				},
			},
		}
	}
	return out
}

func sheetBlockID(title string, used map[string]int) string {
	seg := Slugify(title)
	if seg == "" {
		seg = "sheet"
	}
	used[seg]++
	if n := used[seg]; n > 1 {
		return seg + "-" + strconv.Itoa(n)
	}
	return seg
}

func sheetPreview(sh Sheet) string {
	var b strings.Builder
	b.WriteString("rows=")
	b.WriteString(strconv.Itoa(sh.Rows))
	b.WriteString(" cols=")
	b.WriteString(strconv.Itoa(sh.Cols))
	if sh.Rows == 0 || sh.Cols == 0 {
		return b.String()
	}
	b.WriteString(" | ")
	n := 0
	for c := 0; c < sh.Cols; c++ {
		if c > 0 {
			if n >= headerPreview {
				b.WriteString("…")
				return b.String()
			}
			b.WriteByte('\t')
			n++
		}
		cell := sh.Cells[0][c].Display()
		if n+len(cell) > headerPreview {
			b.WriteString(cell[:headerPreview-n])
			b.WriteString("…")
			return b.String()
		}
		b.WriteString(cell)
		n += len(cell)
	}
	return b.String()
}

// ContentFingerprint is SHA-256 of the grid (not the HTML projection).
func (d *TabularDocument) ContentFingerprint() string {
	h := sha256.New()
	var buf [24]byte
	for _, sh := range d.sheets {
		_, _ = h.Write(unsafeStringBytes(sh.ID))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(unsafeStringBytes(sh.Title))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(strconv.AppendInt(buf[:0], int64(sh.Rows), 10))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(strconv.AppendInt(buf[:0], int64(sh.Cols), 10))
		_, _ = h.Write(fingerprintNL)
		for _, row := range sh.Cells {
			for _, c := range row {
				_, _ = h.Write(unsafeStringBytes(c.Display()))
				_, _ = h.Write(fingerprintSep)
			}
			_, _ = h.Write(fingerprintNL)
		}
	}
	for _, n := range d.named {
		_, _ = h.Write(unsafeStringBytes(n.Name))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(unsafeStringBytes(n.SheetID))
		_, _ = h.Write(fingerprintSep)
		_, _ = h.Write(unsafeStringBytes(n.A1))
		_, _ = h.Write(fingerprintNL)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (d *TabularDocument) findSheet(key string) (int, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		if len(d.sheets) == 1 {
			return 0, true
		}
		return -1, false
	}
	for i, sh := range d.sheets {
		if sh.ID == key || sh.Title == key {
			return i, true
		}
	}
	used := make(map[string]int, len(d.sheets))
	for i, sh := range d.sheets {
		if sheetBlockID(sh.Title, used) == key {
			return i, true
		}
	}
	return -1, false
}

// SplitSheetAddr splits block_id into sheet key and optional A1 (Sheet!B2).
func SplitSheetAddr(blockID string) (sheet, a1 string) {
	blockID = strings.TrimSpace(blockID)
	if blockID == "" {
		return "", ""
	}
	if strings.HasPrefix(blockID, "'") {
		if i := strings.Index(blockID[1:], "'!"); i >= 0 {
			return blockID[1 : 1+i], blockID[1+i+2:]
		}
	}
	if i := strings.LastIndex(blockID, "!"); i >= 0 {
		return blockID[:i], blockID[i+1:]
	}
	return blockID, ""
}

// ParseA1 parses A1 or A1:C3 (1-based inclusive).
func ParseA1(s string) (r1, c1, r2, c2 int, err error) {
	return parseA1(s)
}

func parseA1(s string) (r1, c1, r2, c2 int, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, 0, 0, fmt.Errorf("vfs: empty A1")
	}
	left, right, isRange := strings.Cut(s, ":")
	r1, c1, err = parseA1Cell(left)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if !isRange {
		return r1, c1, r1, c1, nil
	}
	r2, c2, err = parseA1Cell(right)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if r2 < r1 || c2 < c1 {
		return 0, 0, 0, 0, fmt.Errorf("vfs: inverted A1 range %q", s)
	}
	return r1, c1, r2, c2, nil
}

func parseA1Cell(s string) (row, col int, err error) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) {
		r := s[i]
		if r >= 'a' && r <= 'z' {
			r -= 'a' - 'A'
		}
		if r < 'A' || r > 'Z' {
			break
		}
		col = col*26 + int(r-'A'+1)
		i++
	}
	if i == 0 || i == len(s) {
		return 0, 0, fmt.Errorf("vfs: invalid A1 %q", s)
	}
	row, err = strconv.Atoi(s[i:])
	if err != nil || row < 1 || col < 1 {
		return 0, 0, fmt.Errorf("vfs: invalid A1 %q", s)
	}
	return row, col, nil
}

func formatA1(row, col int) string {
	return colLetters(col) + strconv.Itoa(row)
}

func colLetters(col int) string {
	if col < 1 {
		return ""
	}
	var b [8]byte
	n := len(b)
	for col > 0 {
		col--
		n--
		b[n] = byte('A' + col%26)
		col /= 26
	}
	return string(b[n:])
}

func sheetA1(title string, r1, c1, r2, c2 int) string {
	q := quoteSheetTitle(title)
	if r1 == r2 && c1 == c2 {
		return q + "!" + formatA1(r1, c1)
	}
	return q + "!" + formatA1(r1, c1) + ":" + formatA1(r2, c2)
}

func quoteSheetTitle(title string) string {
	if title == "" {
		return "Sheet1"
	}
	if strings.ContainsAny(title, " '![]") {
		return "'" + strings.ReplaceAll(title, "'", "''") + "'"
	}
	return title
}

func (d *TabularDocument) ReadRows(key string, start, end int) (Sheet, []string, error) {
	i, ok := d.findSheet(key)
	if !ok {
		return Sheet{}, nil, fmt.Errorf("unknown block_id %q", key)
	}
	sh := d.sheets[i]
	if start < 1 || end < start {
		return Sheet{}, nil, ErrLineOutOfRange
	}
	if start > sh.Rows+1 {
		return Sheet{}, nil, ErrLineOutOfRange
	}
	if end > sh.Rows+1 {
		end = sh.Rows + 1
	}
	lines := make([]string, 0, end-start)
	for r := start - 1; r < end-1; r++ {
		lines = append(lines, encodeToolRow(sh.Cells[r]))
	}
	return sh, lines, nil
}

func (d *TabularDocument) ReadCell(key, a1 string) (string, error) {
	i, ok := d.findSheet(key)
	if !ok {
		return "", fmt.Errorf("unknown block_id %q", key)
	}
	r1, c1, r2, c2, err := parseA1(a1)
	if err != nil {
		return "", err
	}
	if r1 != r2 || c1 != c2 {
		return "", fmt.Errorf("vfs: not a single cell %q", a1)
	}
	return d.sheets[i].cellAt(r1, c1).Display(), nil
}

func (d *TabularDocument) ReadRangeTSV(key, a1 string) (string, error) {
	i, ok := d.findSheet(key)
	if !ok {
		return "", fmt.Errorf("unknown block_id %q", key)
	}
	r1, c1, r2, c2, err := parseA1(a1)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if r2 >= r1 && c2 >= c1 {
		b.Grow((r2 - r1 + 1) * (c2 - c1 + 1) * 8)
	}
	for r := r1; r <= r2; r++ {
		if r > r1 {
			b.WriteByte('\n')
		}
		for c := c1; c <= c2; c++ {
			if c > c1 {
				b.WriteByte('\t')
			}
			b.WriteString(escapeToolCell(d.sheets[i].cellAt(r, c).Display()))
		}
	}
	return b.String(), nil
}

func (sh Sheet) cellAt(row, col int) Cell {
	if row < 1 || col < 1 || row > sh.Rows || col > sh.Cols {
		return Cell{}
	}
	return sh.Cells[row-1][col-1]
}

func (sh Sheet) mergeHit(r, c int) bool {
	for _, m := range sh.merges {
		if r >= m.r1 && r < m.r2 && c >= m.c1 && c < m.c2 {
			return true
		}
	}
	return false
}

// WithMerge records a 0-based half-open merge rectangle from a provider checkout.
func WithMerge(sh Sheet, startRow, startCol, endRow, endCol int) Sheet {
	sh.merges = append(slices.Clone(sh.merges), gridMerge{
		r1: startRow, c1: startCol, r2: endRow, c2: endCol,
	})
	return sh
}

func (d *TabularDocument) overlayCell(idx, row, col int, input string) error {
	sh := &d.sheets[idx]
	if sh.mergeHit(row-1, col-1) {
		return fmt.Errorf("%w: write into a merge", ErrNotSupported)
	}
	growSheet(sh, row, col)
	sh.Cells[row-1][col-1] = Cell{Input: input}
	trimSheet(sh)
	return d.finishMut()
}

func (d *TabularDocument) overlayRows(idx, start, end int, lines []string) error {
	if end-start != len(lines) {
		return fmt.Errorf("%w: line count must equal end-start", ErrNotSupported)
	}
	sh := &d.sheets[idx]
	cols := sh.Cols
	parsed := make([][]string, len(lines))
	for i, line := range lines {
		cells := splitToolRow(line)
		parsed[i] = cells
		if len(cells) > cols {
			cols = len(cells)
		}
	}
	if cols < 1 {
		cols = 1
	}
	growSheet(sh, end-1, cols)
	for i, cells := range parsed {
		r := start - 1 + i
		for c := 0; c < sh.Cols; c++ {
			if sh.mergeHit(r, c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
			val := ""
			if c < len(cells) {
				val = cells[c]
			}
			sh.Cells[r][c] = Cell{Input: val}
		}
	}
	trimSheet(sh)
	return d.finishMut()
}

func (d *TabularDocument) overlayRange(idx, r1, c1, r2, c2 int, lines []string) error {
	rows := r2 - r1 + 1
	cols := c2 - c1 + 1
	if len(lines) != rows {
		return fmt.Errorf("%w: line count must equal end-start", ErrNotSupported)
	}
	sh := &d.sheets[idx]
	growSheet(sh, r2, c2)
	for i, line := range lines {
		cells := splitToolRow(line)
		if len(cells) > cols {
			return fmt.Errorf("%w: row has %d cells, range is %d cols", ErrNotSupported, len(cells), cols)
		}
		r := r1 - 1 + i
		for c := 0; c < cols; c++ {
			if sh.mergeHit(r, c1-1+c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
			val := ""
			if c < len(cells) {
				val = cells[c]
			}
			sh.Cells[r][c1-1+c] = Cell{Input: val}
		}
	}
	trimSheet(sh)
	return d.finishMut()
}

func (d *TabularDocument) replaceSheetValues(idx int, grid [][]Cell) error {
	sh := &d.sheets[idx]
	for r := range grid {
		for c := range grid[r] {
			if sh.mergeHit(r, c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
		}
	}
	oldR, oldC := sh.Rows, sh.Cols
	sh.Cells = grid
	trimSheet(sh)
	sh.persistRows = max(oldR, sh.Rows)
	sh.persistCols = max(oldC, sh.Cols)
	return d.finishMut()
}

func (d *TabularDocument) finishMut() error {
	if err := d.checkCap(); err != nil {
		return err
	}
	d.reproject()
	return nil
}

func growSheet(sh *Sheet, rows, cols int) {
	if rows < sh.Rows {
		rows = sh.Rows
	}
	if cols < sh.Cols {
		cols = sh.Cols
	}
	if extra := rows - len(sh.Cells); extra > 0 {
		sh.Cells = slices.Grow(sh.Cells, extra)
		for len(sh.Cells) < rows {
			sh.Cells = append(sh.Cells, make([]Cell, cols))
		}
	}
	for i := range sh.Cells {
		if len(sh.Cells[i]) < cols {
			row := make([]Cell, cols)
			copy(row, sh.Cells[i])
			sh.Cells[i] = row
		}
	}
	sh.Rows = len(sh.Cells)
	if len(sh.Cells) > 0 {
		sh.Cols = len(sh.Cells[0])
	} else {
		sh.Cols = cols
	}
}

func trimSheet(sh *Sheet) {
	normalizeSheetShape(sh)
	lastR := -1
	lastC := -1
	for r, row := range sh.Cells {
		for c, cell := range row {
			if !cell.empty() {
				if r > lastR {
					lastR = r
				}
				if c > lastC {
					lastC = c
				}
			}
		}
	}
	if lastR < 0 {
		sh.Cells = nil
		sh.Rows, sh.Cols = 0, 0
		return
	}
	sh.Rows = lastR + 1
	sh.Cols = lastC + 1
	sh.Cells = slices.Clip(sh.Cells[:sh.Rows])
	for i := range sh.Cells {
		row := sh.Cells[i]
		if len(row) > sh.Cols {
			row = row[:sh.Cols]
		}
		sh.Cells[i] = slices.Clip(row)
	}
}

func normalizeSheetShape(sh *Sheet) {
	cols := sh.Cols
	for _, row := range sh.Cells {
		if len(row) > cols {
			cols = len(row)
		}
	}
	if cols > 0 {
		for i := range sh.Cells {
			if len(sh.Cells[i]) < cols {
				row := make([]Cell, cols)
				copy(row, sh.Cells[i])
				sh.Cells[i] = row
			}
		}
	}
	if extra := sh.Rows - len(sh.Cells); extra > 0 {
		sh.Cells = slices.Grow(sh.Cells, extra)
		for len(sh.Cells) < sh.Rows {
			sh.Cells = append(sh.Cells, make([]Cell, cols))
		}
	}
	sh.Rows = len(sh.Cells)
	sh.Cols = cols
}

func cloneSheet(sh Sheet) Sheet {
	out := sh
	if sh.Cells != nil {
		out.Cells = make([][]Cell, len(sh.Cells))
		for i, row := range sh.Cells {
			out.Cells[i] = slices.Clone(row)
		}
	}
	out.merges = slices.Clone(sh.merges)
	return out
}

func cellsFromStrings(grid [][]string) [][]Cell {
	out := make([][]Cell, len(grid))
	for i, row := range grid {
		out[i] = make([]Cell, len(row))
		for j, s := range row {
			out[i][j] = Cell{Input: s, Value: s}
		}
	}
	return out
}

func escapeToolCell(s string) string {
	if !strings.ContainsAny(s, "\t\n\r\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func unescapeToolCell(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case 't':
			b.WriteByte('\t')
			i++
		case 'n':
			b.WriteByte('\n')
			i++
		case 'r':
			b.WriteByte('\r')
			i++
		case '\\':
			b.WriteByte('\\')
			i++
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func encodeToolRow(row []Cell) string {
	if len(row) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(len(row) * 8)
	for i, c := range row {
		if i > 0 {
			b.WriteByte('\t')
		}
		b.WriteString(escapeToolCell(c.Display()))
	}
	return b.String()
}

func splitToolRow(line string) []string {
	if line == "" {
		return []string{""}
	}
	parts := strings.Split(line, "\t")
	for i, p := range parts {
		parts[i] = unescapeToolCell(p)
	}
	return parts
}

func parseToolTSV(text string) [][]string {
	text = strings.TrimRight(text, "\r\n")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	out := make([][]string, len(lines))
	for i, line := range lines {
		out[i] = splitToolRow(strings.TrimSuffix(line, "\r"))
	}
	return padStringGrid(out)
}

func cellsFromToolLines(lines []string) [][]Cell {
	if len(lines) == 0 {
		return nil
	}
	grid := make([][]string, len(lines))
	for i, line := range lines {
		grid[i] = splitToolRow(strings.TrimSuffix(line, "\r"))
	}
	return cellsFromStrings(padStringGrid(grid))
}

func padStringGrid(rows [][]string) [][]string {
	cols := 0
	for _, row := range rows {
		if len(row) > cols {
			cols = len(row)
		}
	}
	if cols == 0 {
		return rows
	}
	for i := range rows {
		if len(rows[i]) < cols {
			row := make([]string, cols)
			copy(row, rows[i])
			rows[i] = row
		}
	}
	return rows
}

func isSpreadsheet(mediaType string) bool {
	return normalizeMediaType(mediaType) == mimeGoogleSpreadsheet
}

var (
	_ Document   = (*TabularDocument)(nil)
	_ Textual    = (*TabularDocument)(nil)
	_ Structured = (*TabularDocument)(nil)
)
