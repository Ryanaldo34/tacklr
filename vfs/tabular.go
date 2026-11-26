package vfs

import (
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
// Zero Format means unspecified (do not send on write).
type Cell struct {
	Input  string
	Value  string
	Format CellFormat
}

// CellFormat is the stored portable bag on a cell (absolute state).
type CellFormat struct {
	Number    string      `json:"number,omitempty"`
	Bold      bool        `json:"bold,omitempty"`
	Italic    bool        `json:"italic,omitempty"`
	Strike    bool        `json:"strike,omitempty"`
	Underline bool        `json:"underline,omitempty"`
	Fill      string      `json:"fill,omitempty"`
	Color     string      `json:"color,omitempty"`
	Align     string      `json:"align,omitempty"`
	VAlign    string      `json:"valign,omitempty"`
	Wrap      string      `json:"wrap,omitempty"`
	Border    *CellBorder `json:"border,omitempty"`
	known     uint16
}

const (
	fmtNumber uint16 = 1 << iota
	fmtBold
	fmtItalic
	fmtStrike
	fmtUnderline
	fmtFill
	fmtColor
	fmtAlign
	fmtVAlign
	fmtWrap
	fmtBorder
)

// CellBorder is one named-style bag. Empty Edges means all four sides.
type CellBorder struct {
	Style string `json:"style,omitempty"`
	Color string `json:"color,omitempty"`
	Edges string `json:"edges,omitempty"`
}

func (c Cell) empty() bool { return c.Input == "" && c.Value == "" && c.Format.IsZero() }

// IsZero reports whether no format fields are set (do not send on write).
func (f CellFormat) IsZero() bool {
	return f.known == 0 && f.Number == "" && !f.Bold && !f.Italic && !f.Strike && !f.Underline &&
		f.Fill == "" && f.Color == "" && f.Align == "" && f.VAlign == "" && f.Wrap == "" &&
		(f.Border == nil || f.Border.zero())
}

func (f CellFormat) has(bit uint16) bool { return f.known&bit != 0 }

func (f *CellFormat) mark(bit uint16) { f.known |= bit }

func (b CellBorder) zero() bool {
	return b.Style == "" && b.Color == "" && b.Edges == ""
}

// String is the tool format bag: number=$#,##0.00,bold,border=thin:bottom
func (f CellFormat) String() string {
	if f.IsZero() {
		return ""
	}
	var parts []string
	if f.Number != "" {
		parts = append(parts, "number="+f.Number)
	}
	if f.Bold {
		parts = append(parts, "bold")
	} else if f.has(fmtBold) {
		parts = append(parts, "bold=false")
	}
	if f.Italic {
		parts = append(parts, "italic")
	} else if f.has(fmtItalic) {
		parts = append(parts, "italic=false")
	}
	if f.Strike {
		parts = append(parts, "strike")
	} else if f.has(fmtStrike) {
		parts = append(parts, "strike=false")
	}
	if f.Underline {
		parts = append(parts, "underline")
	} else if f.has(fmtUnderline) {
		parts = append(parts, "underline=false")
	}
	if f.Fill != "" {
		parts = append(parts, "fill="+f.Fill)
	}
	if f.Color != "" {
		parts = append(parts, "color="+f.Color)
	}
	if f.Align != "" {
		parts = append(parts, "align="+f.Align)
	}
	if f.VAlign != "" {
		parts = append(parts, "valign="+f.VAlign)
	}
	if f.Wrap != "" {
		parts = append(parts, "wrap="+f.Wrap)
	}
	if f.Border != nil && !f.Border.zero() {
		style := normalizeBorderStyle(f.Border.Style)
		b := "border=" + style
		if f.Border.Edges != "" {
			b += ":" + f.Border.Edges
		}
		if f.Border.Color != "" {
			b += ":" + f.Border.Color
		}
		parts = append(parts, b)
	}
	return strings.Join(parts, ",")
}

func normalizeBorderStyle(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "none", "thin", "medium", "thick", "dashed", "dotted", "double":
		return s
	default:
		return "thin"
	}
}

func normalizeAlign(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "left", "center", "right":
		return s
	default:
		return ""
	}
}

func normalizeVAlign(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "top", "middle", "bottom":
		return s
	default:
		return ""
	}
}

func normalizeWrap(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "overflow", "wrap", "clip":
		return s
	default:
		return ""
	}
}

// Normalize maps align, valign, wrap, and border style onto the portable bag.
func (f *CellFormat) Normalize() {
	if f.Align != "" {
		f.Align = normalizeAlign(f.Align)
	}
	if f.VAlign != "" {
		f.VAlign = normalizeVAlign(f.VAlign)
	}
	if f.Wrap != "" {
		f.Wrap = normalizeWrap(f.Wrap)
	}
	if f.Border == nil {
		return
	}
	if f.Border.zero() {
		f.Border = nil
		return
	}
	if f.Border.Style != "" {
		f.Border.Style = normalizeBorderStyle(f.Border.Style)
	}
}

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

// Sheet is one used rectangle. Cells[r][c] is 0-based; agent rows/cols are 1-based.
type Sheet struct {
	ID    string
	Title string
	Index int
	Rows  int
	Cols  int
	Cells [][]Cell
}

// gridBody is the spreadsheet representation (Sheets / Excel).
type gridBody struct {
	sheets []Sheet
	named  []NamedRange
	proj   string
	starts []int
}

// NewTabularDocument builds a grid checkout, trims trailing empty rows/cols, and
// rejects grids larger than MaxSheetCells.
func NewTabularDocument(path, mediaType string, sheets []Sheet, named []NamedRange) (*IR, error) {
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
func adoptTabularDocument(path, mediaType string, sheets []Sheet, named []NamedRange) (*IR, error) {
	n := 0
	for i := range sheets {
		trimSheet(&sheets[i])
		n += sheets[i].Rows * sheets[i].Cols
	}
	if n > MaxSheetCells {
		return nil, fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
	}
	b := &gridBody{sheets: sheets, named: named}
	b.reproject()
	return newIR(path, mediaType, b), nil
}

func (d *gridBody) text() string              { return d.proj }
func (d *gridBody) lineStarts() []int         { return d.starts }
func (d *gridBody) Sheets() []Sheet           { return d.sheets }
func (d *gridBody) NamedRanges() []NamedRange { return d.named }

func (d *gridBody) reproject() {
	d.proj = projectTabularTSV(d.sheets)
	d.starts = lineStartsOf(d.proj)
}

func projectTabularTSV(sheets []Sheet) string {
	var b strings.Builder
	need := 16
	for _, sh := range sheets {
		need += len(sh.Title) + 16 + sh.Rows*(sh.Cols+1)*8
	}
	b.Grow(need)
	for i, sh := range sheets {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("# Sheet: ")
		b.WriteString(sh.Title)
		for _, row := range sh.Cells {
			b.WriteByte('\n')
			writeToolRow(&b, row)
		}
	}
	return b.String()
}

// Blocks implements Structured: one kind=sheet block per sheet.
func (d *gridBody) blocks(string) []Block {
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
	for c := range sh.Cols {
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

// ParseA1 parses A1 or A1:C3 (1-based inclusive).
func ParseA1(s string) (r1, c1, r2, c2 int, err error) {
	return parseA1(s)
}

func parseA1(s string) (r1, c1, r2, c2 int, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, 0, 0, fmt.Errorf("%w: empty", ErrInvalidA1)
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
		return 0, 0, 0, 0, fmt.Errorf("%w: inverted range %q", ErrInvalidA1, s)
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
		return 0, 0, fmt.Errorf("%w %q", ErrInvalidA1, s)
	}
	row, err = strconv.Atoi(s[i:])
	if err != nil || row < 1 || col < 1 {
		return 0, 0, fmt.Errorf("%w %q", ErrInvalidA1, s)
	}
	return row, col, nil
}

// FormatA1 is the 1-based cell address (A1, B2, AA10).
func FormatA1(row, col int) string {
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
	for i := range sh.Cells {
		extendCells(&sh.Cells[i], cols)
	}
	sh.Rows = len(sh.Cells)
	sh.Cols = cols
}

func extendCells(row *[]Cell, cols int) {
	if n := cols - len(*row); n > 0 {
		*row = slices.Grow(*row, n)
		old := len(*row)
		*row = (*row)[:old+n]
		clear((*row)[old:])
	}
}

func cloneSheet(sh Sheet) Sheet {
	out := sh
	if sh.Cells != nil {
		out.Cells = make([][]Cell, len(sh.Cells))
		for i, row := range sh.Cells {
			out.Cells[i] = slices.Clone(row)
			for j := range out.Cells[i] {
				out.Cells[i][j].Format = cloneCellFormat(out.Cells[i][j].Format)
			}
		}
	}
	return out
}

func cloneCellFormat(f CellFormat) CellFormat {
	if f.Border != nil {
		b := *f.Border
		f.Border = &b
	}
	return f
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

func writeEscapedToolCell(b *strings.Builder, s string) {
	if !strings.ContainsAny(s, "\t\n\r\\") {
		b.WriteString(s)
		return
	}
	for i := range len(s) {
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
}

func writeToolRow(b *strings.Builder, row []Cell) {
	for i, c := range row {
		if i > 0 {
			b.WriteByte('\t')
		}
		writeEscapedToolCell(b, c.Display())
	}
}

func padStringGrid(rows [][]string) [][]string {
	cols := 0
	for _, row := range rows {
		if len(row) > cols {
			cols = len(row)
		}
	}
	for i := range rows {
		if n := cols - len(rows[i]); n > 0 {
			rows[i] = slices.Grow(rows[i], n)
			old := len(rows[i])
			rows[i] = rows[i][:old+n]
			clear(rows[i][old:])
		}
	}
	return rows
}

// HexColor normalizes a CSS/Office color to #rrggbb. Empty if not hex.
func HexColor(s string) string {
	r, g, b, ok := ParseRGB(s)
	if !ok {
		return ""
	}
	return FormatRGB(r, g, b)
}

// ParseRGB reads #rrggbb or #aarrggbb / rrggbb / aarrggbb.
func ParseRGB(s string) (r, g, b uint8, ok bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "#")
	if len(s) == 8 {
		s = s[2:]
	}
	if len(s) != 6 {
		return 0, 0, 0, false
	}
	n, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, 0, 0, false
	}
	return uint8((n >> 16) & 0xff), uint8((n >> 8) & 0xff), uint8(n & 0xff), true
}

// FormatRGB writes #rrggbb.
func FormatRGB(r, g, b uint8) string {
	var buf [7]byte
	buf[0] = '#'
	const digits = "0123456789abcdef"
	buf[1] = digits[r>>4]
	buf[2] = digits[r&15]
	buf[3] = digits[g>>4]
	buf[4] = digits[g&15]
	buf[5] = digits[b>>4]
	buf[6] = digits[b&15]
	return string(buf[:])
}
