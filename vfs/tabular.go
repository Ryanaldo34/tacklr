package vfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
// Mutation uses FormatPatch so agents can clear fields (bold=false).
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

// FormatPatch is an explicit format write. Nil pointer = leave; set pointer = write
// (including false / empty). Zero Border clears the border.
type FormatPatch struct {
	Number    *string     `json:"number,omitempty"`
	Bold      *bool       `json:"bold,omitempty"`
	Italic    *bool       `json:"italic,omitempty"`
	Strike    *bool       `json:"strike,omitempty"`
	Underline *bool       `json:"underline,omitempty"`
	Fill      *string     `json:"fill,omitempty"`
	Color     *string     `json:"color,omitempty"`
	Align     *string     `json:"align,omitempty"`
	VAlign    *string     `json:"valign,omitempty"`
	Wrap      *string     `json:"wrap,omitempty"`
	Border    *CellBorder `json:"border,omitempty"`
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

func (p *FormatPatch) empty() bool {
	if p == nil {
		return true
	}
	return p.Number == nil && p.Bold == nil && p.Italic == nil && p.Strike == nil &&
		p.Underline == nil && p.Fill == nil && p.Color == nil && p.Align == nil &&
		p.VAlign == nil && p.Wrap == nil && p.Border == nil
}

// IsZero reports whether no format fields are set (do not send on write).
func (f CellFormat) IsZero() bool {
	return f.known == 0 && f.Number == "" && !f.Bold && !f.Italic && !f.Strike && !f.Underline &&
		f.Fill == "" && f.Color == "" && f.Align == "" && f.VAlign == "" && f.Wrap == "" &&
		(f.Border == nil || f.Border.zero())
}

func (f CellFormat) has(bit uint16) bool { return f.known&bit != 0 }

func (f *CellFormat) mark(bit uint16) {
	if f != nil {
		f.known |= bit
	}
}

func (b CellBorder) zero() bool {
	return b.Style == "" && b.Color == "" && b.Edges == ""
}

func (f CellFormat) equal(o CellFormat) bool {
	if f.known != o.known || f.Number != o.Number || f.Bold != o.Bold || f.Italic != o.Italic ||
		f.Strike != o.Strike || f.Underline != o.Underline || f.Fill != o.Fill ||
		f.Color != o.Color || f.Align != o.Align || f.VAlign != o.VAlign || f.Wrap != o.Wrap {
		return false
	}
	if f.Border == nil || o.Border == nil {
		return f.Border == nil && o.Border == nil
	}
	return *f.Border == *o.Border
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

// Overlay copies src fields that are known or non-zero onto f.
func (f *CellFormat) Overlay(src CellFormat) {
	if f == nil {
		return
	}
	overlayCellFormat(f, src)
}

func overlayCellFormat(dst *CellFormat, src CellFormat) {
	if dst == nil {
		return
	}
	if src.has(fmtNumber) || src.Number != "" {
		dst.Number = src.Number
		dst.mark(fmtNumber)
	}
	if src.has(fmtBold) || src.Bold {
		dst.Bold = src.Bold
		dst.mark(fmtBold)
	}
	if src.has(fmtItalic) || src.Italic {
		dst.Italic = src.Italic
		dst.mark(fmtItalic)
	}
	if src.has(fmtStrike) || src.Strike {
		dst.Strike = src.Strike
		dst.mark(fmtStrike)
	}
	if src.has(fmtUnderline) || src.Underline {
		dst.Underline = src.Underline
		dst.mark(fmtUnderline)
	}
	if src.has(fmtFill) || src.Fill != "" {
		dst.Fill = src.Fill
		dst.mark(fmtFill)
	}
	if src.has(fmtColor) || src.Color != "" {
		dst.Color = src.Color
		dst.mark(fmtColor)
	}
	if src.has(fmtAlign) || src.Align != "" {
		if a := normalizeAlign(src.Align); a != "" || src.has(fmtAlign) {
			dst.Align = a
			dst.mark(fmtAlign)
		}
	}
	if src.has(fmtVAlign) || src.VAlign != "" {
		if a := normalizeVAlign(src.VAlign); a != "" || src.has(fmtVAlign) {
			dst.VAlign = a
			dst.mark(fmtVAlign)
		}
	}
	if src.has(fmtWrap) || src.Wrap != "" {
		if a := normalizeWrap(src.Wrap); a != "" || src.has(fmtWrap) {
			dst.Wrap = a
			dst.mark(fmtWrap)
		}
	}
	if src.has(fmtBorder) || src.Border != nil {
		if src.Border == nil || src.Border.zero() {
			dst.Border = nil
		} else {
			b := *src.Border
			b.Style = normalizeBorderStyle(b.Style)
			dst.Border = &b
		}
		dst.mark(fmtBorder)
	}
}

// ApplyPatch writes each set field, including false and empty.
func (f *CellFormat) ApplyPatch(p FormatPatch) {
	if f == nil {
		return
	}
	if p.Number != nil {
		f.Number = *p.Number
		f.mark(fmtNumber)
	}
	if p.Bold != nil {
		f.Bold = *p.Bold
		f.mark(fmtBold)
	}
	if p.Italic != nil {
		f.Italic = *p.Italic
		f.mark(fmtItalic)
	}
	if p.Strike != nil {
		f.Strike = *p.Strike
		f.mark(fmtStrike)
	}
	if p.Underline != nil {
		f.Underline = *p.Underline
		f.mark(fmtUnderline)
	}
	if p.Fill != nil {
		f.Fill = *p.Fill
		f.mark(fmtFill)
	}
	if p.Color != nil {
		f.Color = *p.Color
		f.mark(fmtColor)
	}
	if p.Align != nil {
		f.Align = normalizeAlign(*p.Align)
		f.mark(fmtAlign)
	}
	if p.VAlign != nil {
		f.VAlign = normalizeVAlign(*p.VAlign)
		f.mark(fmtVAlign)
	}
	if p.Wrap != nil {
		f.Wrap = normalizeWrap(*p.Wrap)
		f.mark(fmtWrap)
	}
	if p.Border != nil {
		if p.Border.zero() {
			f.Border = nil
		} else {
			b := *p.Border
			b.Style = normalizeBorderStyle(b.Style)
			f.Border = &b
		}
		f.mark(fmtBorder)
	}
}

// ParseCellFormat reads the String() bag (number=...,bold,bold=false,border=thin:bottom).
func ParseCellFormat(s string) (CellFormat, error) {
	var f CellFormat
	s = strings.TrimSpace(s)
	if s == "" {
		return f, nil
	}
	parts := strings.Split(s, ",")
	for i := 0; i < len(parts); {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			i++
			continue
		}
		key, val, hasVal := strings.Cut(part, "=")
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "number" {
			for i+1 < len(parts) && !formatBagKey(parts[i+1]) {
				i++
				val += "," + parts[i]
			}
		}
		i++
		switch key {
		case "number":
			f.Number = val
			f.mark(fmtNumber)
		case "bold":
			f.Bold = parseFormatFlag(hasVal, val)
			f.mark(fmtBold)
		case "italic":
			f.Italic = parseFormatFlag(hasVal, val)
			f.mark(fmtItalic)
		case "strike":
			f.Strike = parseFormatFlag(hasVal, val)
			f.mark(fmtStrike)
		case "underline":
			f.Underline = parseFormatFlag(hasVal, val)
			f.mark(fmtUnderline)
		case "fill":
			f.Fill = val
			f.mark(fmtFill)
		case "color":
			f.Color = val
			f.mark(fmtColor)
		case "align":
			f.Align = normalizeAlign(val)
			f.mark(fmtAlign)
		case "valign":
			f.VAlign = normalizeVAlign(val)
			f.mark(fmtVAlign)
		case "wrap":
			f.Wrap = normalizeWrap(val)
			f.mark(fmtWrap)
		case "border":
			b, err := parseBorderBag(val)
			if err != nil {
				return CellFormat{}, err
			}
			f.Border = b
			f.mark(fmtBorder)
		default:
			return CellFormat{}, fmt.Errorf("vfs: unknown format field %q", key)
		}
	}
	return f, nil
}

func formatBagKey(part string) bool {
	part = strings.TrimSpace(part)
	key, _, _ := strings.Cut(part, "=")
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "number", "bold", "italic", "strike", "underline", "fill", "color", "align", "valign", "wrap", "border":
		return true
	default:
		return false
	}
}

func parseFormatFlag(hasVal bool, val string) bool {
	if !hasVal {
		return true
	}
	return !strings.EqualFold(strings.TrimSpace(val), "false")
}

func parseBorderBag(s string) (*CellBorder, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return &CellBorder{}, nil
	}
	parts := strings.Split(s, ":")
	b := &CellBorder{Style: normalizeBorderStyle(parts[0])}
	if len(parts) > 1 && parts[1] != "" {
		if strings.Contains(parts[1], "#") || looksHexColor(parts[1]) {
			b.Color = HexColor(parts[1])
		} else {
			b.Edges = parts[1]
		}
	}
	if len(parts) > 2 {
		b.Color = HexColor(parts[2])
	}
	return b, nil
}

func looksHexColor(s string) bool {
	s = strings.TrimPrefix(strings.TrimSpace(s), "#")
	if len(s) != 6 && len(s) != 8 {
		return false
	}
	_, err := strconv.ParseUint(s, 16, 32)
	return err == nil
}

// Normalize maps align, valign, wrap, and border style onto the portable bag.
func (f *CellFormat) Normalize() {
	if f == nil {
		return
	}
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

func applyCellOverlay(dst *Cell, input string, hasValue bool, patch *FormatPatch) {
	if hasValue {
		dst.Input = input
		if strings.HasPrefix(input, "=") {
			dst.Value = ""
		} else {
			dst.Value = input
		}
	}
	if patch != nil {
		dst.Format.ApplyPatch(*patch)
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
	return newIR(path, mediaType, "utf-8", b), nil
}

func (d *gridBody) text() string              { return d.proj }
func (d *gridBody) lineStarts() []int         { return d.starts }
func (d *gridBody) setText(string) error      { return ErrProjected }
func (d *gridBody) setLine(int, string) error { return ErrProjected }
func (d *gridBody) replaceLines(int, int, []string) error {
	return ErrProjected
}
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

func (d *gridBody) cellCount() int {
	n := 0
	for _, sh := range d.sheets {
		n += sh.Rows * sh.Cols
	}
	return n
}

func (d *gridBody) checkCap() error {
	if d.cellCount() > MaxSheetCells {
		return fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
	}
	return nil
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

// ContentFingerprint is SHA-256 of the grid (not the TSV projection).
// Formula Cell.Value is ignored (Display uses Input). Format fields are included
// so a format-only mutation changes rev.
func (d *gridBody) fingerprint() string {
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
				c.Format.writeFingerprint(h)
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

func (f CellFormat) writeFingerprint(w io.Writer) {
	if f.IsZero() {
		return
	}
	_, _ = w.Write(unsafeStringBytes(f.Number))
	_, _ = w.Write(fingerprintSep)
	if f.Bold {
		_, _ = w.Write(fingerprintOne)
	}
	_, _ = w.Write(fingerprintSep)
	if f.Italic {
		_, _ = w.Write(fingerprintOne)
	}
	_, _ = w.Write(fingerprintSep)
	if f.Strike {
		_, _ = w.Write(fingerprintOne)
	}
	_, _ = w.Write(fingerprintSep)
	if f.Underline {
		_, _ = w.Write(fingerprintOne)
	}
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Fill))
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Color))
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Align))
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.VAlign))
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Wrap))
	if f.Border == nil || f.Border.zero() {
		return
	}
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Border.Style))
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Border.Color))
	_, _ = w.Write(fingerprintSep)
	_, _ = w.Write(unsafeStringBytes(f.Border.Edges))
}

func (d *gridBody) findSheet(key string) (int, bool) {
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

func sheetA1(title string, r1, c1, r2, c2 int) string {
	q := quoteSheetTitle(title)
	if r1 == r2 && c1 == c2 {
		return q + "!" + FormatA1(r1, c1)
	}
	return q + "!" + FormatA1(r1, c1) + ":" + FormatA1(r2, c2)
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

func (d *gridBody) ReadRows(key string, start, end int) (Sheet, []string, error) {
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

func (d *gridBody) ReadCell(key, a1 string) (string, error) {
	c, err := d.Cell(key, a1)
	if err != nil {
		return "", err
	}
	return c.Display(), nil
}

// Cell returns the cell at sheet!A1 (single cell).
func (d *gridBody) Cell(key, a1 string) (Cell, error) {
	i, ok := d.findSheet(key)
	if !ok {
		return Cell{}, fmt.Errorf("unknown block_id %q", key)
	}
	r1, c1, r2, c2, err := parseA1(a1)
	if err != nil {
		return Cell{}, err
	}
	if r1 != r2 || c1 != c2 {
		return Cell{}, fmt.Errorf("vfs: not a single cell %q", a1)
	}
	return d.sheets[i].cellAt(r1, c1), nil
}

func (d *gridBody) ReadRangeTSV(key, a1 string) (string, error) {
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

func (sh Sheet) mergeSlave(r, c int) bool {
	for _, m := range sh.merges {
		if r >= m.r1 && r < m.r2 && c >= m.c1 && c < m.c2 {
			return r != m.r1 || c != m.c1
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

func (d *gridBody) overlayCell(idx, row, col int, input *string, format *FormatPatch) error {
	sh := &d.sheets[idx]
	if sh.mergeSlave(row-1, col-1) {
		return fmt.Errorf("%w: write into a merge", ErrNotSupported)
	}
	growSheet(sh, row, col)
	if input != nil {
		applyCellOverlay(&sh.Cells[row-1][col-1], *input, true, format)
	} else {
		applyCellOverlay(&sh.Cells[row-1][col-1], "", false, format)
	}
	trimSheet(sh)
	return d.finishMut()
}

func (d *gridBody) overlayRows(idx, start, end int, lines []string, hasValue bool, format *FormatPatch) error {
	if hasValue && end-start != len(lines) {
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
	last := end - 1
	if last < 1 {
		last = 1
	}
	growSheet(sh, last, cols)
	n := end - start
	if !hasValue {
		n = last - (start - 1)
		if n < 0 {
			n = 0
		}
	}
	for i := 0; i < n; i++ {
		r := start - 1 + i
		if r < 0 || r >= len(sh.Cells) {
			continue
		}
		var cells []string
		if hasValue && i < len(parsed) {
			cells = parsed[i]
		}
		for c := 0; c < sh.Cols; c++ {
			if sh.mergeSlave(r, c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
			val := ""
			if hasValue && c < len(cells) {
				val = cells[c]
			}
			applyCellOverlay(&sh.Cells[r][c], val, hasValue, format)
		}
	}
	trimSheet(sh)
	return d.finishMut()
}

func (d *gridBody) overlayRange(idx, r1, c1, r2, c2 int, lines []string, hasValue bool, format *FormatPatch) error {
	rows := r2 - r1 + 1
	cols := c2 - c1 + 1
	if hasValue && len(lines) != rows {
		return fmt.Errorf("%w: line count must equal end-start", ErrNotSupported)
	}
	sh := &d.sheets[idx]
	growSheet(sh, r2, c2)
	for i := 0; i < rows; i++ {
		var cells []string
		if hasValue {
			cells = splitToolRow(lines[i])
			if len(cells) > cols {
				return fmt.Errorf("%w: row has %d cells, range is %d cols", ErrNotSupported, len(cells), cols)
			}
		}
		r := r1 - 1 + i
		for c := 0; c < cols; c++ {
			if sh.mergeSlave(r, c1-1+c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
			val := ""
			if hasValue && c < len(cells) {
				val = cells[c]
			}
			applyCellOverlay(&sh.Cells[r][c1-1+c], val, hasValue, format)
		}
	}
	trimSheet(sh)
	return d.finishMut()
}

func (d *gridBody) replaceSheetValues(idx int, grid [][]Cell) error {
	sh := &d.sheets[idx]
	for r := range grid {
		for c := range grid[r] {
			if sh.mergeSlave(r, c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
		}
	}
	oldR, oldC := sh.Rows, sh.Cols
	old := sh.Cells
	for r := range grid {
		for c := range grid[r] {
			if r < len(old) && c < len(old[r]) {
				grid[r][c].Format = cloneCellFormat(old[r][c].Format)
			}
		}
	}
	sh.Cells = grid
	trimSheet(sh)
	sh.persistRows = max(oldR, sh.Rows)
	sh.persistCols = max(oldC, sh.Cols)
	return d.finishMut()
}

func (d *gridBody) overlaySheetFormat(idx int, format *FormatPatch) error {
	if format == nil {
		return nil
	}
	sh := &d.sheets[idx]
	for r := 0; r < sh.Rows; r++ {
		for c := 0; c < sh.Cols; c++ {
			if sh.mergeSlave(r, c) {
				return fmt.Errorf("%w: write into a merge", ErrNotSupported)
			}
			applyCellOverlay(&sh.Cells[r][c], "", false, format)
		}
	}
	trimSheet(sh)
	return d.finishMut()
}

func (d *gridBody) finishMut() error {
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
		extendCells(&sh.Cells[i], cols)
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
			extendCells(&sh.Cells[i], cols)
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
	out.merges = slices.Clone(sh.merges)
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

func escapeToolCell(s string) string {
	if !strings.ContainsAny(s, "\t\n\r\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	writeEscapedToolCell(&b, s)
	return b.String()
}

func writeEscapedToolCell(b *strings.Builder, s string) {
	if !strings.ContainsAny(s, "\t\n\r\\") {
		b.WriteString(s)
		return
	}
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
	writeToolRow(&b, row)
	return b.String()
}

func writeToolRow(b *strings.Builder, row []Cell) {
	for i, c := range row {
		if i > 0 {
			b.WriteByte('\t')
		}
		writeEscapedToolCell(b, c.Display())
	}
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
		if n := cols - len(rows[i]); n > 0 {
			rows[i] = slices.Grow(rows[i], n)
			old := len(rows[i])
			rows[i] = rows[i][:old+n]
			clear(rows[i][old:])
		}
	}
	return rows
}

type formatRect struct {
	r1, c1, r2, c2 int
	format         CellFormat
}

func formatRects(sh Sheet, rows, cols int) []formatRect {
	if rows < 1 || cols < 1 {
		return nil
	}
	visited := make([][]bool, rows)
	for i := range visited {
		visited[i] = make([]bool, cols)
	}
	var out []formatRect
	for r := 0; r < rows; r++ {
		for c := 0; c < cols; c++ {
			if visited[r][c] {
				continue
			}
			f := formatAt(sh, r, c)
			if f.IsZero() {
				continue
			}
			w := 1
			for c+w < cols && !visited[r][c+w] && formatAt(sh, r, c+w).equal(f) {
				w++
			}
			h := 1
			grow := true
			for grow && r+h < rows {
				for x := 0; x < w; x++ {
					if visited[r+h][c+x] || !formatAt(sh, r+h, c+x).equal(f) {
						grow = false
						break
					}
				}
				if grow {
					h++
				}
			}
			for y := 0; y < h; y++ {
				for x := 0; x < w; x++ {
					visited[r+y][c+x] = true
				}
			}
			out = append(out, formatRect{r1: r, c1: c, r2: r + h, c2: c + w, format: f})
		}
	}
	return out
}

func formatAt(sh Sheet, r, c int) CellFormat {
	if r < 0 || c < 0 || r >= len(sh.Cells) || c >= len(sh.Cells[r]) {
		return CellFormat{}
	}
	return sh.Cells[r][c].Format
}

// HexColor normalizes a CSS/Office color to #rrggbb. Empty if not hex.
func HexColor(s string) string {
	r, g, b, ok := ParseRGB(s)
	if !ok {
		return ""
	}
	return FormatRGB(r, g, b)
}

// ExcelARGB is the 8-digit AARRGGBB form used by Office XML.
func ExcelARGB(s string) string {
	r, g, b, ok := ParseRGB(s)
	if !ok {
		return "FF000000"
	}
	return "FF" + strings.ToUpper(FormatRGB(r, g, b)[1:])
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
