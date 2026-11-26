package vfs

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/google/sheets"
	"github.com/ryanaldo34/tacklr/internal/rest"
)

// SheetsAPI is the Sheets subset used by the provider. NewGoogleSheets
// talks to the service.
type SheetsAPI interface {
	Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error)
}

// SheetsSnapshot is the checkout used to build TabularDocument.
type SheetsSnapshot struct {
	Sheets []Sheet
	Named  []NamedRange
}

// googleSheets implements SheetsAPI with the Sheets v4 REST API.
type googleSheets struct {
	c rest.Client
}

// NewGoogleSheets returns a SheetsAPI that authenticates with holder
// (required). base is the API root ("" for Google's); httpClient nil is
// http.DefaultClient.
func NewGoogleSheets(holder *TokenHolder, base string, httpClient *http.Client) SheetsAPI {
	return googleSheets{c: apiClient(holder, base, "https://sheets.googleapis.com/", httpClient)}
}

func (g googleSheets) Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error) {
	var meta sheets.Spreadsheet
	q := url.Values{
		"includeGridData": {"true"},
		"fields":          {"sheets.properties,sheets.data,namedRanges"},
	}
	if err := g.c.Call(ctx, http.MethodGet, "v4/spreadsheets/"+url.PathEscape(spreadsheetID), q, nil, &meta); err != nil {
		return SheetsSnapshot{}, mapDocsError(err)
	}
	var snap SheetsSnapshot
	for i, sh := range meta.Sheets {
		if sh == nil || sh.Properties == nil {
			continue
		}
		title := sh.Properties.Title
		id := strconv.FormatInt(sh.Properties.SheetID, 10)
		item := Sheet{
			ID:    id,
			Title: title,
			Index: int(sh.Properties.Index),
		}
		if item.Index == 0 {
			item.Index = i
		}
		if err := fillSheetGrid(&item, sh.Data); err != nil {
			return SheetsSnapshot{}, err
		}
		snap.Sheets = append(snap.Sheets, item)
	}
	for _, nr := range meta.NamedRanges {
		if nr == nil {
			continue
		}
		n := NamedRange{Name: nr.Name}
		if nr.Range != nil {
			n.SheetID = strconv.FormatInt(nr.Range.SheetID, 10)
			n.A1 = gridRangeA1(nr.Range)
		}
		snap.Named = append(snap.Named, n)
	}
	return snap, nil
}

// fillSheetGrid lays the grid data blocks of one sheet into sh.Cells.
func fillSheetGrid(sh *Sheet, data []*sheets.GridData) error {
	type origin struct {
		gd     *sheets.GridData
		sr, sc int
	}
	blocks := make([]origin, 0, len(data))
	rows, cols := 0, 0
	for _, gd := range data {
		if gd == nil {
			continue
		}
		sr, sc, err := gridOrigin(gd.StartRow, gd.StartColumn)
		if err != nil {
			return err
		}
		blocks = append(blocks, origin{gd, sr, sc})
		rows = max(rows, sr+len(gd.RowData))
		for _, rd := range gd.RowData {
			n := sc
			if rd != nil {
				n += len(rd.Values)
			}
			cols = max(cols, n)
		}
	}
	if rows == 0 || cols == 0 {
		return nil
	}
	if cols > MaxSheetCells || rows > MaxSheetCells/cols {
		return fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
	}
	grid := make([][]Cell, rows)
	for r := range grid {
		grid[r] = make([]Cell, cols)
	}
	for _, b := range blocks {
		gd, sr, sc := b.gd, b.sr, b.sc
		for i, rd := range gd.RowData {
			if rd == nil {
				continue
			}
			for j, cd := range rd.Values {
				if cd == nil {
					continue
				}
				grid[sr+i][sc+j] = gridCell(cd)
			}
		}
	}
	sh.Cells = grid
	trimSheet(sh)
	return nil
}

func gridCell(cd *sheets.CellData) Cell {
	c := Cell{Value: cd.FormattedValue, Input: extendedInput(cd.UserEnteredValue)}
	if c.Input == "" {
		c.Input = c.Value
	}
	c.Format = cellFormatFromSheets(cd.UserEnteredFormat)
	return c
}

func extendedInput(v *sheets.ExtendedValue) string {
	if v == nil {
		return ""
	}
	if v.FormulaValue != nil {
		s := *v.FormulaValue
		if s != "" && !strings.HasPrefix(s, "=") {
			s = "=" + s
		}
		return s
	}
	if v.StringValue != nil {
		return *v.StringValue
	}
	if v.NumberValue != nil {
		return numberString(*v.NumberValue)
	}
	if v.BoolValue != nil {
		return strings.ToUpper(strconv.FormatBool(*v.BoolValue))
	}
	return ""
}

func cellFormatFromSheets(f *sheets.CellFormat) CellFormat {
	if f == nil {
		return CellFormat{}
	}
	out := CellFormat{}
	if f.NumberFormat != nil {
		out.Number = f.NumberFormat.Pattern
		out.mark(fmtNumber)
	}
	if tf := f.TextFormat; tf != nil {
		out.Bold = tf.Bold
		out.Italic = tf.Italic
		out.Strike = tf.Strikethrough
		out.Underline = tf.Underline
		out.mark(fmtBold)
		out.mark(fmtItalic)
		out.mark(fmtStrike)
		out.mark(fmtUnderline)
		if c := colorHex(tf.ForegroundColor, tf.ForegroundColorStyle); c != "" {
			out.Color = c
			out.mark(fmtColor)
		}
	}
	if c := colorHex(f.BackgroundColor, f.BackgroundColorStyle); c != "" {
		out.Fill = c
		out.mark(fmtFill)
	}
	if a := normalizeAlign(f.HorizontalAlignment); a != "" {
		out.Align = a
		out.mark(fmtAlign)
	}
	if a := normalizeVAlign(f.VerticalAlignment); a != "" {
		out.VAlign = a
		out.mark(fmtVAlign)
	}
	if w := sheetsWrap(f.WrapStrategy); w != "" {
		out.Wrap = w
		out.mark(fmtWrap)
	}
	if b := borderFromSheets(f.Borders); b != nil {
		out.Border = b
		out.mark(fmtBorder)
	}
	return out
}

func sheetsWrap(s string) string {
	switch strings.ToUpper(s) {
	case "WRAP":
		return "wrap"
	case "OVERFLOW_CELL":
		return "overflow"
	case "CLIP":
		return "clip"
	default:
		return ""
	}
}

func borderFromSheets(b *sheets.Borders) *CellBorder {
	if b == nil {
		return nil
	}
	type edge struct {
		name string
		b    *sheets.Border
	}
	edges := []edge{{"top", b.Top}, {"bottom", b.Bottom}, {"left", b.Left}, {"right", b.Right}}
	style, color := "", ""
	var names []string
	for _, e := range edges {
		if e.b == nil || e.b.Style == "" || e.b.Style == "NONE" {
			continue
		}
		st := sheetsBorderStyle(e.b.Style)
		col := colorHex(e.b.Color, e.b.ColorStyle)
		if style == "" {
			style, color = st, col
		} else if st != style {
			continue
		}
		names = append(names, e.name)
	}
	if style == "" {
		return nil
	}
	out := &CellBorder{Style: style, Color: color}
	if len(names) < 4 {
		out.Edges = strings.Join(names, ",")
	}
	return out
}

// sheetsBorderStyle is the vfs name of a Sheets border style other than
// NONE. SOLID and unknown styles are thin.
func sheetsBorderStyle(s string) string {
	switch s {
	case "SOLID_MEDIUM":
		return "medium"
	case "SOLID_THICK":
		return "thick"
	case "DASHED":
		return "dashed"
	case "DOTTED":
		return "dotted"
	case "DOUBLE":
		return "double"
	default:
		return "thin"
	}
}

func gridOrigin(startRow, startCol int64) (row, col int, err error) {
	if startRow < 0 || startCol < 0 || startRow > int64(MaxSheetCells) || startCol > int64(MaxSheetCells) {
		return 0, 0, fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
	}
	return int(startRow), int(startCol), nil
}

func colorHex(c *sheets.Color, style *sheets.ColorStyle) string {
	if style != nil && style.RGBColor != nil {
		c = style.RGBColor
	}
	if c == nil {
		return ""
	}
	return FormatRGB(round255(c.Red), round255(c.Green), round255(c.Blue))
}

// round255 is a 0..1 color component as 0..255, clamped.
func round255(v float64) uint8 {
	return uint8(min(max(v*255+0.5, 0), 255))
}

// numberString is a number cell input: an integer has no fraction.
func numberString(x float64) string {
	if x == float64(int64(x)) {
		return strconv.FormatInt(int64(x), 10)
	}
	return strconv.FormatFloat(x, 'f', -1, 64)
}

func gridRangeA1(r *sheets.GridRange) string {
	r1 := int(r.StartRowIndex) + 1
	c1 := int(r.StartColumnIndex) + 1
	r2 := int(r.EndRowIndex)
	c2 := int(r.EndColumnIndex)
	if r2 < r1 {
		r2 = r1
	}
	if c2 < c1 {
		c2 = c1
	}
	return FormatA1(r1, c1) + ":" + FormatA1(r2, c2)
}
