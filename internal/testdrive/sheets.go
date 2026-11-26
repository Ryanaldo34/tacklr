package testdrive

import (
	"cmp"
	"net/http"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/google/sheets"

	"github.com/ryanaldo34/tacklr/vfs"
)

// SeedSheet sets spreadsheets.get state for id.
func (d *FX) SeedSheet(id string, snap vfs.SheetsSnapshot) {
	d.SeedSheetJSON(id, spreadsheetJSON(snap))
}

// SeedSheetJSON sets the spreadsheets.get reply for id to book, a JSON
// value as the Sheets API sends it.
func (d *FX) SeedSheetJSON(id string, book any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.books[id] = book
}

// serveSheetsGet answers spreadsheets.get. A Sheet that was not seeded is
// empty.
func (d *FX) serveSheetsGet(w http.ResponseWriter, _ *http.Request, id string) {
	book, ok := d.books[id]
	if !ok {
		book = spreadsheetJSON(vfs.SheetsSnapshot{})
	}
	writeDriveJSON(w, http.StatusOK, book)
}

// spreadsheetJSON is snap as spreadsheets.get answers it with grid data.
func spreadsheetJSON(snap vfs.SheetsSnapshot) sheets.Spreadsheet {
	var out sheets.Spreadsheet
	for _, sh := range snap.Sheets {
		sid, _ := strconv.ParseInt(sh.ID, 10, 64)
		item := &sheets.Sheet{Properties: &sheets.SheetProperties{SheetID: sid, Title: sh.Title, Index: int64(sh.Index)}}
		grid := &sheets.GridData{RowData: make([]*sheets.RowData, len(sh.Cells))}
		for r, row := range sh.Cells {
			vals := make([]*sheets.CellData, len(row))
			for c, cell := range row {
				vals[c] = cellJSON(cell)
			}
			grid.RowData[r] = &sheets.RowData{Values: vals}
		}
		item.Data = []*sheets.GridData{grid}
		out.Sheets = append(out.Sheets, item)
	}
	for _, nr := range snap.Named {
		item := &sheets.NamedRange{Name: nr.Name}
		if r1, c1, r2, c2, err := vfs.ParseA1(nr.A1); err == nil {
			sid, _ := strconv.ParseInt(nr.SheetID, 10, 64)
			item.Range = &sheets.GridRange{
				SheetID: sid, StartRowIndex: int64(r1 - 1), StartColumnIndex: int64(c1 - 1),
				EndRowIndex: int64(r2), EndColumnIndex: int64(c2),
			}
		}
		out.NamedRanges = append(out.NamedRanges, item)
	}
	return out
}

func cellJSON(c vfs.Cell) *sheets.CellData {
	out := &sheets.CellData{FormattedValue: c.Value}
	if c.Input != "" {
		in := c.Input
		if strings.HasPrefix(in, "=") {
			out.UserEnteredValue = &sheets.ExtendedValue{FormulaValue: &in}
		} else {
			out.UserEnteredValue = &sheets.ExtendedValue{StringValue: &in}
		}
	}
	if !c.Format.IsZero() {
		out.UserEnteredFormat = formatJSON(c.Format)
	}
	return out
}

func formatJSON(f vfs.CellFormat) *sheets.CellFormat {
	out := &sheets.CellFormat{
		HorizontalAlignment: strings.ToUpper(f.Align),
		VerticalAlignment:   strings.ToUpper(f.VAlign),
		WrapStrategy:        map[string]string{"wrap": "WRAP", "clip": "CLIP", "overflow": "OVERFLOW_CELL"}[f.Wrap],
	}
	if f.Number != "" {
		out.NumberFormat = &sheets.NumberFormat{Pattern: f.Number}
	}
	if f.Bold || f.Italic || f.Strike || f.Underline || f.Color != "" {
		out.TextFormat = &sheets.TextFormat{Bold: f.Bold, Italic: f.Italic, Strikethrough: f.Strike, Underline: f.Underline}
		if f.Color != "" {
			out.TextFormat.ForegroundColorStyle = &sheets.ColorStyle{RGBColor: rgbJSON(f.Color)}
		}
	}
	if f.Fill != "" {
		out.BackgroundColorStyle = &sheets.ColorStyle{RGBColor: rgbJSON(f.Fill)}
	}
	if f.Border != nil {
		out.Borders = borderJSON(f.Border)
	}
	return out
}

// borderStyles maps vfs border styles to Sheets; any other style is SOLID.
var borderStyles = map[string]string{"medium": "SOLID_MEDIUM", "thick": "SOLID_THICK", "dashed": "DASHED", "dotted": "DOTTED", "double": "DOUBLE"}

func borderJSON(b *vfs.CellBorder) *sheets.Borders {
	edge := &sheets.Border{Style: cmp.Or(borderStyles[b.Style], "SOLID")}
	if b.Color != "" {
		edge.ColorStyle = &sheets.ColorStyle{RGBColor: rgbJSON(b.Color)}
	}
	on := func(name string) *sheets.Border {
		if b.Edges == "" || strings.Contains(b.Edges, name) {
			return edge
		}
		return nil
	}
	return &sheets.Borders{Top: on("top"), Bottom: on("bottom"), Left: on("left"), Right: on("right")}
}

func rgbJSON(hex string) *sheets.Color {
	r, g, b, _ := vfs.ParseRGB(hex)
	return &sheets.Color{Red: float64(r) / 255, Green: float64(g) / 255, Blue: float64(b) / 255}
}
