package testdrive

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/google/sheets"

	"github.com/ryanaldo34/tacklr/vfs"
)

// SeedSheet sets spreadsheets.get state for id.
func (d *FX) SeedSheet(id string, snap vfs.SheetsSnapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.books.Seed(id, snap)
}

// SheetSnap is a copy of the seeded workbook (CAS / out-of-band mutation tests).
func (d *FX) SheetSnap(id string) vfs.SheetsSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, _ := d.books.Get(context.Background(), id) // MemorySheets fails only on a done context
	return s
}

func (d *FX) serveSheetsGet(w http.ResponseWriter, _ *http.Request, id string) {
	snap, _ := d.books.Get(context.Background(), id)
	writeDriveJSON(w, http.StatusOK, spreadsheetJSON(snap))
}

func (d *FX) serveSheetsValues(w http.ResponseWriter, r *http.Request, id string) {
	var body sheets.BatchUpdateValuesRequest
	_ = json.NewDecoder(r.Body).Decode(&body)
	req := vfs.SheetsValuesBatch{}
	for _, vr := range body.Data {
		req.Data = append(req.Data, vfs.SheetsValueRange{Range: vr.Range, Values: vr.Values})
	}
	_ = d.books.BatchUpdateValues(context.Background(), id, req)
	writeDriveJSON(w, http.StatusOK, map[string]any{})
}

func (d *FX) serveSheetsBatch(w http.ResponseWriter, r *http.Request, id string) {
	var body sheets.BatchUpdateSpreadsheetRequest
	_ = json.NewDecoder(r.Body).Decode(&body)
	req := vfs.SheetsBatch{}
	for _, call := range body.Requests {
		rng := call.RepeatCell.Range
		req.Requests = append(req.Requests, vfs.SheetsRepeatCell{
			SheetID:  rng.SheetID,
			StartRow: int(rng.StartRowIndex), StartCol: int(rng.StartColumnIndex),
			EndRow: int(rng.EndRowIndex), EndCol: int(rng.EndColumnIndex),
			Format: formatFromSheets(call.RepeatCell.Cell.UserEnteredFormat),
		})
	}
	_ = d.books.BatchUpdate(context.Background(), id, req)
	writeDriveJSON(w, http.StatusOK, map[string]any{})
}

// spreadsheetJSON is snap as spreadsheets.get answers it with grid data.
func spreadsheetJSON(snap vfs.SheetsSnapshot) sheets.Spreadsheet {
	out := sheets.Spreadsheet{SpreadsheetID: snap.SpreadsheetID}
	for _, sh := range snap.Sheets {
		sid, _ := strconv.ParseInt(sh.ID, 10, 64)
		item := &sheets.Sheet{Properties: &sheets.SheetProperties{SheetID: sid, Title: sh.Title, Index: int64(sh.Index)}}
		for _, m := range sh.MergeBounds() {
			item.Merges = append(item.Merges, &sheets.GridRange{
				StartRowIndex: int64(m[0]), StartColumnIndex: int64(m[1]), EndRowIndex: int64(m[2]), EndColumnIndex: int64(m[3]),
			})
		}
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

// formatFromSheets is the vfs format of a repeatCell format.
func formatFromSheets(f *sheets.CellFormat) vfs.CellFormat {
	out := vfs.CellFormat{
		Fill:   hexOf(f.BackgroundColorStyle),
		Align:  strings.ToLower(f.HorizontalAlignment),
		VAlign: strings.ToLower(f.VerticalAlignment),
		Wrap:   map[string]string{"WRAP": "wrap", "CLIP": "clip", "OVERFLOW_CELL": "overflow"}[f.WrapStrategy],
	}
	if f.NumberFormat != nil {
		out.Number = f.NumberFormat.Pattern
	}
	if tf := f.TextFormat; tf != nil {
		out.Bold, out.Italic, out.Strike, out.Underline = tf.Bold, tf.Italic, tf.Strikethrough, tf.Underline
		out.Color = hexOf(tf.ForegroundColorStyle)
	}
	if f.Borders != nil {
		out.Border = borderFromReq(f.Borders)
	}
	return out
}

// hexOf is the #rrggbb of a color style, or "" for none.
func hexOf(c *sheets.ColorStyle) string {
	if c == nil || c.RGBColor == nil {
		return ""
	}
	return vfs.FormatRGB(uint8(c.RGBColor.Red*255+0.5), uint8(c.RGBColor.Green*255+0.5), uint8(c.RGBColor.Blue*255+0.5))
}

// borderFromReq is the vfs border of the edges that are not NONE: the
// style and color of the first, on each such edge.
func borderFromReq(b *sheets.Borders) *vfs.CellBorder {
	type edge struct {
		name string
		b    *sheets.Border
	}
	var out *vfs.CellBorder
	var names []string
	for _, e := range []edge{{"top", b.Top}, {"bottom", b.Bottom}, {"left", b.Left}, {"right", b.Right}} {
		if e.b == nil || e.b.Style == "" || e.b.Style == "NONE" {
			continue
		}
		if out == nil {
			out = &vfs.CellBorder{Style: cmp.Or(vfsBorderStyles[e.b.Style], "thin"), Color: hexOf(e.b.ColorStyle)}
		}
		names = append(names, e.name)
	}
	if out != nil && len(names) < 4 {
		out.Edges = strings.Join(names, ",")
	}
	return out
}

// vfsBorderStyles maps Sheets border styles to vfs; any other style is thin.
var vfsBorderStyles = map[string]string{"SOLID_MEDIUM": "medium", "SOLID_THICK": "thick", "DASHED": "dashed", "DOTTED": "dotted", "DOUBLE": "double"}
