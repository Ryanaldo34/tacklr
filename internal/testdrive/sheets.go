package testdrive

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/api/sheets/v4"

	"github.com/ryanaldo34/tacklr/vfs"
)

// SeedSheet sets spreadsheets.get state for id.
func (d *FX) SeedSheet(id string, snap vfs.SheetsSnapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.books == nil {
		d.books = vfs.NewMemorySheets()
	}
	d.books.Seed(id, snap)
}

// SheetSnap is a copy of the seeded workbook (CAS / out-of-band mutation tests).
func (d *FX) SheetSnap(id string) vfs.SheetsSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.books == nil {
		return vfs.SheetsSnapshot{}
	}
	s, _ := d.books.Get(context.Background(), id)
	return s
}

func (d *FX) serveSheets(w http.ResponseWriter, r *http.Request) {
	id, kind := sheetsID(r.URL.Path)
	if id == "" {
		writeDriveErr(w, http.StatusNotFound, "not found")
		return
	}
	switch {
	case r.Method == http.MethodGet && kind == "":
		d.serveSheetsGet(w, r, id)
	case r.Method == http.MethodPost && kind == "values":
		d.serveSheetsValues(w, r, id)
	case r.Method == http.MethodPost && kind == "batchUpdate":
		d.serveSheetsBatch(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func sheetsID(path string) (id, kind string) {
	i := strings.Index(path, "/spreadsheets/")
	if i < 0 {
		return "", ""
	}
	rest := path[i+len("/spreadsheets/"):]
	switch {
	case strings.Contains(rest, "/values:batchUpdate"):
		id = strings.TrimSuffix(strings.Split(rest, "/")[0], "/")
		return id, "values"
	case strings.Contains(rest, ":batchUpdate"):
		id, _, _ = strings.Cut(rest, ":")
		return strings.Trim(id, "/"), "batchUpdate"
	default:
		return strings.Trim(rest, "/"), ""
	}
}

func (d *FX) serveSheetsGet(w http.ResponseWriter, r *http.Request, id string) {
	if d.books == nil {
		d.books = vfs.NewMemorySheets()
	}
	snap, err := d.books.Get(r.Context(), id)
	if err != nil {
		writeDriveErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeDriveJSON(w, http.StatusOK, spreadsheetJSON(snap))
}

func (d *FX) serveSheetsValues(w http.ResponseWriter, r *http.Request, id string) {
	raw, _ := io.ReadAll(r.Body)
	var body sheets.BatchUpdateValuesRequest
	if err := json.Unmarshal(raw, &body); err != nil {
		writeDriveErr(w, http.StatusBadRequest, "invalid write")
		return
	}
	req := vfs.SheetsValuesBatch{}
	for _, vr := range body.Data {
		if vr == nil {
			continue
		}
		row := vfs.SheetsValueRange{Range: vr.Range}
		for _, r := range vr.Values {
			var cells []string
			for _, c := range r {
				cells = append(cells, fmtCell(c))
			}
			row.Values = append(row.Values, cells)
		}
		req.Data = append(req.Data, row)
	}
	if d.books == nil {
		d.books = vfs.NewMemorySheets()
	}
	if err := d.books.BatchUpdateValues(r.Context(), id, req); err != nil {
		writeDriveErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeDriveJSON(w, http.StatusOK, map[string]any{})
}

func (d *FX) serveSheetsBatch(w http.ResponseWriter, r *http.Request, id string) {
	raw, _ := io.ReadAll(r.Body)
	var body sheets.BatchUpdateSpreadsheetRequest
	if err := json.Unmarshal(raw, &body); err != nil {
		writeDriveErr(w, http.StatusBadRequest, "invalid write")
		return
	}
	req := vfs.SheetsBatch{}
	for _, call := range body.Requests {
		if call == nil || call.RepeatCell == nil || call.RepeatCell.Range == nil {
			continue
		}
		rng := call.RepeatCell.Range
		fmt := vfs.CellFormat{}
		if call.RepeatCell.Cell != nil && call.RepeatCell.Cell.UserEnteredFormat != nil {
			fmt = formatFromSheets(call.RepeatCell.Cell.UserEnteredFormat)
		}
		req.Requests = append(req.Requests, vfs.SheetsRepeatCell{
			SheetID:  rng.SheetId,
			StartRow: int(rng.StartRowIndex), StartCol: int(rng.StartColumnIndex),
			EndRow: int(rng.EndRowIndex), EndCol: int(rng.EndColumnIndex),
			Format: fmt,
		})
	}
	if d.books == nil {
		d.books = vfs.NewMemorySheets()
	}
	if err := d.books.BatchUpdate(r.Context(), id, req); err != nil {
		writeDriveErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeDriveJSON(w, http.StatusOK, map[string]any{})
}

func spreadsheetJSON(snap vfs.SheetsSnapshot) map[string]any {
	var sheetsJSON []any
	for _, sh := range snap.Sheets {
		sid, _ := strconv.ParseInt(sh.ID, 10, 64)
		var merges []any
		for _, m := range sh.MergeBounds() {
			merges = append(merges, map[string]any{
				"startRowIndex": m[0], "startColumnIndex": m[1],
				"endRowIndex": m[2], "endColumnIndex": m[3],
			})
		}
		var rows []any
		for _, row := range sh.Cells {
			var vals []any
			for _, c := range row {
				vals = append(vals, cellJSON(c))
			}
			rows = append(rows, map[string]any{"values": vals})
		}
		item := map[string]any{
			"properties": map[string]any{"sheetId": sid, "title": sh.Title, "index": sh.Index},
			"data":       []any{map[string]any{"startRow": 0, "startColumn": 0, "rowData": rows}},
		}
		if len(merges) > 0 {
			item["merges"] = merges
		}
		sheetsJSON = append(sheetsJSON, item)
	}
	var named []any
	for _, nr := range snap.Named {
		item := map[string]any{"name": nr.Name}
		if nr.A1 != "" {
			sid, _ := strconv.ParseInt(nr.SheetID, 10, 64)
			if r1, c1, r2, c2, err := vfs.ParseA1(nr.A1); err == nil {
				item["range"] = map[string]any{
					"sheetId": sid, "startRowIndex": r1 - 1, "startColumnIndex": c1 - 1,
					"endRowIndex": r2, "endColumnIndex": c2,
				}
			}
		}
		named = append(named, item)
	}
	out := map[string]any{"spreadsheetId": snap.SpreadsheetID, "sheets": sheetsJSON}
	if len(named) > 0 {
		out["namedRanges"] = named
	}
	return out
}

func cellJSON(c vfs.Cell) map[string]any {
	out := map[string]any{}
	if c.Value != "" {
		out["formattedValue"] = c.Value
	}
	if c.Input != "" {
		if strings.HasPrefix(c.Input, "=") {
			out["userEnteredValue"] = map[string]any{"formulaValue": c.Input}
		} else {
			out["userEnteredValue"] = map[string]any{"stringValue": c.Input}
		}
	}
	if !c.Format.IsZero() {
		out["userEnteredFormat"] = formatJSON(c.Format)
	}
	return out
}

func formatJSON(f vfs.CellFormat) map[string]any {
	out := map[string]any{}
	if f.Number != "" {
		out["numberFormat"] = map[string]any{"pattern": f.Number}
	}
	tf := map[string]any{}
	if f.Bold {
		tf["bold"] = true
	}
	if f.Italic {
		tf["italic"] = true
	}
	if f.Strike {
		tf["strikethrough"] = true
	}
	if f.Underline {
		tf["underline"] = true
	}
	if f.Color != "" {
		tf["foregroundColorStyle"] = map[string]any{"rgbColor": rgbJSON(f.Color)}
	}
	if len(tf) > 0 {
		out["textFormat"] = tf
	}
	if f.Fill != "" {
		out["backgroundColorStyle"] = map[string]any{"rgbColor": rgbJSON(f.Fill)}
	}
	if f.Align != "" {
		out["horizontalAlignment"] = strings.ToUpper(f.Align)
	}
	if f.VAlign != "" {
		out["verticalAlignment"] = strings.ToUpper(f.VAlign)
	}
	switch f.Wrap {
	case "wrap":
		out["wrapStrategy"] = "WRAP"
	case "clip":
		out["wrapStrategy"] = "CLIP"
	case "overflow":
		out["wrapStrategy"] = "OVERFLOW_CELL"
	}
	if f.Border != nil {
		out["borders"] = borderJSON(f.Border)
	}
	return out
}

func borderJSON(b *vfs.CellBorder) map[string]any {
	style := "SOLID"
	switch b.Style {
	case "medium":
		style = "SOLID_MEDIUM"
	case "thick":
		style = "SOLID_THICK"
	case "dashed":
		style = "DASHED"
	case "dotted":
		style = "DOTTED"
	case "double":
		style = "DOUBLE"
	}
	edge := map[string]any{"style": style}
	if b.Color != "" {
		edge["colorStyle"] = map[string]any{"rgbColor": rgbJSON(b.Color)}
	}
	out := map[string]any{}
	want := b.Edges
	for _, name := range []string{"top", "bottom", "left", "right"} {
		if want == "" || strings.Contains(want, name) {
			out[name] = edge
		}
	}
	return out
}

func rgbJSON(hex string) map[string]any {
	r, g, b, ok := vfs.ParseRGB(hex)
	if !ok {
		return map[string]any{}
	}
	return map[string]any{"red": float64(r) / 255, "green": float64(g) / 255, "blue": float64(b) / 255}
}

func formatFromSheets(f *sheets.CellFormat) vfs.CellFormat {
	out := vfs.CellFormat{}
	if f == nil {
		return out
	}
	if f.NumberFormat != nil {
		out.Number = f.NumberFormat.Pattern
	}
	if tf := f.TextFormat; tf != nil {
		out.Bold, out.Italic, out.Strike, out.Underline = tf.Bold, tf.Italic, tf.Strikethrough, tf.Underline
		if tf.ForegroundColorStyle != nil && tf.ForegroundColorStyle.RgbColor != nil {
			c := tf.ForegroundColorStyle.RgbColor
			out.Color = vfs.FormatRGB(uint8(c.Red*255+0.5), uint8(c.Green*255+0.5), uint8(c.Blue*255+0.5))
		}
	}
	if f.BackgroundColorStyle != nil && f.BackgroundColorStyle.RgbColor != nil {
		c := f.BackgroundColorStyle.RgbColor
		out.Fill = vfs.FormatRGB(uint8(c.Red*255+0.5), uint8(c.Green*255+0.5), uint8(c.Blue*255+0.5))
	}
	out.Align = strings.ToLower(f.HorizontalAlignment)
	out.VAlign = strings.ToLower(f.VerticalAlignment)
	switch f.WrapStrategy {
	case "WRAP":
		out.Wrap = "wrap"
	case "CLIP":
		out.Wrap = "clip"
	case "OVERFLOW_CELL":
		out.Wrap = "overflow"
	}
	if f.Borders != nil {
		out.Border = borderFromReq(f.Borders)
	}
	return out
}

func borderFromReq(b *sheets.Borders) *vfs.CellBorder {
	if b == nil {
		return nil
	}
	type edge struct {
		name string
		b    *sheets.Border
	}
	style, color := "", ""
	var names []string
	for _, e := range []edge{{"top", b.Top}, {"bottom", b.Bottom}, {"left", b.Left}, {"right", b.Right}} {
		if e.b == nil || e.b.Style == "" || e.b.Style == "NONE" {
			continue
		}
		st := "thin"
		switch e.b.Style {
		case "SOLID_MEDIUM":
			st = "medium"
		case "SOLID_THICK":
			st = "thick"
		case "DASHED":
			st = "dashed"
		case "DOTTED":
			st = "dotted"
		case "DOUBLE":
			st = "double"
		}
		col := ""
		if e.b.ColorStyle != nil && e.b.ColorStyle.RgbColor != nil {
			c := e.b.ColorStyle.RgbColor
			col = vfs.FormatRGB(uint8(c.Red*255+0.5), uint8(c.Green*255+0.5), uint8(c.Blue*255+0.5))
		}
		if style == "" {
			style, color = st, col
		}
		names = append(names, e.name)
	}
	if style == "" {
		return nil
	}
	out := &vfs.CellBorder{Style: style, Color: color}
	if len(names) < 4 {
		out.Edges = strings.Join(names, ",")
	}
	return out
}

func fmtCell(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	default:
		b, _ := json.Marshal(v)
		return strings.Trim(string(b), `"`)
	}
}
