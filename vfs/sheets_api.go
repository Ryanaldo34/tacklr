package vfs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// SheetsAPI is the Sheets subset used by the provider. MemorySheets is the
// in-memory implementation; GoogleSheets talks to the service.
type SheetsAPI interface {
	Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error)
	BatchUpdateValues(ctx context.Context, spreadsheetID string, req SheetsValuesBatch) error
	BatchUpdate(ctx context.Context, spreadsheetID string, req SheetsBatch) error
}

// SheetsBatch is one spreadsheets.batchUpdate (repeatCell userEnteredFormat).
type SheetsBatch struct {
	Requests []SheetsRepeatCell
}

// SheetsRepeatCell is one repeatCell of a format bag.
type SheetsRepeatCell struct {
	SheetID            int64
	StartRow, StartCol int // 0-based inclusive
	EndRow, EndCol     int // 0-based exclusive
	Format             CellFormat
}

// SheetsSnapshot is the checkout used to build TabularDocument.
type SheetsSnapshot struct {
	SpreadsheetID string
	RevisionID    string
	Sheets        []Sheet
	Named         []NamedRange
}

// SheetsValuesBatch is one spreadsheets.values.batchUpdate call.
type SheetsValuesBatch struct {
	Data []SheetsValueRange
}

// SheetsValueRange is one A1 range of string cells (USER_ENTERED).
type SheetsValueRange struct {
	Range  string
	Values [][]string
}

type googleSheets struct {
	service *sheets.Service
}

// NewGoogleSheets builds a SheetsAPI from a user token holder.
func NewGoogleSheets(ctx context.Context, holder *TokenHolder) (SheetsAPI, error) {
	return newGoogleSheets(ctx, holder)
}

func newGoogleSheets(ctx context.Context, holder *TokenHolder) (*googleSheets, error) {
	if holder == nil {
		return nil, fmt.Errorf("vfs: sheets token required")
	}
	svc, err := sheets.NewService(ctx, option.WithTokenSource(holder))
	if err != nil {
		return nil, fmt.Errorf("vfs: sheets service: %w", err)
	}
	return &googleSheets{service: svc}, nil
}

func (g googleSheets) Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error) {
	meta, err := g.service.Spreadsheets.Get(spreadsheetID).
		IncludeGridData(true).
		Fields("spreadsheetId,sheets.properties,sheets.merges,sheets.data,namedRanges").
		Context(ctx).
		Do()
	if err != nil {
		return SheetsSnapshot{}, mapDocsError(err)
	}
	snap := SheetsSnapshot{SpreadsheetID: meta.SpreadsheetId}
	for i, sh := range meta.Sheets {
		if sh == nil || sh.Properties == nil {
			continue
		}
		title := sh.Properties.Title
		id := strconv.FormatInt(sh.Properties.SheetId, 10)
		item := Sheet{
			ID:    id,
			Title: title,
			Index: int(sh.Properties.Index),
		}
		if item.Index == 0 {
			item.Index = i
		}
		for _, m := range sh.Merges {
			if m == nil {
				continue
			}
			item.merges = append(item.merges, gridMerge{
				r1: int(m.StartRowIndex), c1: int(m.StartColumnIndex),
				r2: int(m.EndRowIndex), c2: int(m.EndColumnIndex),
			})
		}
		if err := fillSheetGrid(&item, sh.Data); err != nil {
			return SheetsSnapshot{}, err
		}
		snap.Sheets = append(snap.Sheets, item)
	}
	cells := 0
	for i := range snap.Sheets {
		cells += snap.Sheets[i].Rows * snap.Sheets[i].Cols
		if cells > MaxSheetCells {
			return SheetsSnapshot{}, fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
		}
	}
	for _, nr := range meta.NamedRanges {
		if nr == nil {
			continue
		}
		n := NamedRange{Name: nr.Name}
		if nr.Range != nil {
			n.SheetID = strconv.FormatInt(nr.Range.SheetId, 10)
			n.A1 = gridRangeA1(nr.Range)
		}
		snap.Named = append(snap.Named, n)
	}
	return snap, nil
}

func (g googleSheets) BatchUpdateValues(ctx context.Context, spreadsheetID string, req SheetsValuesBatch) error {
	data := make([]*sheets.ValueRange, 0, len(req.Data))
	for _, r := range req.Data {
		vals := make([][]any, len(r.Values))
		for i, row := range r.Values {
			vals[i] = make([]any, len(row))
			for j, c := range row {
				vals[i][j] = c
			}
		}
		data = append(data, &sheets.ValueRange{Range: r.Range, Values: vals})
	}
	_, err := g.service.Spreadsheets.Values.BatchUpdate(spreadsheetID, &sheets.BatchUpdateValuesRequest{
		ValueInputOption: "USER_ENTERED",
		Data:             data,
	}).Context(ctx).Do()
	return mapDocsError(err)
}

func (g googleSheets) BatchUpdate(ctx context.Context, spreadsheetID string, req SheetsBatch) error {
	if len(req.Requests) == 0 {
		return nil
	}
	calls := make([]*sheets.Request, 0, len(req.Requests))
	for _, r := range req.Requests {
		cell, fields := repeatCellPayload(r.Format)
		if fields == "" {
			continue
		}
		calls = append(calls, &sheets.Request{RepeatCell: &sheets.RepeatCellRequest{
			Range: &sheets.GridRange{
				SheetId:          r.SheetID,
				StartRowIndex:    int64(r.StartRow),
				StartColumnIndex: int64(r.StartCol),
				EndRowIndex:      int64(r.EndRow),
				EndColumnIndex:   int64(r.EndCol),
			},
			Cell:   cell,
			Fields: fields,
		}})
	}
	if len(calls) == 0 {
		return nil
	}
	_, err := g.service.Spreadsheets.BatchUpdate(spreadsheetID, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: calls,
	}).Context(ctx).Do()
	return mapDocsError(err)
}

func fillSheetGrid(sh *Sheet, data []*sheets.GridData) error {
	if len(data) == 0 {
		return nil
	}
	rows, cols := 0, 0
	for _, gd := range data {
		if gd == nil {
			continue
		}
		sr, sc, err := gridOrigin(gd.StartRow, gd.StartColumn)
		if err != nil {
			return err
		}
		if sr+len(gd.RowData) > rows {
			rows = sr + len(gd.RowData)
		}
		for _, rd := range gd.RowData {
			n := sc
			if rd != nil {
				n += len(rd.Values)
			}
			if n > cols {
				cols = n
			}
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
	for _, gd := range data {
		if gd == nil {
			continue
		}
		sr, sc, err := gridOrigin(gd.StartRow, gd.StartColumn)
		if err != nil {
			return err
		}
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
	c := Cell{Value: cd.FormattedValue}
	if cd.UserEnteredValue != nil {
		c.Input = extendedInput(cd.UserEnteredValue)
	}
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
		return cellString(*v.NumberValue)
	}
	if v.BoolValue != nil {
		return cellString(*v.BoolValue)
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

func sheetsBorderStyle(s string) string {
	switch s {
	case "SOLID":
		return "thin"
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
	case "NONE":
		return "none"
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

func irBorderStyle(s string) string {
	switch normalizeBorderStyle(s) {
	case "medium":
		return "SOLID_MEDIUM"
	case "thick":
		return "SOLID_THICK"
	case "dashed":
		return "DASHED"
	case "dotted":
		return "DOTTED"
	case "double":
		return "DOUBLE"
	case "none":
		return "NONE"
	default:
		return "SOLID"
	}
}

func colorHex(c *sheets.Color, style *sheets.ColorStyle) string {
	if style != nil && style.RgbColor != nil {
		c = style.RgbColor
	}
	if c == nil {
		return ""
	}
	return FormatRGB(round255(c.Red), round255(c.Green), round255(c.Blue))
}

func round255(v float64) uint8 {
	n := v*255 + 0.5
	if n < 0 {
		return 0
	}
	if n > 255 {
		return 255
	}
	return uint8(uint(n) & 0xff)
}

func hexColor(s string) *sheets.Color {
	r, g, b, ok := ParseRGB(s)
	if !ok {
		return nil
	}
	return &sheets.Color{
		Red:   float64(r) / 255,
		Green: float64(g) / 255,
		Blue:  float64(b) / 255,
	}
}

func rgbStyle(c *sheets.Color) *sheets.ColorStyle {
	if c == nil {
		return nil
	}
	return &sheets.ColorStyle{RgbColor: c}
}

func repeatCellPayload(f CellFormat) (*sheets.CellData, string) {
	if f.IsZero() {
		return nil, ""
	}
	sf := &sheets.CellFormat{}
	var fields []string
	if f.has(fmtNumber) || f.Number != "" {
		sf.NumberFormat = &sheets.NumberFormat{Type: numberFormatType(f.Number), Pattern: f.Number}
		fields = append(fields, "userEnteredFormat.numberFormat")
	}
	if f.has(fmtBold) || f.has(fmtItalic) || f.has(fmtStrike) || f.has(fmtUnderline) ||
		f.Bold || f.Italic || f.Strike || f.Underline || f.has(fmtColor) || f.Color != "" {
		tf := &sheets.TextFormat{
			Bold: f.Bold, Italic: f.Italic, Strikethrough: f.Strike, Underline: f.Underline,
		}
		if c := hexColor(f.Color); c != nil {
			tf.ForegroundColorStyle = rgbStyle(c)
		}
		sf.TextFormat = tf
		if f.has(fmtBold) || f.Bold {
			fields = append(fields, "userEnteredFormat.textFormat.bold")
		}
		if f.has(fmtItalic) || f.Italic {
			fields = append(fields, "userEnteredFormat.textFormat.italic")
		}
		if f.has(fmtStrike) || f.Strike {
			fields = append(fields, "userEnteredFormat.textFormat.strikethrough")
		}
		if f.has(fmtUnderline) || f.Underline {
			fields = append(fields, "userEnteredFormat.textFormat.underline")
		}
		if f.has(fmtColor) || f.Color != "" {
			fields = append(fields, "userEnteredFormat.textFormat.foregroundColorStyle")
		}
	}
	if f.has(fmtFill) || f.Fill != "" {
		if c := hexColor(f.Fill); c != nil {
			sf.BackgroundColorStyle = rgbStyle(c)
			fields = append(fields, "userEnteredFormat.backgroundColorStyle")
		} else if f.has(fmtFill) {
			sf.BackgroundColorStyle = &sheets.ColorStyle{}
			fields = append(fields, "userEnteredFormat.backgroundColorStyle")
		}
	}
	if f.has(fmtAlign) || f.Align != "" {
		if a := normalizeAlign(f.Align); a != "" {
			sf.HorizontalAlignment = strings.ToUpper(a)
		} else {
			sf.HorizontalAlignment = "LEFT"
		}
		fields = append(fields, "userEnteredFormat.horizontalAlignment")
	}
	if f.has(fmtVAlign) || f.VAlign != "" {
		if a := normalizeVAlign(f.VAlign); a != "" {
			sf.VerticalAlignment = strings.ToUpper(a)
		} else {
			sf.VerticalAlignment = "BOTTOM"
		}
		fields = append(fields, "userEnteredFormat.verticalAlignment")
	}
	if f.has(fmtWrap) || f.Wrap != "" {
		switch f.Wrap {
		case "wrap":
			sf.WrapStrategy = "WRAP"
		case "clip":
			sf.WrapStrategy = "CLIP"
		default:
			sf.WrapStrategy = "OVERFLOW_CELL"
		}
		fields = append(fields, "userEnteredFormat.wrapStrategy")
	}
	if f.has(fmtBorder) || (f.Border != nil && !f.Border.zero()) {
		sf.Borders = irBorders(f.Border)
		fields = append(fields, "userEnteredFormat.borders")
	}
	if len(fields) == 0 {
		return nil, ""
	}
	return &sheets.CellData{UserEnteredFormat: sf}, strings.Join(fields, ",")
}

func irBorders(b *CellBorder) *sheets.Borders {
	if b == nil || b.zero() {
		none := &sheets.Border{Style: "NONE"}
		return &sheets.Borders{Top: none, Bottom: none, Left: none, Right: none}
	}
	style := irBorderStyle(b.Style)
	col := hexColor(b.Color)
	edge := func() *sheets.Border {
		return &sheets.Border{Style: style, ColorStyle: rgbStyle(col)}
	}
	out := &sheets.Borders{}
	if borderEdge(b.Edges, "top") {
		out.Top = edge()
	}
	if borderEdge(b.Edges, "bottom") {
		out.Bottom = edge()
	}
	if borderEdge(b.Edges, "left") {
		out.Left = edge()
	}
	if borderEdge(b.Edges, "right") {
		out.Right = edge()
	}
	return out
}

func borderEdge(edges, name string) bool {
	return edges == "" || strings.Contains(edges, name)
}

func numberFormatType(pattern string) string {
	p := strings.ToLower(pattern)
	switch {
	case strings.ContainsAny(p, "$€¥"):
		return "CURRENCY"
	case strings.Contains(p, "%"):
		return "PERCENT"
	case strings.Contains(p, "e+") || strings.Contains(p, "e-"):
		return "SCIENTIFIC"
	}
	date, time := dateTimeTokens(p)
	switch {
	case date && time:
		return "DATE_TIME"
	case time:
		return "TIME"
	case date:
		return "DATE"
	default:
		return "NUMBER"
	}
}

func dateTimeTokens(p string) (date, time bool) {
	if strings.Contains(p, "am/pm") || strings.Contains(p, "a/p") {
		time = true
	}
	for i := 0; i < len(p); i++ {
		switch p[i] {
		case 'h', 's':
			time = true
		case 'y', 'd':
			date = true
		case 'm':
			if minuteContext(p, i) {
				time = true
			} else {
				date = true
			}
		}
	}
	return date, time
}

func minuteContext(p string, i int) bool {
	for j := i - 1; j >= 0; j-- {
		if p[j] == ':' || p[j] == ' ' || p[j] == 'm' {
			continue
		}
		return p[j] == 'h' || p[j] == 's'
	}
	for j := i + 1; j < len(p); j++ {
		if p[j] == ':' || p[j] == ' ' || p[j] == 'm' {
			continue
		}
		return p[j] == 'h' || p[j] == 's'
	}
	return false
}

func cellString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprint(x)
	}
}

func gridRangeA1(r *sheets.GridRange) string {
	if r == nil {
		return ""
	}
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

func snapshotToTabular(path string, snap SheetsSnapshot) (*IR, error) {
	td, err := NewTabularDocument(path, mimeGoogleSpreadsheet, snap.Sheets, snap.Named)
	if err != nil {
		return nil, err
	}
	attachPersistHint(td, persistHint{fileID: snap.SpreadsheetID})
	return td, nil
}

func tabularOverlayBatch(td *gridBody) (SheetsValuesBatch, error) {
	var data []SheetsValueRange
	for _, sh := range td.sheets {
		rows := sh.Rows
		cols := sh.Cols
		if rows == 0 || cols == 0 {
			continue
		}
		grid := make([][]string, rows)
		for r := 0; r < rows; r++ {
			grid[r] = make([]string, cols)
			if r < len(sh.Cells) {
				for c := 0; c < cols && c < len(sh.Cells[r]); c++ {
					if sh.mergeSlave(r, c) {
						continue
					}
					grid[r][c] = sh.Cells[r][c].Input
				}
			}
		}
		title := sh.Title
		if title == "" {
			title = "Sheet1"
		}
		data = append(data, SheetsValueRange{
			Range:  sheetA1(title, 1, 1, rows, cols),
			Values: grid,
		})
	}
	return SheetsValuesBatch{Data: data}, nil
}

func tabularFormatBatch(td *gridBody) SheetsBatch {
	var reqs []SheetsRepeatCell
	for _, sh := range td.sheets {
		id, err := strconv.ParseInt(sh.ID, 10, 64)
		if err != nil {
			id = 0
		}
		rows, cols := sh.Rows, sh.Cols
		for _, rec := range formatRects(sh, rows, cols) {
			reqs = append(reqs, SheetsRepeatCell{
				SheetID:  id,
				StartRow: rec.r1, StartCol: rec.c1, EndRow: rec.r2, EndCol: rec.c2,
				Format: rec.format,
			})
		}
	}
	return SheetsBatch{Requests: reqs}
}
