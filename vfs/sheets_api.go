package vfs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// SheetsAPI is the Sheets subset used by the provider. Tests inject a fake.
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

func (g googleSheets) require() error {
	if g.service == nil {
		return fmt.Errorf("vfs: sheets service required")
	}
	return nil
}

func (g googleSheets) Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error) {
	if err := g.require(); err != nil {
		return SheetsSnapshot{}, err
	}
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
	if err := g.require(); err != nil {
		return err
	}
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
	if err := g.require(); err != nil {
		return err
	}
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
	}
	if tf := f.TextFormat; tf != nil {
		out.Bold = tf.Bold
		out.Italic = tf.Italic
		out.Strike = tf.Strikethrough
		out.Color = colorHex(tf.ForegroundColor, tf.ForegroundColorStyle)
	}
	out.Fill = colorHex(f.BackgroundColor, f.BackgroundColorStyle)
	out.Align = normalizeAlign(f.HorizontalAlignment)
	if b := borderFromSheets(f.Borders); b != nil {
		out.Border = b
	}
	return out
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
	r, g, b := round255(c.Red), round255(c.Green), round255(c.Blue)
	var buf [7]byte
	buf[0] = '#'
	hexPut(buf[1:3], r)
	hexPut(buf[3:5], g)
	hexPut(buf[5:7], b)
	return string(buf[:])
}

func hexPut(dst []byte, n int) {
	const digits = "0123456789abcdef"
	dst[0] = digits[n>>4]
	dst[1] = digits[n&15]
}

func round255(v float64) int {
	n := int(v*255 + 0.5)
	if n < 0 {
		return 0
	}
	if n > 255 {
		return 255
	}
	return n
}

func hexColor(s string) *sheets.Color {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "#")
	if len(s) == 8 {
		s = s[2:]
	}
	if len(s) != 6 {
		return nil
	}
	n, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return nil
	}
	return &sheets.Color{
		Red:   float64(n>>16) / 255,
		Green: float64((n>>8)&0xff) / 255,
		Blue:  float64(n&0xff) / 255,
	}
}

func repeatCellPayload(f CellFormat) (*sheets.CellData, string) {
	if f.IsZero() {
		return nil, ""
	}
	sf := &sheets.CellFormat{}
	var fields []string
	if f.Number != "" {
		sf.NumberFormat = &sheets.NumberFormat{Type: numberFormatType(f.Number), Pattern: f.Number}
		fields = append(fields, "userEnteredFormat.numberFormat")
	}
	if f.Bold || f.Italic || f.Strike || f.Color != "" {
		tf := &sheets.TextFormat{Bold: f.Bold, Italic: f.Italic, Strikethrough: f.Strike}
		if c := hexColor(f.Color); c != nil {
			tf.ForegroundColor = c
		}
		sf.TextFormat = tf
		if f.Bold {
			fields = append(fields, "userEnteredFormat.textFormat.bold")
		}
		if f.Italic {
			fields = append(fields, "userEnteredFormat.textFormat.italic")
		}
		if f.Strike {
			fields = append(fields, "userEnteredFormat.textFormat.strikethrough")
		}
		if f.Color != "" {
			fields = append(fields, "userEnteredFormat.textFormat.foregroundColor")
		}
	}
	if f.Fill != "" {
		if c := hexColor(f.Fill); c != nil {
			sf.BackgroundColor = c
			fields = append(fields, "userEnteredFormat.backgroundColor")
		}
	}
	if a := normalizeAlign(f.Align); a != "" {
		sf.HorizontalAlignment = strings.ToUpper(a)
		fields = append(fields, "userEnteredFormat.horizontalAlignment")
	}
	if f.Border != nil && !f.Border.zero() {
		sf.Borders = irBorders(f.Border)
		fields = append(fields, "userEnteredFormat.borders")
	}
	if len(fields) == 0 {
		return nil, ""
	}
	return &sheets.CellData{UserEnteredFormat: sf}, strings.Join(fields, ",")
}

func irBorders(b *CellBorder) *sheets.Borders {
	style := irBorderStyle(b.Style)
	col := hexColor(b.Color)
	edge := func() *sheets.Border {
		return &sheets.Border{Style: style, Color: col}
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
	case strings.Contains(p, "$") || strings.Contains(p, "€") || strings.Contains(p, "¥"):
		return "CURRENCY"
	case strings.Contains(p, "%"):
		return "PERCENT"
	case strings.ContainsAny(p, "ymd"):
		return "DATE"
	default:
		return "NUMBER"
	}
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

func snapshotToTabular(path string, snap SheetsSnapshot) (*TabularDocument, error) {
	td, err := NewTabularDocument(path, mimeGoogleSpreadsheet, snap.Sheets, snap.Named)
	if err != nil {
		return nil, err
	}
	attachPersistHint(td, persistHint{fileID: snap.SpreadsheetID})
	return td, nil
}

func tabularOverlayBatch(td *TabularDocument) (SheetsValuesBatch, error) {
	var data []SheetsValueRange
	for _, sh := range td.sheets {
		rows := sh.Rows
		cols := sh.Cols
		if sh.persistRows > rows {
			rows = sh.persistRows
		}
		if sh.persistCols > cols {
			cols = sh.persistCols
		}
		if rows == 0 || cols == 0 {
			continue
		}
		if len(sh.merges) > 0 {
			for r := 0; r < rows; r++ {
				for c := 0; c < cols; c++ {
					if sh.mergeHit(r, c) {
						return SheetsValuesBatch{}, fmt.Errorf("%w: write into a merge", ErrNotSupported)
					}
				}
			}
		}
		grid := make([][]string, rows)
		for r := 0; r < rows; r++ {
			grid[r] = make([]string, cols)
			if r < len(sh.Cells) {
				for c := 0; c < cols && c < len(sh.Cells[r]); c++ {
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

func tabularFormatBatch(td *TabularDocument) SheetsBatch {
	var reqs []SheetsRepeatCell
	for _, sh := range td.sheets {
		id, err := strconv.ParseInt(sh.ID, 10, 64)
		if err != nil {
			id = 0
		}
		for r := 0; r < sh.Rows; r++ {
			c := 0
			for c < sh.Cols {
				cell := sh.Cells[r][c]
				if cell.Format.IsZero() {
					c++
					continue
				}
				end := c + 1
				for end < sh.Cols && sh.Cells[r][end].Format.equal(cell.Format) {
					end++
				}
				reqs = append(reqs, SheetsRepeatCell{
					SheetID:  id,
					StartRow: r, StartCol: c, EndRow: r + 1, EndCol: end,
					Format: cell.Format,
				})
				c = end
			}
		}
	}
	return SheetsBatch{Requests: reqs}
}
