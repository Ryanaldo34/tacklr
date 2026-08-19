package vfs

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// SheetsAPI is the Sheets subset used by the provider. Tests inject a fake.
type SheetsAPI interface {
	Get(ctx context.Context, spreadsheetID string) (SheetsSnapshot, error)
	BatchUpdateValues(ctx context.Context, spreadsheetID string, req SheetsValuesBatch) error
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
		Fields("spreadsheetId,sheets.properties,sheets.merges,namedRanges").
		Context(ctx).
		Do()
	if err != nil {
		return SheetsSnapshot{}, mapDocsError(err)
	}
	snap := SheetsSnapshot{SpreadsheetID: meta.SpreadsheetId}
	var ranges []string
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
		snap.Sheets = append(snap.Sheets, item)
		if title != "" {
			ranges = append(ranges, quoteSheetTitle(title))
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
	if len(ranges) == 0 {
		return snap, nil
	}
	formulas, err := g.batchGet(ctx, spreadsheetID, ranges, "FORMULA")
	if err != nil {
		return SheetsSnapshot{}, err
	}
	values, err := g.batchGet(ctx, spreadsheetID, ranges, "FORMATTED_VALUE")
	if err != nil {
		return SheetsSnapshot{}, err
	}
	if err := fillSnapshotValues(&snap, formulas, values); err != nil {
		return SheetsSnapshot{}, err
	}
	return snap, nil
}

func (g googleSheets) batchGet(ctx context.Context, id string, ranges []string, render string) ([]*sheets.ValueRange, error) {
	resp, err := g.service.Spreadsheets.Values.BatchGet(id).
		Ranges(ranges...).
		ValueRenderOption(render).
		Context(ctx).
		Do()
	if err != nil {
		return nil, mapDocsError(err)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.ValueRanges, nil
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

func fillSnapshotValues(snap *SheetsSnapshot, formulas, values []*sheets.ValueRange) error {
	cells := 0
	for i := range snap.Sheets {
		var fvals, vvals [][]any
		if i < len(formulas) && formulas[i] != nil {
			fvals = formulas[i].Values
		}
		if i < len(values) && values[i] != nil {
			vvals = values[i].Values
		}
		rows := max(len(fvals), len(vvals))
		cols := 0
		for _, row := range fvals {
			if len(row) > cols {
				cols = len(row)
			}
		}
		for _, row := range vvals {
			if len(row) > cols {
				cols = len(row)
			}
		}
		if rows > 0 && cols > 0 && cells+rows*cols > MaxSheetCells {
			return fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
		}
		grid := make([][]Cell, rows)
		for r := 0; r < rows; r++ {
			grid[r] = make([]Cell, cols)
			for c := 0; c < cols; c++ {
				in := cellString(valueAt(fvals, r, c))
				val := cellString(valueAt(vvals, r, c))
				grid[r][c] = Cell{Input: in, Value: val}
			}
		}
		snap.Sheets[i].Cells = grid
		trimSheet(&snap.Sheets[i])
		cells += snap.Sheets[i].Rows * snap.Sheets[i].Cols
		if cells > MaxSheetCells {
			return fmt.Errorf("%w (max %d cells)", ErrTooLarge, MaxSheetCells)
		}
	}
	return nil
}

func valueAt(grid [][]any, r, c int) any {
	if r < 0 || r >= len(grid) || c < 0 || c >= len(grid[r]) {
		return nil
	}
	return grid[r][c]
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
	return formatA1(r1, c1) + ":" + formatA1(r2, c2)
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
