// Package sheets is the Sheets v4 JSON the vfs adapter reads and writes.
package sheets

// Spreadsheet is spreadsheets.get with grid data.
type Spreadsheet struct {
	SpreadsheetID string        `json:"spreadsheetId,omitempty"`
	Sheets        []*Sheet      `json:"sheets,omitempty"`
	NamedRanges   []*NamedRange `json:"namedRanges,omitempty"`
}

// Sheet is one tab: its properties, merges and cells.
type Sheet struct {
	Properties *SheetProperties `json:"properties,omitempty"`
	Merges     []*GridRange     `json:"merges,omitempty"`
	Data       []*GridData      `json:"data,omitempty"`
}

// SheetProperties names a sheet.
type SheetProperties struct {
	SheetID int64  `json:"sheetId,omitempty"`
	Title   string `json:"title,omitempty"`
	Index   int64  `json:"index,omitempty"`
}

// GridRange is a half-open block of cells on a sheet.
type GridRange struct {
	SheetID          int64 `json:"sheetId,omitempty"`
	StartRowIndex    int64 `json:"startRowIndex,omitempty"`
	EndRowIndex      int64 `json:"endRowIndex,omitempty"`
	StartColumnIndex int64 `json:"startColumnIndex,omitempty"`
	EndColumnIndex   int64 `json:"endColumnIndex,omitempty"`
}

// NamedRange is a named block of cells.
type NamedRange struct {
	Name  string     `json:"name,omitempty"`
	Range *GridRange `json:"range,omitempty"`
}

// GridData is cells from a start row and column.
type GridData struct {
	StartRow    int64      `json:"startRow,omitempty"`
	StartColumn int64      `json:"startColumn,omitempty"`
	RowData     []*RowData `json:"rowData,omitempty"`
}

// RowData is one row's cells.
type RowData struct {
	Values []*CellData `json:"values,omitempty"`
}

// CellData is one cell: what the user entered, how it shows, its format.
type CellData struct {
	UserEnteredValue  *ExtendedValue `json:"userEnteredValue,omitempty"`
	FormattedValue    string         `json:"formattedValue,omitempty"`
	UserEnteredFormat *CellFormat    `json:"userEnteredFormat,omitempty"`
}

// ExtendedValue is a cell input; one field is set.
type ExtendedValue struct {
	FormulaValue *string  `json:"formulaValue,omitempty"`
	StringValue  *string  `json:"stringValue,omitempty"`
	NumberValue  *float64 `json:"numberValue,omitempty"`
	BoolValue    *bool    `json:"boolValue,omitempty"`
}

// CellFormat is a cell's format.
type CellFormat struct {
	NumberFormat         *NumberFormat `json:"numberFormat,omitempty"`
	TextFormat           *TextFormat   `json:"textFormat,omitempty"`
	BackgroundColor      *Color        `json:"backgroundColor,omitempty"`
	BackgroundColorStyle *ColorStyle   `json:"backgroundColorStyle,omitempty"`
	HorizontalAlignment  string        `json:"horizontalAlignment,omitempty"`
	VerticalAlignment    string        `json:"verticalAlignment,omitempty"`
	WrapStrategy         string        `json:"wrapStrategy,omitempty"`
	Borders              *Borders      `json:"borders,omitempty"`
}

// NumberFormat is a number pattern and its type.
type NumberFormat struct {
	Type    string `json:"type,omitempty"`
	Pattern string `json:"pattern,omitempty"`
}

// TextFormat is a cell's text style.
type TextFormat struct {
	Bold                 bool        `json:"bold,omitempty"`
	Italic               bool        `json:"italic,omitempty"`
	Strikethrough        bool        `json:"strikethrough,omitempty"`
	Underline            bool        `json:"underline,omitempty"`
	ForegroundColor      *Color      `json:"foregroundColor,omitempty"`
	ForegroundColorStyle *ColorStyle `json:"foregroundColorStyle,omitempty"`
}

// Color is an RGB color with channels from 0 to 1.
type Color struct {
	Red   float64 `json:"red,omitempty"`
	Green float64 `json:"green,omitempty"`
	Blue  float64 `json:"blue,omitempty"`
}

// ColorStyle is a color; an empty one clears it.
type ColorStyle struct {
	RGBColor *Color `json:"rgbColor,omitempty"`
}

// Borders are a cell's four edges.
type Borders struct {
	Top    *Border `json:"top,omitempty"`
	Bottom *Border `json:"bottom,omitempty"`
	Left   *Border `json:"left,omitempty"`
	Right  *Border `json:"right,omitempty"`
}

// Border is one edge.
type Border struct {
	Style      string      `json:"style,omitempty"`
	Color      *Color      `json:"color,omitempty"`
	ColorStyle *ColorStyle `json:"colorStyle,omitempty"`
}

// BatchUpdateSpreadsheetRequest is spreadsheets.batchUpdate.
type BatchUpdateSpreadsheetRequest struct {
	Requests []*Request `json:"requests,omitempty"`
}

// Request is one batchUpdate edit.
type Request struct {
	RepeatCell *RepeatCellRequest `json:"repeatCell,omitempty"`
}

// RepeatCellRequest sets the Fields of Cell on every cell of Range.
type RepeatCellRequest struct {
	Range  *GridRange `json:"range,omitempty"`
	Cell   *CellData  `json:"cell,omitempty"`
	Fields string     `json:"fields,omitempty"`
}

// BatchUpdateValuesRequest is spreadsheets.values.batchUpdate.
type BatchUpdateValuesRequest struct {
	ValueInputOption string        `json:"valueInputOption,omitempty"`
	Data             []*ValueRange `json:"data,omitempty"`
}

// ValueRange is the values of an A1 range.
type ValueRange struct {
	Range  string     `json:"range,omitempty"`
	Values [][]string `json:"values,omitempty"`
}
