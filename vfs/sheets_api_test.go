package vfs_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// The Sheets client reads a workbook with its values, formulas, formats,
// borders and named ranges, skips null entries, and refuses a grid
// that starts before the sheet.
func TestGoogleSheets_httpGet(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.New()
	fx.SeedSheetJSON("negative", map[string]any{"sheets": []any{map[string]any{
		"properties": map[string]any{"sheetId": 1, "title": "Bad"},
		"data":       []any{map[string]any{"startRow": -1, "rowData": []any{}}},
	}}})
	fx.SeedSheetJSON("sheet1", map[string]any{
		"sheets": []any{
			nil,
			map[string]any{"properties": nil},
			map[string]any{
				"properties": map[string]any{"sheetId": 1, "title": "Budget"},
				"data": []any{
					nil,
					map[string]any{
						"startRow":    0,
						"startColumn": 0,
						"rowData": []any{
							map[string]any{"values": []any{
								map[string]any{"userEnteredValue": map[string]any{"stringValue": "Date"}, "formattedValue": "Date"},
								map[string]any{"userEnteredValue": map[string]any{"stringValue": "Amount"}, "formattedValue": "Amount"},
								map[string]any{"userEnteredValue": map[string]any{"boolValue": true}, "formattedValue": "TRUE"},
								map[string]any{"userEnteredValue": map[string]any{"numberValue": 3.5}, "formattedValue": "3.5"},
								map[string]any{"userEnteredValue": map[string]any{"numberValue": 42}, "formattedValue": "42"},
								map[string]any{"formattedValue": "shown"},
							}},
							map[string]any{"values": []any{
								map[string]any{"userEnteredValue": map[string]any{"stringValue": "2026-01-01"}, "formattedValue": "2026-01-01"},
								map[string]any{
									"formattedValue":   "$42.00",
									"userEnteredValue": map[string]any{"stringValue": "42"},
									"userEnteredFormat": map[string]any{
										"numberFormat":         map[string]any{"pattern": "$#,##0.00"},
										"textFormat":           map[string]any{"bold": true, "italic": true, "strikethrough": true, "underline": true, "foregroundColorStyle": map[string]any{"rgbColor": map[string]any{"red": 0, "green": 0.2, "blue": 0.4}}},
										"horizontalAlignment":  "RIGHT",
										"verticalAlignment":    "MIDDLE",
										"wrapStrategy":         "WRAP",
										"backgroundColorStyle": map[string]any{"rgbColor": map[string]any{"red": 1, "green": 0.8, "blue": 0}},
										"borders": map[string]any{
											"top":    map[string]any{"style": "SOLID_MEDIUM", "colorStyle": map[string]any{"rgbColor": map[string]any{"red": 0, "green": 0, "blue": 0}}},
											"bottom": map[string]any{"style": "SOLID_MEDIUM"},
											"left":   map[string]any{"style": "SOLID_THICK"},
											"right":  map[string]any{"style": "DASHED"},
										},
									},
								},
								map[string]any{
									"formattedValue":   "clip",
									"userEnteredValue": map[string]any{"stringValue": "clip"},
									"userEnteredFormat": map[string]any{
										"wrapStrategy":      "CLIP",
										"verticalAlignment": "TOP",
										"borders":           map[string]any{"top": map[string]any{"style": "DOTTED"}, "bottom": map[string]any{"style": "DOUBLE"}, "left": map[string]any{"style": "NONE"}, "right": map[string]any{"style": "SOLID"}},
									},
								},
								map[string]any{
									"formattedValue":    "overflow",
									"userEnteredValue":  map[string]any{"stringValue": "overflow"},
									"userEnteredFormat": map[string]any{"wrapStrategy": "OVERFLOW_CELL", "verticalAlignment": "BOTTOM"},
								},
							}},
							map[string]any{"values": []any{
								map[string]any{
									"formattedValue":   "43",
									"userEnteredValue": map[string]any{"formulaValue": "A1+1"},
								},
								map[string]any{"userEnteredValue": map[string]any{"boolValue": false}, "formattedValue": "FALSE"},
								nil,
								map[string]any{"userEnteredValue": map[string]any{}, "formattedValue": "#N/A"},
								map[string]any{"formattedValue": "plain", "userEnteredFormat": map[string]any{"borders": map[string]any{"top": map[string]any{"style": "NONE"}}}},
							}},
							nil,
						},
					},
				},
			},
			map[string]any{
				"properties": map[string]any{"sheetId": 2, "title": "Empty", "index": 1},
				"data":       []any{map[string]any{"startRow": 0, "startColumn": 0, "rowData": []any{}}},
			},
		},
		"namedRanges": []any{
			nil,
			map[string]any{"name": "Bare"},
			map[string]any{
				"name": "Total",
				"range": map[string]any{
					"sheetId": 1, "startRowIndex": 1, "startColumnIndex": 1,
					"endRowIndex": 1, "endColumnIndex": 1,
				},
			},
		},
	})
	fx.Fail(testdrive.Fault{Call: "spreadsheets.get", ID: "missing", Status: http.StatusNotFound})
	srv := testhttp.New(t, fx)
	api := vfs.NewGoogleSheets(vfs.NewTokenHolder(mount.Credential{Token: "tok"}), srv.URL+"/", srv.Client())

	if _, err := api.Get(ctx, "missing"); err == nil {
		t.Fatal("Get missing")
	}
	if _, err := api.Get(ctx, "negative"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("Get with a negative start row = %v, want ErrTooLarge", err)
	}

	snap, err := api.Get(ctx, "sheet1")
	if err != nil || len(snap.Sheets) != 2 {
		t.Fatalf("Get = %+v err=%v", snap, err)
	}
	at := func(row, col int) vfs.Cell { return snap.Sheets[0].Cells[row-1][col-1] }
	b2 := at(2, 2)
	if b2.Input != "42" || b2.Value != "$42.00" || !b2.Format.Bold || !b2.Format.Italic ||
		!b2.Format.Strike || !b2.Format.Underline || b2.Format.Number != "$#,##0.00" ||
		b2.Format.Align != "right" || b2.Format.VAlign != "middle" || b2.Format.Wrap != "wrap" ||
		b2.Format.Fill != "#ffcc00" || b2.Format.Color != "#003366" ||
		b2.Format.Border == nil || b2.Format.Border.Style != "medium" {
		t.Fatalf("Get B2 = %+v format=%+v border=%+v", b2, b2.Format, b2.Format.Border)
	}
	c2 := at(2, 3)
	if c2.Format.Wrap != "clip" || c2.Format.VAlign != "top" {
		t.Fatalf("Get C2 wrap = %+v", c2.Format)
	}
	d2 := at(2, 4)
	if d2.Format.Wrap != "overflow" || d2.Format.VAlign != "bottom" {
		t.Fatalf("Get D2 wrap = %+v", d2.Format)
	}
	c1 := at(1, 3)
	if c1.Input != "TRUE" {
		t.Fatalf("Get bool C1 = %+v", c1)
	}
	d1 := at(1, 4)
	if d1.Input != "3.5" {
		t.Fatalf("Get number D1 = %+v", d1)
	}
	e1 := at(1, 5)
	if e1.Input != "42" {
		t.Fatalf("Get int E1 = %+v", e1)
	}
	f1 := at(1, 6)
	if f1.Input != "shown" || f1.Value != "shown" {
		t.Fatalf("Get formatted-only F1 = %+v", f1)
	}
	a3 := at(3, 1)
	if a3.Input != "=A1+1" {
		t.Fatalf("Get formula A3 = %+v", a3)
	}
	if d3, e3 := at(3, 4), at(3, 5); d3.Input != "#N/A" || e3.Input != "plain" || e3.Format.Border != nil {
		t.Fatalf("Get error value D3 = %+v, borderless E3 = %+v", d3, e3)
	}
	if snap.Sheets[1].Title != "Empty" || snap.Sheets[1].Rows != 0 {
		t.Fatalf("empty sheet = %+v", snap.Sheets[1])
	}
	if len(snap.Named) != 2 || snap.Named[0].Name != "Bare" || snap.Named[1].Name != "Total" {
		t.Fatalf("named = %+v", snap.Named)
	}
}
