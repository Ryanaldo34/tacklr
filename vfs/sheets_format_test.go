package vfs

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

func TestGoogleSheets_httpGetBatchUpdate(t *testing.T) {
	ctx := t.Context()
	var batchBody, valuesBody string
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/spreadsheets/") {
			if strings.Contains(r.URL.Path, "/missing") {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{"code": 404, "message": "not found"},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"spreadsheetId": "sheet1",
				"sheets": []any{
					nil,
					map[string]any{"properties": nil},
					map[string]any{
						"properties": map[string]any{"sheetId": 1, "title": "Budget"},
						"merges":     []any{nil, map[string]any{"startRowIndex": 0, "startColumnIndex": 0, "endRowIndex": 1, "endColumnIndex": 1}},
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
										nil,
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
												"borders":           map[string]any{"top": map[string]any{"style": "DOTTED"}, "bottom": map[string]any{"style": "DOUBLE"}, "left": map[string]any{"style": "NONE"}},
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
									}},
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
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "values:batchUpdate") {
			raw, _ := io.ReadAll(r.Body)
			valuesBody = string(raw)
			_ = json.NewEncoder(w).Encode(map[string]any{})
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":batchUpdate") {
			raw, _ := io.ReadAll(r.Body)
			batchBody = string(raw)
			_ = json.NewEncoder(w).Encode(map[string]any{})
			return
		}
		http.NotFound(w, r)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	holder := NewTokenHolder(Credential{Token: "tok"})
	hc := &http.Client{Transport: &oauth2.Transport{Source: holder, Base: ts.Client().Transport}}
	svc, err := sheets.NewService(ctx, option.WithHTTPClient(hc), option.WithEndpoint(ts.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	api := googleSheets{service: svc}

	if _, err := api.Get(ctx, "missing"); err == nil {
		t.Fatal("Get missing")
	}

	snap, err := api.Get(ctx, "sheet1")
	if err != nil || snap.SpreadsheetID != "sheet1" || len(snap.Sheets) != 2 {
		t.Fatalf("Get = %+v err=%v", snap, err)
	}
	b2 := snap.Sheets[0].cellAt(2, 2)
	if b2.Input != "42" || b2.Value != "$42.00" || !b2.Format.Bold || !b2.Format.Italic ||
		!b2.Format.Strike || !b2.Format.Underline || b2.Format.Number != "$#,##0.00" ||
		b2.Format.Align != "right" || b2.Format.VAlign != "middle" || b2.Format.Wrap != "wrap" ||
		b2.Format.Fill != "#ffcc00" || b2.Format.Color != "#003366" ||
		b2.Format.Border == nil || b2.Format.Border.Style != "medium" {
		t.Fatalf("Get B2 = %+v format=%+v border=%+v", b2, b2.Format, b2.Format.Border)
	}
	c2 := snap.Sheets[0].cellAt(2, 3)
	if c2.Format.Wrap != "clip" || c2.Format.VAlign != "top" {
		t.Fatalf("Get C2 wrap = %+v", c2.Format)
	}
	d2 := snap.Sheets[0].cellAt(2, 4)
	if d2.Format.Wrap != "overflow" || d2.Format.VAlign != "bottom" {
		t.Fatalf("Get D2 wrap = %+v", d2.Format)
	}
	c1 := snap.Sheets[0].cellAt(1, 3)
	if c1.Input != "TRUE" {
		t.Fatalf("Get bool C1 = %+v", c1)
	}
	d1 := snap.Sheets[0].cellAt(1, 4)
	if d1.Input != "3.5" {
		t.Fatalf("Get number D1 = %+v", d1)
	}
	a3 := snap.Sheets[0].cellAt(3, 1)
	if a3.Input != "=A1+1" {
		t.Fatalf("Get formula A3 = %+v", a3)
	}
	if snap.Sheets[1].Title != "Empty" || snap.Sheets[1].Rows != 0 {
		t.Fatalf("empty sheet = %+v", snap.Sheets[1])
	}
	if len(snap.Named) != 2 || snap.Named[0].Name != "Bare" || snap.Named[1].Name != "Total" {
		t.Fatalf("named = %+v", snap.Named)
	}

	if err := api.BatchUpdate(ctx, "sheet1", SheetsBatch{}); err != nil {
		t.Fatal(err)
	}
	if err := api.BatchUpdate(ctx, "sheet1", SheetsBatch{Requests: []SheetsRepeatCell{{
		SheetID: 1, StartRow: 0, StartCol: 0, EndRow: 1, EndCol: 1,
	}}}); err != nil {
		t.Fatal(err)
	}

	full := CellFormat{
		Number: "0.00%", Bold: true, Italic: true, Strike: true, Underline: true,
		Fill: "#ffcc00", Color: "#003366", Align: "right", VAlign: "middle", Wrap: "wrap",
		Border: &CellBorder{Style: "medium", Edges: "top,bottom", Color: "#000000"},
	}
	full.mark(fmtNumber | fmtBold | fmtItalic | fmtStrike | fmtUnderline | fmtFill | fmtColor | fmtAlign | fmtVAlign | fmtWrap | fmtBorder)
	if err := api.BatchUpdate(ctx, "sheet1", SheetsBatch{Requests: []SheetsRepeatCell{
		{SheetID: 1, StartRow: 1, StartCol: 1, EndRow: 2, EndCol: 2, Format: full},
		{SheetID: 1, StartRow: 0, StartCol: 2, EndRow: 1, EndCol: 3, Format: CellFormat{Wrap: "clip", VAlign: "top", Border: &CellBorder{Style: "thick"}}},
		{SheetID: 1, StartRow: 0, StartCol: 3, EndRow: 1, EndCol: 4, Format: CellFormat{Wrap: "overflow", Align: "nope", VAlign: "nope", Border: &CellBorder{Style: "dashed"}}},
		{SheetID: 1, StartRow: 2, StartCol: 0, EndRow: 3, EndCol: 1, Format: CellFormat{Border: &CellBorder{Style: "dotted", Edges: "left"}}},
		{SheetID: 1, StartRow: 2, StartCol: 1, EndRow: 3, EndCol: 2, Format: CellFormat{Border: &CellBorder{Style: "double"}}},
		{SheetID: 1, StartRow: 2, StartCol: 2, EndRow: 3, EndCol: 3, Format: CellFormat{Border: &CellBorder{Style: "none"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(batchBody, "userEnteredFormat") ||
		!strings.Contains(batchBody, "italic") ||
		!strings.Contains(batchBody, "strikethrough") ||
		!strings.Contains(batchBody, "underline") ||
		!strings.Contains(batchBody, "backgroundColorStyle") ||
		!strings.Contains(batchBody, "foregroundColorStyle") ||
		!strings.Contains(batchBody, "rgbColor") ||
		!strings.Contains(batchBody, "horizontalAlignment") ||
		!strings.Contains(batchBody, "verticalAlignment") ||
		!strings.Contains(batchBody, "wrapStrategy") ||
		!strings.Contains(batchBody, "numberFormat") ||
		!strings.Contains(batchBody, "borders") ||
		!strings.Contains(batchBody, "SOLID_MEDIUM") ||
		!strings.Contains(batchBody, "CLIP") ||
		!strings.Contains(batchBody, "OVERFLOW_CELL") {
		t.Fatalf("BatchUpdate body = %s", batchBody)
	}

	off := false
	empty := ""
	cleared := CellFormat{}
	cleared.ApplyPatch(FormatPatch{Bold: &off, Fill: &empty, Align: &empty, VAlign: &empty, Wrap: &empty, Border: &CellBorder{}})
	if err := api.BatchUpdate(ctx, "sheet1", SheetsBatch{Requests: []SheetsRepeatCell{{
		SheetID: 1, StartRow: 0, StartCol: 0, EndRow: 1, EndCol: 1,
		Format: cleared,
	}}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(batchBody, "userEnteredFormat.textFormat.bold") ||
		!strings.Contains(batchBody, "backgroundColorStyle") {
		t.Fatalf("clear format body = %s", batchBody)
	}
	if numberFormatType("h:mm") != "TIME" || numberFormatType("m/d/yyyy h:mm") != "DATE_TIME" ||
		numberFormatType("0.00E+00") != "SCIENTIFIC" || numberFormatType("0%") != "PERCENT" ||
		numberFormatType("yyyy") != "DATE" || numberFormatType("h:mm a/p") != "TIME" ||
		numberFormatType("h:mm am/pm") != "DATE_TIME" {
		t.Fatalf("number types: %s %s %s %s %s %s %s",
			numberFormatType("h:mm"), numberFormatType("m/d/yyyy h:mm"), numberFormatType("0.00E+00"),
			numberFormatType("0%"), numberFormatType("yyyy"), numberFormatType("h:mm a/p"),
			numberFormatType("h:mm am/pm"))
	}

	if err := api.BatchUpdateValues(ctx, "sheet1", SheetsValuesBatch{Data: []SheetsValueRange{
		{Range: "Budget!B2", Values: [][]string{{"99"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(valuesBody, "USER_ENTERED") || !strings.Contains(valuesBody, "99") ||
		!strings.Contains(valuesBody, "Budget!B2") {
		t.Fatalf("BatchUpdateValues body = %s", valuesBody)
	}
}
