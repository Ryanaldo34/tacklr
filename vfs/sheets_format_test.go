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
			_ = json.NewEncoder(w).Encode(map[string]any{
				"spreadsheetId": "sheet1",
				"sheets": []any{
					map[string]any{
						"properties": map[string]any{"sheetId": 1, "title": "Budget", "index": 0},
						"data": []any{
							map[string]any{
								"startRow":    0,
								"startColumn": 0,
								"rowData": []any{
									map[string]any{"values": []any{
										map[string]any{"userEnteredValue": map[string]any{"stringValue": "Date"}, "formattedValue": "Date"},
										map[string]any{"userEnteredValue": map[string]any{"stringValue": "Amount"}, "formattedValue": "Amount"},
									}},
									map[string]any{"values": []any{
										map[string]any{"userEnteredValue": map[string]any{"stringValue": "2026-01-01"}, "formattedValue": "2026-01-01"},
										map[string]any{
											"formattedValue":   "$42.00",
											"userEnteredValue": map[string]any{"stringValue": "42"},
											"userEnteredFormat": map[string]any{
												"numberFormat":        map[string]any{"pattern": "$#,##0.00"},
												"textFormat":          map[string]any{"bold": true},
												"horizontalAlignment": "RIGHT",
												"backgroundColor":     map[string]any{"red": 1, "green": 0.8, "blue": 0},
												"borders":             map[string]any{"bottom": map[string]any{"style": "SOLID"}},
											},
										},
									}},
									map[string]any{"values": []any{
										map[string]any{
											"formattedValue":   "43",
											"userEnteredValue": map[string]any{"formulaValue": "=A1+1"},
										},
									}},
								},
							},
						},
					},
				},
				"namedRanges": []any{
					map[string]any{
						"name": "Total",
						"range": map[string]any{
							"sheetId": 1, "startRowIndex": 1, "startColumnIndex": 1,
							"endRowIndex": 2, "endColumnIndex": 2,
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

	snap, err := api.Get(ctx, "sheet1")
	if err != nil || snap.SpreadsheetID != "sheet1" || len(snap.Sheets) != 1 {
		t.Fatalf("Get = %+v err=%v", snap, err)
	}
	b2 := snap.Sheets[0].cellAt(2, 2)
	if b2.Input != "42" || b2.Value != "$42.00" || !b2.Format.Bold || b2.Format.Number != "$#,##0.00" ||
		b2.Format.Align != "right" || b2.Format.Fill != "#ffcc00" ||
		b2.Format.Border == nil || b2.Format.Border.Style != "thin" || b2.Format.Border.Edges != "bottom" {
		t.Fatalf("Get B2 = %+v format=%+v border=%+v", b2, b2.Format, b2.Format.Border)
	}
	a3 := snap.Sheets[0].cellAt(3, 1)
	if a3.Input != "=A1+1" {
		t.Fatalf("Get formula A3 = %+v", a3)
	}
	if len(snap.Named) != 1 || snap.Named[0].Name != "Total" {
		t.Fatalf("named = %+v", snap.Named)
	}

	err = api.BatchUpdate(ctx, "sheet1", SheetsBatch{Requests: []SheetsRepeatCell{{
		SheetID: 1, StartRow: 1, StartCol: 1, EndRow: 2, EndCol: 2,
		Format: CellFormat{
			Number: "$#,##0.00", Bold: true, Italic: true, Strike: true,
			Fill: "#ffcc00", Color: "#003366", Align: "right",
			Border: &CellBorder{Style: "thin", Edges: "bottom"},
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(batchBody, "userEnteredFormat") ||
		!strings.Contains(batchBody, "italic") ||
		!strings.Contains(batchBody, "strikethrough") ||
		!strings.Contains(batchBody, "backgroundColorStyle") ||
		!strings.Contains(batchBody, "foregroundColorStyle") ||
		!strings.Contains(batchBody, "rgbColor") ||
		!strings.Contains(batchBody, "horizontalAlignment") ||
		!strings.Contains(batchBody, "numberFormat") ||
		!strings.Contains(batchBody, "borders") {
		t.Fatalf("BatchUpdate body = %s", batchBody)
	}

	off := false
	cleared := CellFormat{}
	cleared.ApplyPatch(FormatPatch{Bold: &off})
	err = api.BatchUpdate(ctx, "sheet1", SheetsBatch{Requests: []SheetsRepeatCell{{
		SheetID: 1, StartRow: 0, StartCol: 0, EndRow: 1, EndCol: 1,
		Format: cleared,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(batchBody, "userEnteredFormat.textFormat.bold") {
		t.Fatalf("clear bold body = %s", batchBody)
	}
	if numberFormatType("h:mm") != "TIME" || numberFormatType("m/d/yyyy h:mm") != "DATE_TIME" ||
		numberFormatType("0.00E+00") != "SCIENTIFIC" {
		t.Fatalf("number types: %s %s %s", numberFormatType("h:mm"), numberFormatType("m/d/yyyy h:mm"), numberFormatType("0.00E+00"))
	}

	err = api.BatchUpdateValues(ctx, "sheet1", SheetsValuesBatch{Data: []SheetsValueRange{
		{Range: "Budget!B2", Values: [][]string{{"99"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(valuesBody, "USER_ENTERED") || !strings.Contains(valuesBody, "99") ||
		!strings.Contains(valuesBody, "Budget!B2") {
		t.Fatalf("BatchUpdateValues body = %s", valuesBody)
	}
}
