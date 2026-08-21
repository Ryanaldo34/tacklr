package adapters

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs"
)

const XLSXMediaType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

const (
	relOfficeDoc = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
	relWorksheet = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
	relSharedStr = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings"
	relStyles    = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"
	nsMain       = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
	nsPkgRel     = "http://schemas.openxmlformats.org/package/2006/relationships"
	nsPkgTypes   = "http://schemas.openxmlformats.org/package/2006/content-types"
	nsOfficeRel  = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
	ctSheet      = "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
	ctWorkbook   = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
	ctShared     = "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"
	ctStyles     = "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"
)

// XLSX maps workbook.xml / sheetN.xml / sharedStrings.xml / styles.xml to TabularDocument.
type XLSX struct{}

func (XLSX) MediaTypes() []string { return []string{XLSXMediaType} }

func (XLSX) DecodeSheets(ctx context.Context, _, _ string, data []byte) ([]vfs.Sheet, []vfs.NamedRange, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(data) > vfs.MaxReadFileBytes {
		return nil, nil, fmt.Errorf("%w (max %d bytes)", vfs.ErrTooLarge, vfs.MaxReadFileBytes)
	}
	sheets, err := parseXLSX(data)
	if err != nil {
		return nil, nil, err
	}
	return sheets, nil, nil
}

func (XLSX) EncodeSheets(ctx context.Context, sheets []vfs.Sheet, _ []vfs.NamedRange) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encodeXLSX(sheets)
}

func (XLSX) Decode(ctx context.Context, virtualPath, mediaType string, data []byte) (vfs.Document, error) {
	sheets, _, err := (XLSX{}).DecodeSheets(ctx, virtualPath, mediaType, data)
	if err != nil {
		return nil, err
	}
	if mediaType == "" {
		mediaType = XLSXMediaType
	}
	return vfs.NewTabularDocument(virtualPath, mediaType, sheets, nil)
}

func (XLSX) Encode(ctx context.Context, doc vfs.Document) ([]byte, error) {
	g, ok := vfs.AsGrid(doc)
	if !ok {
		return nil, fmt.Errorf("xlsx: %w", vfs.ErrNotSupported)
	}
	return (XLSX{}).EncodeSheets(ctx, g.Sheets(), g.NamedRanges())
}

func parseXLSX(data []byte) ([]vfs.Sheet, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("xlsx: %w", err)
	}
	parts := zipParts(zr)
	if parts["xl/workbook.xml"] == nil {
		return nil, fmt.Errorf("xlsx: missing workbook.xml")
	}
	var wb []wbSheet
	if err := readXLSXPart(parts, "xl/workbook.xml", func(r io.Reader) error {
		var e error
		wb, e = parseWorkbook(r)
		return e
	}); err != nil {
		return nil, err
	}
	var rels map[string]string
	if err := readXLSXPart(parts, "xl/_rels/workbook.xml.rels", func(r io.Reader) error {
		var e error
		rels, e = parseRels(r)
		return e
	}); err != nil {
		return nil, err
	}
	var ss []string
	if err := readXLSXPart(parts, "xl/sharedStrings.xml", func(r io.Reader) error {
		var e error
		ss, e = parseSharedStrings(r)
		return e
	}); err != nil {
		return nil, err
	}
	var st xlsxStyles
	if err := readXLSXPart(parts, "xl/styles.xml", func(r io.Reader) error {
		var e error
		st, e = parseStyles(r)
		return e
	}); err != nil {
		return nil, err
	}
	out := make([]vfs.Sheet, 0, len(wb))
	for i, w := range wb {
		target := rels[w.rid]
		if target == "" {
			continue
		}
		target = path.Clean(strings.TrimPrefix(target, "/"))
		if !strings.HasPrefix(target, "xl/") {
			target = path.Join("xl", target)
		}
		var cells [][]vfs.Cell
		if err := readXLSXPart(parts, target, func(r io.Reader) error {
			var e error
			cells, e = parseSheetXML(r, ss, st)
			return e
		}); err != nil {
			return nil, err
		}
		id := w.sheetID
		if id == "" {
			id = strconv.Itoa(i + 1)
		}
		out = append(out, vfs.Sheet{ID: id, Title: w.name, Index: i, Cells: cells})
	}
	if len(out) == 0 {
		out = []vfs.Sheet{{Title: "Sheet1", ID: "1"}}
	}
	return out, nil
}

func zipParts(zr *zip.Reader) map[string]*zip.File {
	m := make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		m[path.Clean(f.Name)] = f
	}
	return m
}

func readXLSXPart(parts map[string]*zip.File, name string, fn func(io.Reader) error) error {
	f := parts[path.Clean(name)]
	if f == nil {
		return fn(bytes.NewReader(nil))
	}
	if f.UncompressedSize64 > uint64(vfs.MaxReadFileBytes) {
		return fmt.Errorf("%w (max %d bytes)", vfs.ErrTooLarge, vfs.MaxReadFileBytes)
	}
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	lr := &io.LimitedReader{R: rc, N: int64(vfs.MaxReadFileBytes) + 1}
	if err := fn(lr); err != nil {
		return err
	}
	if lr.N == 0 {
		return fmt.Errorf("%w (max %d bytes)", vfs.ErrTooLarge, vfs.MaxReadFileBytes)
	}
	return nil
}

type wbSheet struct{ name, sheetID, rid string }

func parseWorkbook(r io.Reader) ([]wbSheet, error) {
	dec := xml.NewDecoder(r)
	var out []wbSheet
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xlsx workbook: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "sheet" {
			continue
		}
		out = append(out, wbSheet{
			name:    attr(se, "name"),
			sheetID: attr(se, "sheetId"),
			rid:     attr(se, "id"),
		})
	}
	return out, nil
}

func parseRels(r io.Reader) (map[string]string, error) {
	out := map[string]string{}
	if r == nil {
		return out, nil
	}
	dec := xml.NewDecoder(r)
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xlsx rels: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "Relationship" {
			continue
		}
		out[attr(se, "Id")] = attr(se, "Target")
	}
	return out, nil
}

func parseSharedStrings(r io.Reader) ([]string, error) {
	dec := xml.NewDecoder(r)
	var out []string
	var cur strings.Builder
	inSI, inT := false, false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xlsx sharedStrings: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "sst":
				if n, err := strconv.Atoi(attr(t, "uniqueCount")); err == nil && n > 0 {
					out = make([]string, 0, n)
				}
			case "si":
				inSI = true
				cur.Reset()
			case "t":
				inT = inSI
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "t":
				inT = false
			case "si":
				out = append(out, cur.String())
				inSI = false
			}
		case xml.CharData:
			if inT {
				cur.Write(t)
			}
		}
	}
	return out, nil
}

type xlsxStyles struct {
	numFmts []string
	fonts   []vfs.CellFormat
	fills   []string
	borders []vfs.CellBorder
	xfs     []vfs.CellFormat
}

func parseStyles(r io.Reader) (xlsxStyles, error) {
	st := xlsxStyles{numFmts: make([]string, 164)}
	for id, code := range builtinNumFmts {
		if id < len(st.numFmts) {
			st.numFmts[id] = code
		}
	}
	dec := xml.NewDecoder(r)
	section := ""
	var font vfs.CellFormat
	var fill string
	var border vfs.CellBorder
	var xf vfs.CellFormat
	inBorder := false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return xlsxStyles{}, fmt.Errorf("xlsx styles: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "numFmts", "fonts", "fills", "borders", "cellXfs":
				section = t.Name.Local
			case "numFmt":
				id, _ := strconv.Atoi(attr(t, "numFmtId"))
				for len(st.numFmts) <= id {
					st.numFmts = append(st.numFmts, "")
				}
				st.numFmts[id] = attr(t, "formatCode")
			case "font":
				font = vfs.CellFormat{}
			case "b":
				font.Bold = true
			case "i":
				font.Italic = true
			case "strike":
				font.Strike = true
			case "u":
				font.Underline = true
			case "color":
				if section == "fonts" {
					font.Color = vfs.HexColor(attr(t, "rgb"))
				} else if inBorder {
					border.Color = vfs.HexColor(attr(t, "rgb"))
				}
			case "fill":
				fill = ""
			case "fgColor":
				fill = vfs.HexColor(attr(t, "rgb"))
			case "border":
				border = vfs.CellBorder{}
			case "left", "right", "top", "bottom":
				if section != "borders" {
					break
				}
				inBorder = true
				style := attr(t, "style")
				if style == "" {
					break
				}
				if border.Style == "" {
					border.Style = style
				}
				name := t.Name.Local
				if border.Edges == "" {
					border.Edges = name
				} else {
					border.Edges += "," + name
				}
			case "xf":
				xf = vfs.CellFormat{}
				numFmtID, _ := strconv.Atoi(attr(t, "numFmtId"))
				fontID, _ := strconv.Atoi(attr(t, "fontId"))
				fillID, _ := strconv.Atoi(attr(t, "fillId"))
				borderID, _ := strconv.Atoi(attr(t, "borderId"))
				if numFmtID < len(st.numFmts) {
					xf.Number = st.numFmts[numFmtID]
					if xf.Number == "General" {
						xf.Number = ""
					}
				}
				if fontID >= 0 && fontID < len(st.fonts) {
					f := st.fonts[fontID]
					xf.Bold, xf.Italic, xf.Strike, xf.Underline, xf.Color = f.Bold, f.Italic, f.Strike, f.Underline, f.Color
				}
				if fillID >= 0 && fillID < len(st.fills) {
					xf.Fill = st.fills[fillID]
				}
				if borderID >= 0 && borderID < len(st.borders) && st.borders[borderID].Style != "" {
					b := st.borders[borderID]
					if edgeCount(b.Edges) >= 4 {
						b.Edges = ""
					}
					xf.Border = &b
				}
			case "alignment":
				xf.Align = strings.ToLower(attr(t, "horizontal"))
				xf.VAlign = strings.ToLower(attr(t, "vertical"))
				if attr(t, "wrapText") == "1" || strings.EqualFold(attr(t, "wrapText"), "true") {
					xf.Wrap = "wrap"
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "font":
				if section == "fonts" {
					st.fonts = append(st.fonts, font)
				}
			case "fill":
				if section == "fills" {
					st.fills = append(st.fills, fill)
				}
			case "border":
				if section == "borders" {
					st.borders = append(st.borders, border)
				}
			case "xf":
				if section == "cellXfs" {
					xf.Normalize()
					st.xfs = append(st.xfs, xf)
				}
			case "left", "right", "top", "bottom":
				inBorder = false
			case "numFmts", "fonts", "fills", "borders", "cellXfs":
				section = ""
			}
		}
	}
	return st, nil
}

func edgeCount(edges string) int {
	if edges == "" {
		return 4
	}
	return strings.Count(edges, ",") + 1
}

func parseSheetXML(r io.Reader, ss []string, st xlsxStyles) ([][]vfs.Cell, error) {
	dec := xml.NewDecoder(r)
	type curCell struct {
		ref, t    string
		style     int
		f, v, is  strings.Builder
		inF, inV  bool
		inIS, inT bool
		alive     bool
	}
	var cur curCell
	maxR, maxC := 0, 0
	type placed struct {
		r, c int
		cell vfs.Cell
	}
	var placedCells []placed
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xlsx sheet: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "c":
				cur.alive = true
				cur.ref, cur.t = attr(t, "r"), attr(t, "t")
				cur.style = 0
				cur.f.Reset()
				cur.v.Reset()
				cur.is.Reset()
				cur.inF, cur.inV, cur.inIS, cur.inT = false, false, false, false
				if s := attr(t, "s"); s != "" {
					cur.style, _ = strconv.Atoi(s)
				}
			case "f":
				if cur.alive {
					cur.inF = true
				}
			case "v":
				if cur.alive {
					cur.inV = true
				}
			case "is":
				if cur.alive {
					cur.inIS = true
				}
			case "t":
				if cur.alive && cur.inIS {
					cur.inT = true
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "f":
				cur.inF = false
			case "v":
				cur.inV = false
			case "t":
				cur.inT = false
			case "is":
				cur.inIS = false
			case "c":
				if !cur.alive || cur.ref == "" {
					cur.alive = false
					break
				}
				r, c, r2, c2, err := vfs.ParseA1(cur.ref)
				if err != nil || r != r2 || c != c2 {
					cur.alive = false
					break
				}
				fv, vv, is := cur.f.String(), cur.v.String(), cur.is.String()
				cell := vfs.Cell{Value: vv}
				switch {
				case fv != "":
					if !strings.HasPrefix(fv, "=") {
						fv = "=" + fv
					}
					cell.Input = fv
				case cur.t == "s":
					if i, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil && i >= 0 && i < len(ss) {
						cell.Input = ss[i]
						cell.Value = ss[i]
					}
				case cur.t == "inlineStr":
					cell.Input = is
					cell.Value = is
				case cur.t == "b":
					if vv == "1" {
						cell.Input, cell.Value = "TRUE", "TRUE"
					} else {
						cell.Input, cell.Value = "FALSE", "FALSE"
					}
				default:
					cell.Input = vv
					cell.Value = vv
				}
				if cur.style >= 0 && cur.style < len(st.xfs) {
					cell.Format = st.xfs[cur.style]
				}
				if r > maxR {
					maxR = r
				}
				if c > maxC {
					maxC = c
				}
				placedCells = append(placedCells, placed{r: r, c: c, cell: cell})
				cur.alive = false
			}
		case xml.CharData:
			if !cur.alive {
				continue
			}
			switch {
			case cur.inF:
				cur.f.Write(t)
			case cur.inV:
				cur.v.Write(t)
			case cur.inT:
				cur.is.Write(t)
			}
		}
	}
	if maxR == 0 {
		return nil, nil
	}
	if maxC < 1 || maxC > vfs.MaxSheetCells || maxR > vfs.MaxSheetCells/maxC {
		return nil, fmt.Errorf("%w (max %d cells)", vfs.ErrTooLarge, vfs.MaxSheetCells)
	}
	grid := make([][]vfs.Cell, maxR)
	for i := range grid {
		grid[i] = make([]vfs.Cell, maxC)
	}
	for _, p := range placedCells {
		grid[p.r-1][p.c-1] = p.cell
	}
	return grid, nil
}

var builtinNumFmts = map[int]string{
	0: "General", 1: "0", 2: "0.00", 3: "#,##0", 4: "#,##0.00",
	9: "0%", 10: "0.00%", 14: "mm-dd-yy", 49: "@",
}

type styleKey struct {
	Number, Fill, Color, Align, VAlign, Wrap string
	Bold, Italic, Strike, Underline          bool
	BStyle, BColor, BEdges                   string
}

func formatKey(f vfs.CellFormat) styleKey {
	k := styleKey{
		Number: f.Number, Fill: f.Fill, Color: f.Color, Align: f.Align,
		VAlign: f.VAlign, Wrap: f.Wrap,
		Bold: f.Bold, Italic: f.Italic, Strike: f.Strike, Underline: f.Underline,
	}
	if f.Border != nil {
		k.BStyle, k.BColor, k.BEdges = f.Border.Style, f.Border.Color, f.Border.Edges
	}
	return k
}

func encodeXLSX(sheets []vfs.Sheet) ([]byte, error) {
	if len(sheets) == 0 {
		sheets = []vfs.Sheet{{Title: "Sheet1"}}
	}
	type intern struct {
		fonts   []vfs.CellFormat
		fills   []string
		borders []vfs.CellBorder
		numFmts []string
		numIDs  []int
		xfs     []vfs.CellFormat
		xfIndex map[styleKey]int
	}
	st := intern{
		fonts:   []vfs.CellFormat{{}},
		fills:   []string{"", "gray125"},
		borders: []vfs.CellBorder{{}},
		xfIndex: map[styleKey]int{{}: 0},
		xfs:     []vfs.CellFormat{{}},
	}
	indexOf := func(f vfs.CellFormat) int {
		f.Normalize()
		if f.IsZero() {
			return 0
		}
		k := formatKey(f)
		if i, ok := st.xfIndex[k]; ok {
			return i
		}
		st.fonts = append(st.fonts, vfs.CellFormat{Bold: f.Bold, Italic: f.Italic, Strike: f.Strike, Underline: f.Underline, Color: f.Color})
		st.fills = append(st.fills, f.Fill)
		if f.Border != nil {
			st.borders = append(st.borders, *f.Border)
		} else {
			st.borders = append(st.borders, vfs.CellBorder{})
		}
		if f.Number != "" {
			st.numFmts = append(st.numFmts, f.Number)
			st.numIDs = append(st.numIDs, 164+len(st.numIDs))
		}
		st.xfs = append(st.xfs, f)
		i := len(st.xfs) - 1
		st.xfIndex[k] = i
		return i
	}
	var shared []string
	sharedAt := map[string]int{}
	internStr := func(s string) int {
		if i, ok := sharedAt[s]; ok {
			return i
		}
		i := len(shared)
		shared = append(shared, s)
		sharedAt[s] = i
		return i
	}

	names := make([]string, len(sheets))
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	writeBytes := func(name string, body []byte) error {
		w, err := zw.Create(name)
		if err != nil {
			return err
		}
		_, err = w.Write(body)
		return err
	}
	var sheetBuf bytes.Buffer
	for i, sh := range sheets {
		name := sh.Title
		if name == "" {
			name = "Sheet" + strconv.Itoa(i+1)
		}
		names[i] = name
		sheetBuf.Reset()
		writeSheetXML(&sheetBuf, sh, indexOf, internStr)
		if err := writeBytes(fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1), sheetBuf.Bytes()); err != nil {
			return nil, err
		}
	}

	var types strings.Builder
	types.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	types.WriteString(`<Types xmlns="` + nsPkgTypes + `">`)
	types.WriteString(`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>`)
	types.WriteString(`<Default Extension="xml" ContentType="application/xml"/>`)
	types.WriteString(`<Override PartName="/xl/workbook.xml" ContentType="` + ctWorkbook + `"/>`)
	types.WriteString(`<Override PartName="/xl/sharedStrings.xml" ContentType="` + ctShared + `"/>`)
	types.WriteString(`<Override PartName="/xl/styles.xml" ContentType="` + ctStyles + `"/>`)
	var wb, wbRels strings.Builder
	wb.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	wb.WriteString(`<workbook xmlns="` + nsMain + `" xmlns:r="` + nsOfficeRel + `"><sheets>`)
	wbRels.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	wbRels.WriteString(`<Relationships xmlns="` + nsPkgRel + `">`)
	for i, name := range names {
		rid := "rId" + strconv.Itoa(i+1)
		part := fmt.Sprintf("/xl/worksheets/sheet%d.xml", i+1)
		types.WriteString(`<Override PartName="` + part + `" ContentType="` + ctSheet + `"/>`)
		wb.WriteString(`<sheet name="`)
		writeXMLEsc(&wb, name)
		wb.WriteString(`" sheetId="` + strconv.Itoa(i+1) + `" r:id="` + rid + `"/>`)
		wbRels.WriteString(`<Relationship Id="` + rid + `" Type="` + relWorksheet + `" Target="worksheets/sheet` + strconv.Itoa(i+1) + `.xml"/>`)
	}
	n := len(names)
	wbRels.WriteString(`<Relationship Id="rId` + strconv.Itoa(n+1) + `" Type="` + relSharedStr + `" Target="sharedStrings.xml"/>`)
	wbRels.WriteString(`<Relationship Id="rId` + strconv.Itoa(n+2) + `" Type="` + relStyles + `" Target="styles.xml"/>`)
	wb.WriteString(`</sheets></workbook>`)
	wbRels.WriteString(`</Relationships>`)
	types.WriteString(`</Types>`)
	if err := writeBytes("[Content_Types].xml", []byte(types.String())); err != nil {
		return nil, err
	}
	if err := writeBytes("_rels/.rels", []byte(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="`+nsPkgRel+`"><Relationship Id="rId1" Type="`+relOfficeDoc+`" Target="xl/workbook.xml"/></Relationships>`)); err != nil {
		return nil, err
	}
	if err := writeBytes("xl/workbook.xml", []byte(wb.String())); err != nil {
		return nil, err
	}
	if err := writeBytes("xl/_rels/workbook.xml.rels", []byte(wbRels.String())); err != nil {
		return nil, err
	}
	sheetBuf.Reset()
	writeShared(&sheetBuf, shared)
	if err := writeBytes("xl/sharedStrings.xml", sheetBuf.Bytes()); err != nil {
		return nil, err
	}
	sheetBuf.Reset()
	writeStyles(&sheetBuf, st.fonts, st.fills, st.borders, st.numFmts, st.numIDs, st.xfs)
	if err := writeBytes("xl/styles.xml", sheetBuf.Bytes()); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeSheetXML(w *bytes.Buffer, sh vfs.Sheet, indexOf func(vfs.CellFormat) int, internStr func(string) int) {
	w.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	w.WriteString(`<worksheet xmlns="` + nsMain + `"><sheetData>`)
	var num [32]byte
	for r, row := range sh.Cells {
		if len(row) == 0 {
			continue
		}
		w.WriteString(`<row r="`)
		w.Write(strconv.AppendInt(num[:0], int64(r+1), 10))
		w.WriteString(`">`)
		for c, cell := range row {
			if cell.Input == "" && cell.Value == "" && cell.Format.IsZero() {
				continue
			}
			w.WriteString(`<c r="`)
			w.WriteString(vfs.FormatA1(r+1, c+1))
			w.WriteByte('"')
			if si := indexOf(cell.Format); si > 0 {
				w.WriteString(` s="`)
				w.Write(strconv.AppendInt(num[:0], int64(si), 10))
				w.WriteByte('"')
			}
			switch {
			case strings.HasPrefix(cell.Input, "="):
				w.WriteString(`><f>`)
				writeXMLEsc(w, strings.TrimPrefix(cell.Input, "="))
				w.WriteString(`</f>`)
				if cell.Value != "" {
					w.WriteString(`<v>`)
					writeXMLEsc(w, cell.Value)
					w.WriteString(`</v>`)
				}
				w.WriteString(`</c>`)
			case cell.Format.Number != "" && isNumericInput(cell.Input):
				w.WriteString(`><v>`)
				writeXMLEsc(w, cell.Input)
				w.WriteString(`</v></c>`)
			default:
				text := cell.Input
				if text == "" {
					text = cell.Value
				}
				idx := internStr(text)
				w.WriteString(` t="s"><v>`)
				w.Write(strconv.AppendInt(num[:0], int64(idx), 10))
				w.WriteString(`</v></c>`)
			}
		}
		w.WriteString(`</row>`)
	}
	w.WriteString(`</sheetData></worksheet>`)
}

func writeShared(w *bytes.Buffer, ss []string) {
	n := strconv.Itoa(len(ss))
	w.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	w.WriteString(`<sst xmlns="` + nsMain + `" count="` + n + `" uniqueCount="` + n + `">`)
	for _, s := range ss {
		w.WriteString(`<si><t`)
		if len(s) > 0 && (s[0] == ' ' || s[len(s)-1] == ' ') {
			w.WriteString(` xml:space="preserve"`)
		}
		w.WriteByte('>')
		writeXMLEsc(w, s)
		w.WriteString(`</t></si>`)
	}
	w.WriteString(`</sst>`)
}

func writeStyles(w *bytes.Buffer, fonts []vfs.CellFormat, fills []string, borders []vfs.CellBorder, numFmts []string, numIDs []int, xfs []vfs.CellFormat) {
	w.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	w.WriteString(`<styleSheet xmlns="` + nsMain + `">`)
	if len(numFmts) > 0 {
		w.WriteString(`<numFmts count="` + strconv.Itoa(len(numFmts)) + `">`)
		for i, code := range numFmts {
			w.WriteString(`<numFmt numFmtId="` + strconv.Itoa(numIDs[i]) + `" formatCode="`)
			writeXMLEsc(w, code)
			w.WriteString(`"/>`)
		}
		w.WriteString(`</numFmts>`)
	}
	w.WriteString(`<fonts count="` + strconv.Itoa(len(fonts)) + `">`)
	for _, f := range fonts {
		w.WriteString(`<font>`)
		if f.Bold {
			w.WriteString(`<b/>`)
		}
		if f.Italic {
			w.WriteString(`<i/>`)
		}
		if f.Strike {
			w.WriteString(`<strike/>`)
		}
		if f.Underline {
			w.WriteString(`<u/>`)
		}
		if f.Color != "" {
			w.WriteString(`<color rgb="` + vfs.ExcelARGB(f.Color) + `"/>`)
		}
		w.WriteString(`<sz val="11"/><name val="Calibri"/></font>`)
	}
	w.WriteString(`</fonts>`)
	w.WriteString(`<fills count="` + strconv.Itoa(len(fills)) + `">`)
	for i, fill := range fills {
		switch {
		case i == 0:
			w.WriteString(`<fill><patternFill patternType="none"/></fill>`)
		case i == 1 && fill == "gray125":
			w.WriteString(`<fill><patternFill patternType="gray125"/></fill>`)
		case fill != "":
			w.WriteString(`<fill><patternFill patternType="solid"><fgColor rgb="` + vfs.ExcelARGB(fill) + `"/></patternFill></fill>`)
		default:
			w.WriteString(`<fill><patternFill patternType="none"/></fill>`)
		}
	}
	w.WriteString(`</fills>`)
	w.WriteString(`<borders count="` + strconv.Itoa(len(borders)) + `">`)
	for _, br := range borders {
		w.WriteString(`<border>`)
		writeBorderEdge(w, "left", br)
		writeBorderEdge(w, "right", br)
		writeBorderEdge(w, "top", br)
		writeBorderEdge(w, "bottom", br)
		w.WriteString(`<diagonal/></border>`)
	}
	w.WriteString(`</borders>`)
	w.WriteString(`<cellXfs count="` + strconv.Itoa(len(xfs)) + `">`)
	numIDsByXf := 0
	for i, xf := range xfs {
		numID, fontID, fillID, borderID := 0, 0, 0, 0
		if i > 0 {
			fontID = i
			fillID = i + 1
			if fillID >= len(fills) {
				fillID = 0
			}
			borderID = i
			if xf.Number != "" {
				numID = 164 + numIDsByXf
				numIDsByXf++
			}
		}
		w.WriteString(`<xf numFmtId="` + strconv.Itoa(numID) + `" fontId="` + strconv.Itoa(fontID) + `" fillId="` + strconv.Itoa(fillID) + `" borderId="` + strconv.Itoa(borderID) + `" xfId="0"`)
		if xf.Number != "" {
			w.WriteString(` applyNumberFormat="1"`)
		}
		if xf.Bold || xf.Italic || xf.Strike || xf.Underline || xf.Color != "" {
			w.WriteString(` applyFont="1"`)
		}
		if xf.Fill != "" {
			w.WriteString(` applyFill="1"`)
		}
		if xf.Border != nil {
			w.WriteString(` applyBorder="1"`)
		}
		if xf.Align != "" || xf.VAlign != "" || xf.Wrap != "" {
			w.WriteString(` applyAlignment="1"><alignment`)
			if xf.Align != "" {
				w.WriteString(` horizontal="`)
				writeXMLEsc(w, xf.Align)
				w.WriteString(`"`)
			}
			if xf.VAlign != "" {
				w.WriteString(` vertical="`)
				writeXMLEsc(w, xf.VAlign)
				w.WriteString(`"`)
			}
			if xf.Wrap == "wrap" {
				w.WriteString(` wrapText="1"`)
			}
			w.WriteString(`/></xf>`)
		} else {
			w.WriteString(`/>`)
		}
	}
	w.WriteString(`</cellXfs></styleSheet>`)
}

func writeBorderEdge(w *bytes.Buffer, name string, br vfs.CellBorder) {
	want := br.Style != "" && (br.Edges == "" || strings.Contains(br.Edges, name))
	if !want {
		w.WriteString(`<` + name + `/>`)
		return
	}
	style := br.Style
	if style == "" {
		style = "thin"
	}
	w.WriteString(`<` + name + ` style="` + style + `"`)
	if br.Color != "" {
		w.WriteString(`><color rgb="` + vfs.ExcelARGB(br.Color) + `"/></` + name + `>`)
		return
	}
	w.WriteString(`/>`)
}

func isNumericInput(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func writeXMLEsc(w io.Writer, s string) {
	_ = xml.EscapeText(w, []byte(s))
}
