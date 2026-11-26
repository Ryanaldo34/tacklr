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

// XLSX is the vfs.SheetNormalizer of Excel workbooks: it maps workbook.xml,
// sheetN.xml, sharedStrings.xml and styles.xml to sheets. Codecs registers
// it in a vfs.TabularCodec.
type XLSX struct{}

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
	// The zip reader fails a part that is longer than its header size, so
	// the header check bounds the read.
	return fn(rc)
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
					// A styled border has at least one edge; four edges are all.
					if strings.Count(b.Edges, ",") >= 3 {
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
			// Character data outside a cell is skipped, and a cell start
			// resets these, so they need no cell check.
			case "f":
				cur.inF = true
			case "v":
				cur.inV = true
			case "is":
				cur.inIS = true
			case "t":
				cur.inT = cur.inIS
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
	// A placed cell has a column, so maxC is at least 1.
	if maxR > vfs.MaxSheetCells/maxC {
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
