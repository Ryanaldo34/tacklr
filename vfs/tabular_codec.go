package vfs

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// SheetNormalizer converts a workbook format to and from []Sheet.
type SheetNormalizer interface {
	DecodeSheets(ctx context.Context, path, mediaType string, data []byte) ([]Sheet, []NamedRange, error)
	EncodeSheets(ctx context.Context, sheets []Sheet, named []NamedRange) ([]byte, error)
}

// TabularCodec adapts a SheetNormalizer to the VFS codec registry.
// Decode yields a grid checkout. Encode writes native bytes (xlsx, …).
type TabularCodec struct {
	Types      []string
	Normalizer SheetNormalizer
}

func (c TabularCodec) MediaTypes() []string { return c.Types }

func (c TabularCodec) Decode(ctx context.Context, virtualPath, mediaType string, data []byte) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Normalizer == nil {
		return nil, fmt.Errorf("vfs: sheet normalizer required")
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	if mediaType == "" && len(c.Types) > 0 {
		mediaType = c.Types[0]
	}
	sheets, named, err := c.Normalizer.DecodeSheets(ctx, virtualPath, mediaType, data)
	if err != nil {
		return nil, err
	}
	return adoptTabularDocument(virtualPath, mediaType, sheets, named)
}

func (c TabularCodec) Encode(ctx context.Context, doc Document) ([]byte, error) {
	if c.Normalizer == nil {
		return nil, fmt.Errorf("vfs: sheet normalizer required")
	}
	g, ok := AsGrid(doc)
	if !ok {
		return nil, ErrNotTextual
	}
	return c.Normalizer.EncodeSheets(ctx, g.Sheets(), g.NamedRanges())
}

func (c TabularCodec) Create(virtualPath, mediaType string, mut Mutation) (Document, error) {
	if mediaType == "" && len(c.Types) > 0 {
		mediaType = c.Types[0]
	}
	title := strings.TrimSuffix(path.Base(virtualPath), path.Ext(virtualPath))
	if title == "" || title == "." {
		title = "Sheet1"
	}
	var sheets []Sheet
	if mut.Blocks != nil {
		sheets = sheetsFromBlocks(mut.Blocks, title)
	} else {
		body := ""
		if mut.Content != nil {
			body = *mut.Content
		}
		if looksLikeHTML(body) {
			return nil, fmt.Errorf("vfs: HTML content is not accepted; use blocks")
		}
		sheets = liftTabular(title, body)
	}
	return adoptTabularDocument(virtualPath, mediaType, sheets, nil)
}

// SheetsHTML decodes Google Sheets HTML ZIP export (Drive RO).
type SheetsHTML struct{}

func (SheetsHTML) DecodeSheets(_ context.Context, _, _ string, data []byte) ([]Sheet, []NamedRange, error) {
	sheets, err := decodeSheetsHTML(data)
	if err != nil {
		return nil, nil, err
	}
	return sheets, nil, nil
}

func (SheetsHTML) EncodeSheets(context.Context, []Sheet, []NamedRange) ([]byte, error) {
	return nil, ErrNotSupported
}

func sheetsTabularCodec() TabularCodec {
	return TabularCodec{Types: []string{mimeGoogleSpreadsheet}, Normalizer: SheetsHTML{}}
}

// SheetsCodec is the default-registry codec for Google Sheets HTML export.
type SheetsCodec struct{}

func (SheetsCodec) MediaTypes() []string {
	return []string{mimeGoogleSpreadsheet}
}

func (SheetsCodec) Decode(ctx context.Context, virtualPath, mediaType string, data []byte) (Document, error) {
	return sheetsTabularCodec().Decode(ctx, virtualPath, mediaType, data)
}

func (SheetsCodec) Create(path, mediaType string, mut Mutation) (Document, error) {
	return sheetsTabularCodec().Create(path, mediaType, mut)
}

func decodeSheetsHTML(data []byte) ([]Sheet, error) {
	if looksLikeZip(data) {
		files, err := unzipHTMLFiles(data)
		if err != nil {
			return nil, err
		}
		var out []Sheet
		for _, f := range files {
			if !utf8.Valid(f.body) {
				return nil, ErrInvalidUTF8
			}
			part, err := parseSheetsHTML(f.body, f.title)
			if err != nil {
				return nil, err
			}
			out = append(out, part...)
		}
		return out, nil
	}
	if !utf8.Valid(data) {
		return nil, ErrInvalidUTF8
	}
	return parseSheetsHTML(data, "")
}

type htmlFile struct {
	title string
	body  []byte
}

func unzipHTMLFiles(data []byte) ([]htmlFile, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	var all, skipIndex []htmlFile
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		base := path.Base(f.Name)
		lower := strings.ToLower(base)
		if !strings.HasSuffix(lower, ".html") && !strings.HasSuffix(lower, ".htm") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		body, err := readCapped(rc, MaxReadFileBytes, zipSizeHint(f.UncompressedSize64, MaxReadFileBytes))
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		if len(body) > MaxReadFileBytes {
			return nil, errFileExceeds(MaxReadFileBytes)
		}
		title := strings.TrimSuffix(base, path.Ext(base))
		hf := htmlFile{title: title, body: body}
		all = append(all, hf)
		if !strings.EqualFold(base, "index.html") {
			skipIndex = append(skipIndex, hf)
		}
	}
	if len(skipIndex) > 0 {
		return skipIndex, nil
	}
	if len(all) == 0 {
		return nil, ErrNotSupported
	}
	return all, nil
}

func parseSheetsHTML(raw []byte, fallback string) ([]Sheet, error) {
	doc, err := html.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	body := findHTMLBody(doc)
	if body == nil {
		body = doc
	}
	tables := collectHTMLTables(body)
	if len(tables) == 0 {
		title := fallback
		if title == "" {
			title = firstHTMLTitle(doc)
		}
		if title == "" {
			title = "Sheet1"
		}
		return []Sheet{{Title: title}}, nil
	}
	if fallback == "" {
		fallback = firstHTMLTitle(doc)
	}
	out := make([]Sheet, 0, len(tables))
	for i, grid := range tables {
		title := fallback
		if title == "" || i > 0 {
			if i > 0 || title == "" {
				title = "Sheet" + strconv.Itoa(i+1)
			}
		}
		if i == 0 && fallback != "" {
			title = fallback
		}
		sh := Sheet{Title: title, Cells: cellsFromStrings(grid)}
		trimSheet(&sh)
		out = append(out, sh)
	}
	return out, nil
}

func collectHTMLTables(n *html.Node) [][][]string {
	var out [][][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, "table") {
			if grid, ok := tableGrid(n); ok {
				out = append(out, grid)
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

func tableGrid(n *html.Node) ([][]string, bool) {
	var rows [][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, "tr") {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (strings.EqualFold(c.Data, "td") || strings.EqualFold(c.Data, "th")) {
					cells = append(cells, cellHTMLText(c))
				}
			}
			if len(cells) > 0 {
				rows = append(rows, cells)
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode {
				walk(c)
			}
		}
	}
	walk(n)
	if len(rows) == 0 {
		return nil, false
	}
	return padStringGrid(rows), true
}

func cellHTMLText(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n == nil {
			return
		}
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
			return
		}
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, "br") {
			b.WriteByte('\n')
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return strings.TrimSpace(b.String())
}

func firstHTMLTitle(n *html.Node) string {
	if n == nil {
		return ""
	}
	if n.Type == html.ElementNode && strings.EqualFold(n.Data, "title") {
		return strings.TrimSpace(FormatInline(collectInline(n, nil)))
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if t := firstHTMLTitle(c); t != "" {
			return t
		}
	}
	return ""
}

func liftTabular(title, body string) []Sheet {
	if title == "" {
		title = "Sheet1"
	}
	grid := parseDelimited(body)
	sh := Sheet{Title: title, Cells: cellsFromStrings(grid)}
	trimSheet(&sh)
	return []Sheet{sh}
}

func parseDelimited(body string) [][]string {
	body = strings.TrimRight(body, "\r\n")
	if body == "" {
		return nil
	}
	comma := ','
	if strings.Contains(body, "\t") {
		comma = '\t'
	}
	r := csv.NewReader(strings.NewReader(body))
	r.Comma = comma
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return parseToolTSV(body)
	}
	return padStringGrid(rows)
}

func sheetsFromBlocks(blocks []Block, fallback string) []Sheet {
	out := make([]Sheet, 0, len(blocks))
	for i, b := range blocks {
		if b.Kind != "" && b.Kind != BlockKindSheet {
			continue
		}
		title := blockAttr(b, "title")
		if title == "" {
			title = b.ID
		}
		if title == "" {
			title = fallback
		}
		if title == "" {
			title = "Sheet" + strconv.Itoa(i+1)
		}
		sh := Sheet{
			ID:    blockAttr(b, "sheet_id"),
			Title: title,
			Cells: cellsFromStrings(parseToolTSV(b.Text)),
		}
		trimSheet(&sh)
		out = append(out, sh)
	}
	if len(out) == 0 {
		if fallback == "" {
			fallback = "Sheet1"
		}
		out = []Sheet{{Title: fallback}}
	}
	return out
}
