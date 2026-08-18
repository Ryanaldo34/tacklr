package vfs

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// DocsCodec decodes Google Docs HTML (unzipped export or canonical projection)
// into *RichDocument. It does not implement IdentityCodec.
type DocsCodec struct{}

// MediaTypes implements Codec.
func (DocsCodec) MediaTypes() []string {
	return []string{mimeGoogleDocument}
}

// Decode requires valid UTF-8 and rejects payloads larger than MaxReadFileBytes.
func (DocsCodec) Decode(ctx context.Context, virtualPath, mediaType string, data []byte) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	if mediaType == "" {
		mediaType = mimeGoogleDocument
	}
	htmlBytes := data
	if looksLikeZip(data) {
		raw, err := unzipHTML(data)
		if err != nil {
			return nil, err
		}
		htmlBytes = raw
	}
	if !utf8.Valid(htmlBytes) {
		return nil, ErrInvalidUTF8
	}
	blocks, err := decodeDocsHTML(htmlBytes)
	if err != nil {
		return nil, err
	}
	return NewRichDocument(virtualPath, mediaType, blocks), nil
}

func looksLikeZip(data []byte) bool {
	return len(data) >= 2 && data[0] == 'P' && data[1] == 'K'
}

func unzipHTML(data []byte) ([]byte, error) {
	if looksLikeZip(data) {
		return unzipHTMLEntry(data)
	}
	if len(data) > 0 && data[0] == '<' {
		return data, nil
	}
	return nil, ErrNotSupported
}

func unzipHTMLEntry(data []byte) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotSupported, err)
	}
	var index, rootHTML, anyHTML *zip.File
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := f.Name
		base := path.Base(name)
		if strings.EqualFold(base, "index.html") {
			index = f
			break
		}
		if strings.HasSuffix(strings.ToLower(base), ".html") {
			if anyHTML == nil {
				anyHTML = f
			}
			if !strings.Contains(name, "/") {
				rootHTML = f
			}
		}
	}
	pick := index
	if pick == nil {
		pick = rootHTML
	}
	if pick == nil {
		pick = anyHTML
	}
	if pick == nil {
		return nil, ErrNotSupported
	}
	rc, err := pick.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(io.LimitReader(rc, int64(MaxReadFileBytes)+1))
}

func decodeDocsHTML(raw []byte) ([]Block, error) {
	doc, err := html.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	body := findHTMLBody(doc)
	if body == nil {
		body = doc
	}
	var dec htmlDecoder
	dec.walk(body, 0, "")
	return dec.blocks, nil
}

type htmlDecoder struct {
	blocks  []Block
	listSeq int
	imgSeq  int
}

func (d *htmlDecoder) walk(n *html.Node, listDepth int, listID string) {
	if n == nil {
		return
	}
	if n.Type == html.ElementNode {
		switch strings.ToLower(n.Data) {
		case "script":
			return
		case "style", "head":
			return
		case "h1", "h2", "h3", "h4", "h5", "h6":
			if hasClass(n, "tacklr-tab") {
				return
			}
			level := int(n.Data[1] - '0')
			text := strings.TrimSpace(innerText(n))
			if text != "" {
				d.blocks = append(d.blocks, Block{
					Kind: BlockKindHeading,
					Text: text,
					Style: StyleMeta{
						Level:      level,
						Attributes: map[string]string{},
					},
				})
			}
			return
		case "p", "div":
			full, skipImgs, imgs := collectParagraph(n)
			if len(imgs) > 0 && strings.TrimSpace(skipImgs) == "" {
				d.blocks = append(d.blocks, imgs...)
				return
			}
			if text := strings.TrimSpace(full); text != "" {
				d.blocks = append(d.blocks, Block{
					Kind:  BlockKindParagraph,
					Text:  text,
					Style: StyleMeta{Attributes: map[string]string{}},
				})
			}
			d.blocks = append(d.blocks, imgs...)
			return
		case "li":
			if listID == "" {
				d.listSeq++
				listID = "l" + strconv.Itoa(d.listSeq)
			}
			level := listDepth
			if level < 1 {
				level = 1
			}
			listType := "ul"
			if p := n.Parent; p != nil && strings.EqualFold(p.Data, "ol") {
				listType = "ol"
			}
			text := strings.TrimSpace(directItemText(n))
			d.blocks = append(d.blocks, Block{
				Kind: BlockKindListItem,
				Text: text,
				Style: StyleMeta{
					Level: level,
					Attributes: map[string]string{
						"list_type": listType,
						"list_id":   listID,
					},
				},
			})
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode {
					switch strings.ToLower(c.Data) {
					case "ul", "ol":
						d.walk(c, level+1, listID)
					}
				}
			}
			return
		case "ul", "ol":
			id := listID
			if listDepth == 0 {
				d.listSeq++
				id = "l" + strconv.Itoa(d.listSeq)
			}
			depth := listDepth
			if depth < 1 {
				depth = 1
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				d.walk(c, depth, id)
			}
			return
		case "table":
			if t, ok := decodeHTMLTable(n); ok {
				d.blocks = append(d.blocks, t)
			}
			return
		case "figure":
			if img, ok := decodeHTMLImage(n, &d.imgSeq); ok {
				d.blocks = append(d.blocks, img)
			}
			return
		case "img":
			if img, ok := decodeHTMLImage(n, &d.imgSeq); ok {
				d.blocks = append(d.blocks, img)
			}
			return
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		d.walk(c, listDepth, listID)
	}
}

func findHTMLBody(n *html.Node) *html.Node {
	if n == nil {
		return nil
	}
	if n.Type == html.ElementNode && strings.EqualFold(n.Data, "body") {
		return n
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if found := findHTMLBody(c); found != nil {
			return found
		}
	}
	return nil
}

func hasClass(n *html.Node, class string) bool {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, "class") {
			for _, c := range strings.Fields(a.Val) {
				if c == class {
					return true
				}
			}
		}
	}
	return false
}

func attrVal(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

func innerText(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode {
			switch strings.ToLower(n.Data) {
			case "script", "style":
				return
			}
		}
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return b.String()
}

func collectParagraph(n *html.Node) (full, skipImgs string, imgs []Block) {
	var fullB, skipB strings.Builder
	seq := 0
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, inSkip bool) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode {
			switch strings.ToLower(n.Data) {
			case "script", "style":
				return
			case "img", "figure":
				if img, ok := decodeHTMLImage(n, &seq); ok {
					imgs = append(imgs, img)
				}
				if strings.EqualFold(n.Data, "figure") {
					for c := n.FirstChild; c != nil; c = c.NextSibling {
						if c.Type == html.ElementNode {
							switch strings.ToLower(c.Data) {
							case "img", "figure":
								continue
							}
						}
						walk(c, true)
					}
				}
				return
			}
		}
		if n.Type == html.TextNode {
			fullB.WriteString(n.Data)
			if !inSkip {
				skipB.WriteString(n.Data)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, inSkip)
		}
	}
	walk(n, false)
	return fullB.String(), skipB.String(), imgs
}

func directItemText(n *html.Node) string {
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode {
			switch strings.ToLower(c.Data) {
			case "ul", "ol":
				continue
			}
		}
		if c.Type == html.TextNode {
			b.WriteString(c.Data)
			continue
		}
		b.WriteString(innerText(c))
	}
	return b.String()
}

func decodeHTMLImage(n *html.Node, seq *int) (Block, bool) {
	oid := attrVal(n, "data-object-id")
	src := attrVal(n, "src")
	alt := attrVal(n, "alt")
	if strings.EqualFold(n.Data, "figure") {
		if oid == "" {
			oid = attrVal(n, "data-object-id")
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode && strings.EqualFold(c.Data, "img") {
				if src == "" {
					src = attrVal(c, "src")
				}
				if alt == "" {
					alt = attrVal(c, "alt")
				}
				if oid == "" {
					oid = attrVal(c, "data-object-id")
				}
			}
		}
	}
	if oid == "" && src == "" && strings.EqualFold(n.Data, "figure") && n.FirstChild == nil {
		*seq++
		oid = "img-html-" + strconv.Itoa(*seq)
	}
	if oid == "" && src == "" {
		return Block{}, false
	}
	if oid == "" {
		*seq++
		oid = "img-html-" + strconv.Itoa(*seq)
	}
	attrs := map[string]string{"object_id": oid}
	if src != "" && (strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://")) {
		attrs["content_uri"] = src
	}
	return Block{Kind: BlockKindImage, Text: alt, Style: StyleMeta{Attributes: attrs}}, true
}

func decodeHTMLTable(n *html.Node) (Block, bool) {
	var rows [][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, "tr") {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (strings.EqualFold(c.Data, "td") || strings.EqualFold(c.Data, "th")) {
					cells = append(cells, sanitizeCell(strings.TrimSpace(innerText(c))))
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
		return Block{}, false
	}
	cols := 0
	for _, r := range rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	for i := range rows {
		for len(rows[i]) < cols {
			rows[i] = append(rows[i], "")
		}
	}
	return Block{
		Kind: BlockKindTable,
		Text: encodeTSV(rows),
		Style: StyleMeta{
			Attributes: map[string]string{
				"rows": strconv.Itoa(len(rows)),
				"cols": strconv.Itoa(cols),
			},
		},
	}, true
}
