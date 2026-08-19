// Package adapters contains source-format codecs for rich text documents.
package adapters

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs"
)

const DOCXMediaType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

// DOCX converts the main document part of a Word document. It intentionally
// operates on the portable WordprocessingML subset used by common editors.
type DOCX struct{}

func (DOCX) DecodeBlocks(ctx context.Context, path, _ string, data []byte) ([]vfs.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	part, err := zipPart(data, "word/document.xml")
	if err != nil {
		return nil, fmt.Errorf("docx: %w", err)
	}
	return parseDOCX(part)
}

func (DOCX) EncodeBlocks(ctx context.Context, blocks []vfs.Block) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	parts := map[string]string{
		"[Content_Types].xml": contentTypesXML,
		"_rels/.rels":         relsXML,
		"word/document.xml":   docXML(blocks),
	}
	for name, body := range parts {
		w, err := zw.Create(name)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(w, body); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func zipPart(data []byte, name string) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	for _, file := range zr.File {
		if file.Name != name {
			continue
		}
		r, err := file.Open()
		if err != nil {
			return nil, err
		}
		part, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		return part, nil
	}
	return nil, fmt.Errorf("missing %s", name)
}

type docxParagraph struct {
	style string
	list  bool
	runs  []vfs.Run
}

func parseDOCX(data []byte) ([]vfs.Block, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	var paragraphs []docxParagraph
	var paragraph *docxParagraph
	var run *vfs.Run
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse document.xml: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "p":
				paragraph = &docxParagraph{}
			case "pStyle":
				if paragraph != nil {
					paragraph.style = attr(t, "val")
				}
			case "numPr":
				if paragraph != nil {
					paragraph.list = true
				}
			case "r":
				if paragraph != nil {
					run = &vfs.Run{}
				}
			case "b", "i", "u", "strike":
				if run != nil {
					if run.Marks == nil {
						run.Marks = map[string]string{}
					}
					switch t.Name.Local {
					case "b":
						run.Marks[vfs.MarkBold] = "true"
					case "i":
						run.Marks[vfs.MarkItalic] = "true"
					case "strike":
						run.Marks[vfs.MarkStrike] = "true"
					}
				}
			case "t":
				if run == nil || paragraph == nil {
					continue
				}
				var text string
				if err := dec.DecodeElement(&text, &t); err != nil {
					return nil, err
				}
				run.Text += text
			case "tab":
				if run != nil {
					run.Text += "\t"
				}
			case "br", "cr":
				if run != nil {
					run.Text += "\n"
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "r":
				if paragraph != nil && run != nil && run.Text != "" {
					paragraph.runs = append(paragraph.runs, *run)
				}
				run = nil
			case "p":
				if paragraph != nil {
					paragraphs = append(paragraphs, *paragraph)
				}
				paragraph = nil
			}
		}
	}
	blocks := make([]vfs.Block, 0, len(paragraphs))
	for i, paragraph := range paragraphs {
		if len(paragraph.runs) == 0 {
			continue
		}
		kind, level := vfs.BlockKindParagraph, 0
		if strings.HasPrefix(paragraph.style, "Heading") {
			kind = vfs.BlockKindHeading
			level, _ = strconv.Atoi(strings.TrimPrefix(paragraph.style, "Heading"))
		}
		if paragraph.list {
			kind = vfs.BlockKindListItem
		}
		blocks = append(blocks, vfs.Block{
			ID:    fmt.Sprintf("block-%06d", i+1),
			Kind:  kind,
			Text:  vfs.FormatInline(paragraph.runs),
			Runs:  paragraph.runs,
			Style: vfs.StyleMeta{Level: level},
		})
	}
	return blocks, nil
}

func attr(e xml.StartElement, name string) string {
	for _, a := range e.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

func docXML(blocks []vfs.Block) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`)
	for _, block := range blocks {
		b.WriteString("<w:p>")
		if block.Kind == vfs.BlockKindHeading && block.Style.Level > 0 {
			b.WriteString(`<w:pPr><w:pStyle w:val="Heading` + strconv.Itoa(block.Style.Level) + `"/></w:pPr>`)
		} else if block.Kind == vfs.BlockKindListItem || block.Kind == "list-item" {
			b.WriteString(`<w:pPr><w:numPr><w:ilvl w:val="0"/><w:numId w:val="1"/></w:numPr></w:pPr>`)
		}
		runs := block.Runs
		if len(runs) == 0 {
			runs = vfs.ParseInline(block.Text)
		}
		for _, run := range runs {
			b.WriteString("<w:r>")
			if len(run.Marks) > 0 {
				b.WriteString("<w:rPr>")
				if run.Marks[vfs.MarkBold] == "true" {
					b.WriteString("<w:b/>")
				}
				if run.Marks[vfs.MarkItalic] == "true" {
					b.WriteString("<w:i/>")
				}
				if run.Marks[vfs.MarkStrike] == "true" {
					b.WriteString("<w:strike/>")
				}
				b.WriteString("</w:rPr>")
			}
			b.WriteString(`<w:t xml:space="preserve">`)
			_ = xml.EscapeText(&b, []byte(run.Text))
			b.WriteString("</w:t></w:r>")
		}
		b.WriteString("</w:p>")
	}
	b.WriteString(`</w:body></w:document>`)
	return b.String()
}

const contentTypesXML = `<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>`
const relsXML = `<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>`
