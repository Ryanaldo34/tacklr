// Package adapters contains source-format codecs for rich text documents.
package adapters

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
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

func (DOCX) DecodeRich(ctx context.Context, path, _ string, data []byte) (*vfs.RichTextDocument, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	part, err := zipPart(data, "word/document.xml")
	if err != nil {
		return nil, fmt.Errorf("docx: %w", err)
	}
	return parseDOCX(part)
}

func (DOCX) EncodeRich(ctx context.Context, doc *vfs.RichTextDocument) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	parts := map[string]string{
		"[Content_Types].xml": contentTypesXML,
		"_rels/.rels":         relsXML,
		"word/document.xml":   docXML(doc),
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
		defer r.Close()
		return io.ReadAll(r)
	}
	return nil, fmt.Errorf("missing %s", name)
}

type docxParagraph struct {
	style string
	list  bool
	runs  []vfs.RichTextRun
}

func parseDOCX(data []byte) (*vfs.RichTextDocument, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	var paragraphs []docxParagraph
	var paragraph *docxParagraph
	var run *vfs.RichTextRun
	for {
		tok, err := dec.Token()
		if err == io.EOF {
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
					run = &vfs.RichTextRun{}
				}
			case "b", "i", "u", "strike":
				if run != nil {
					if run.Attributes == nil {
						run.Attributes = map[string]string{}
					}
					run.Attributes[t.Name.Local] = "true"
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
	blocks := make([]vfs.RichTextBlock, 0, len(paragraphs))
	for i, paragraph := range paragraphs {
		if len(paragraph.runs) == 0 {
			continue
		}
		kind, level := "paragraph", 0
		if strings.HasPrefix(paragraph.style, "Heading") {
			kind = "heading"
			level, _ = strconv.Atoi(strings.TrimPrefix(paragraph.style, "Heading"))
		}
		if paragraph.list {
			kind = "list-item"
		}
		blocks = append(blocks, vfs.RichTextBlock{ID: fmt.Sprintf("block-%06d", i+1), Kind: kind, Level: level, Runs: paragraph.runs})
	}
	return &vfs.RichTextDocument{Schema: vfs.RichTextSchema, Blocks: blocks}, nil
}

func attr(e xml.StartElement, name string) string {
	for _, a := range e.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

func docXML(doc *vfs.RichTextDocument) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`)
	for _, block := range doc.Blocks {
		b.WriteString("<w:p>")
		if block.Kind == "heading" && block.Level > 0 {
			b.WriteString(`<w:pPr><w:pStyle w:val="Heading` + strconv.Itoa(block.Level) + `"/></w:pPr>`)
		} else if block.Kind == "list-item" {
			b.WriteString(`<w:pPr><w:numPr><w:ilvl w:val="0"/><w:numId w:val="1"/></w:numPr></w:pPr>`)
		}
		for _, run := range block.Runs {
			b.WriteString("<w:r>")
			if len(run.Attributes) > 0 {
				b.WriteString("<w:rPr>")
				for _, mark := range []string{"b", "i", "u", "strike"} {
					if run.Attributes[mark] == "true" {
						b.WriteString("<w:" + mark + "/>")
					}
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
