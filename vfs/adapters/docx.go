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
	"slices"
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

func zipPart(data []byte, name string) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(zr.File, func(f *zip.File) bool { return f.Name == name })
	if i < 0 {
		return nil, fmt.Errorf("missing %s", name)
	}
	r, err := zr.File[i].Open()
	if err != nil {
		return nil, err
	}
	// Close of a part reader only repeats a read error.
	defer r.Close()
	return io.ReadAll(r)
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
				// A run exists only in a paragraph.
				if run == nil {
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
