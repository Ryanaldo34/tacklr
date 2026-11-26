package vfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/google/docs"
	"github.com/ryanaldo34/tacklr/internal/rest"
)

// DocsAPI is the Docs subset used by the provider.
type DocsAPI interface {
	Get(ctx context.Context, documentID string) (DocsSnapshot, error)
}

// DocsSnapshot is the checkout used to build IR.
type DocsSnapshot struct {
	Title string
	Tabs  []DocTab
	Body  []DocsSpan
	Lists map[string]DocsListProps
}

// DocsListProps is list metadata from documents.get: the glyph type of each
// nesting level ("" where the level has none).
type DocsListProps struct {
	GlyphTypes []string
}

// DocsSpan is one IR-mapped span from a tab body.
type DocsSpan struct {
	TabID      string
	Kind       string
	ObjectID   string
	Level      int
	NamedStyle string
	ListID     string
	Nesting    int
	Text       string
}

func snapshotToRich(path string, snap DocsSnapshot) *IR {
	blocks := make([]Block, 0, len(snap.Body))
	for _, sp := range snap.Body {
		blocks = append(blocks, spanToBlock(sp, snap.Lists))
	}
	return newRichDocument(path, mimeGoogleDocument, blocks, snap.Tabs)
}

func spanToBlock(sp DocsSpan, lists map[string]DocsListProps) Block {
	attrs := map[string]string{}
	if sp.TabID != "" {
		attrs["tab_id"] = sp.TabID
	}
	b := Block{Kind: sp.Kind, Text: sp.Text, Style: StyleMeta{Level: sp.Level, Attributes: attrs}}
	switch sp.Kind {
	case "heading":
		b.Kind = BlockKindHeading
	case "list_item":
		b.Kind = BlockKindListItem
		attrs["list_id"] = sp.ListID
		lt := "ul"
		// A level is numbered by its own glyph, or by the list's first level.
		if g := lists[sp.ListID].GlyphTypes; (sp.Nesting < len(g) && glyphOrdered(g[sp.Nesting])) || (len(g) > 0 && glyphOrdered(g[0])) {
			lt = "ol"
		}
		attrs["list_type"] = lt
	case "table":
		b.Kind = BlockKindTable
		grid, _ := parseTSV(sp.Text)
		rows, cols := len(grid), 0
		if rows > 0 {
			cols = len(grid[0])
		}
		attrs["rows"] = strconv.Itoa(rows)
		attrs["cols"] = strconv.Itoa(cols)
	case "image":
		b.Kind = BlockKindImage
		attrs["object_id"] = sp.ObjectID
		if sp.NamedStyle != "" && (strings.HasPrefix(sp.NamedStyle, "http://") || strings.HasPrefix(sp.NamedStyle, "https://")) {
			attrs["content_uri"] = sp.NamedStyle
		}
		b.Text = sp.Text
	default:
		b.Kind = BlockKindParagraph
	}
	return b
}

func glyphOrdered(glyphType string) bool {
	switch glyphType {
	case "DECIMAL", "ZERO_DECIMAL", "UPPER_ALPHA", "ALPHA", "UPPER_ROMAN", "ROMAN",
		"UPPERALPHA", "UPPERROMAN":
		return true
	default:
		u := strings.ToUpper(glyphType)
		return strings.Contains(u, "DECIMAL") ||
			strings.Contains(u, "ALPHA") ||
			strings.Contains(u, "ROMAN")
	}
}

// googleDocs implements DocsAPI with the Docs v1 REST API.
type googleDocs struct {
	c rest.Client
}

// NewGoogleDocs returns a DocsAPI that authenticates with holder
// (required). base is the API root ("" for Google's); httpClient nil is
// http.DefaultClient.
func NewGoogleDocs(holder *TokenHolder, base string, httpClient *http.Client) DocsAPI {
	return googleDocs{c: apiClient(holder, base, "https://docs.googleapis.com/", httpClient)}
}

// Get implements DocsAPI. Always sets includeTabsContent=true.
func (g googleDocs) Get(ctx context.Context, documentID string) (DocsSnapshot, error) {
	var d docs.Document
	if err := g.c.Call(ctx, http.MethodGet, "v1/documents/"+url.PathEscape(documentID), url.Values{"includeTabsContent": {"true"}}, nil, &d); err != nil {
		return DocsSnapshot{}, mapDocsError(err)
	}
	return snapshotFromDocument(&d), nil
}

func mapDocsError(err error) error {
	gerr, ok := errors.AsType[*rest.Error](err)
	if !ok {
		return err
	}
	switch gerr.Code {
	case 401:
		return ErrAuthExpired
	case 404:
		return ErrNotExist
	case 403:
		if gerr.Message == "" {
			return ErrPermission
		}
		return fmt.Errorf("%w: %s", ErrPermission, gerr.Message)
	default:
		return fmt.Errorf("vfs: docs %w", gerr)
	}
}

type tabBody struct {
	ID    string
	Title string
	Index int
	Body  *docs.Body
	Lists map[string]docs.List
	Objs  map[string]docs.InlineObject
}

func allDocumentTabs(d *docs.Document) []tabBody {
	if len(d.Tabs) == 0 {
		if d.Body != nil {
			return []tabBody{{Body: d.Body, Lists: d.Lists, Objs: d.InlineObjects}}
		}
		return nil
	}
	var out []tabBody
	var walk func([]*docs.Tab)
	walk = func(ts []*docs.Tab) {
		for _, t := range ts {
			if t == nil {
				continue
			}
			if t.DocumentTab != nil && t.DocumentTab.Body != nil {
				id, title, idx := "", "", 0
				if t.TabProperties != nil {
					id, title = t.TabProperties.TabID, t.TabProperties.Title
					idx = int(t.TabProperties.Index)
				}
				out = append(out, tabBody{
					ID: id, Title: title, Index: idx,
					Body: t.DocumentTab.Body, Lists: t.DocumentTab.Lists, Objs: t.DocumentTab.InlineObjects,
				})
			}
			walk(t.ChildTabs)
		}
	}
	walk(d.Tabs)
	return out
}

func snapshotFromDocument(d *docs.Document) DocsSnapshot {
	snap := DocsSnapshot{
		Title: d.Title,
		Lists: map[string]DocsListProps{},
	}
	tabs := allDocumentTabs(d)
	for _, tb := range tabs {
		if tb.ID != "" || tb.Title != "" {
			snap.Tabs = append(snap.Tabs, DocTab{ID: tb.ID, Title: tb.Title, Index: tb.Index})
		}
		mergeListProps(snap.Lists, tb.Lists)
		if tb.Lists == nil {
			tb.Lists = d.Lists
		}
		if tb.Objs == nil {
			tb.Objs = d.InlineObjects
		}
		snap.Body = append(snap.Body, walkBodySpans(tb)...)
	}
	return snap
}

func mergeListProps(dst map[string]DocsListProps, src map[string]docs.List) {
	for id, l := range src {
		p := DocsListProps{}
		if l.ListProperties != nil {
			for i, nl := range l.ListProperties.NestingLevels {
				if nl == nil {
					continue
				}
				for len(p.GlyphTypes) <= i {
					p.GlyphTypes = append(p.GlyphTypes, "")
				}
				p.GlyphTypes[i] = nl.GlyphType
			}
		}
		dst[id] = p
	}
}

// walkBodySpans is the spans of tb's body, which allDocumentTabs never
// leaves nil.
func walkBodySpans(tb tabBody) []DocsSpan {
	out := make([]DocsSpan, 0, len(tb.Body.Content))
	for _, el := range tb.Body.Content {
		if el == nil {
			continue
		}
		out = append(out, structuralToSpans(el, tb)...)
	}
	return out
}

func structuralToSpans(el *docs.StructuralElement, tb tabBody) []DocsSpan {
	switch {
	case el.Table != nil:
		return []DocsSpan{tableSpan(el, tb)}
	case el.Paragraph != nil:
		return paragraphSpans(el, tb)
	default:
		return nil
	}
}

func tableSpan(el *docs.StructuralElement, tb tabBody) DocsSpan {
	t := el.Table
	span := DocsSpan{TabID: tb.ID, Kind: "table"}
	var rows [][]string
	for r, row := range t.TableRows {
		if row == nil {
			continue
		}
		for c, cell := range row.TableCells {
			if cell == nil {
				continue
			}
			text := firstCellParagraph(cell)
			for len(rows) <= r {
				rows = append(rows, nil)
			}
			for len(rows[r]) <= c {
				rows[r] = append(rows[r], "")
			}
			rows[r][c] = sanitizeCell(text)
		}
	}
	span.Text = encodeTSV(rows)
	return span
}

func firstCellParagraph(cell *docs.TableCell) string {
	for _, el := range cell.Content {
		if el == nil || el.Paragraph == nil {
			continue
		}
		return paragraphText(el.Paragraph)
	}
	return ""
}

func paragraphSpans(el *docs.StructuralElement, tb tabBody) []DocsSpan {
	p := el.Paragraph
	text := paragraphText(p)
	var images []DocsSpan
	onlyImage := true
	for _, e := range p.Elements {
		if e == nil {
			continue
		}
		if e.InlineObjectElement != nil {
			oid := e.InlineObjectElement.InlineObjectID
			img := DocsSpan{TabID: tb.ID, Kind: "image", ObjectID: oid}
			if obj, ok := tb.Objs[oid]; ok && obj.InlineObjectProperties != nil &&
				obj.InlineObjectProperties.EmbeddedObject != nil &&
				obj.InlineObjectProperties.EmbeddedObject.ImageProperties != nil {
				img.Text = obj.InlineObjectProperties.EmbeddedObject.Title
				img.NamedStyle = obj.InlineObjectProperties.EmbeddedObject.ImageProperties.ContentURI
			}
			images = append(images, img)
			continue
		}
		if e.TextRun != nil && strings.TrimSpace(strings.TrimSuffix(e.TextRun.Content, "\n")) != "" {
			onlyImage = false
		} else if e.TextRun == nil && e.InlineObjectElement == nil {
			onlyImage = false
		}
	}
	if onlyImage && len(images) > 0 && strings.TrimSpace(text) == "" {
		return images
	}
	span := DocsSpan{TabID: tb.ID, Text: text}
	named := ""
	if p.ParagraphStyle != nil {
		named = p.ParagraphStyle.NamedStyleType
	}
	span.NamedStyle = named
	switch {
	case p.Bullet != nil:
		span.Kind = "list_item"
		span.ListID = p.Bullet.ListID
		span.Nesting = int(p.Bullet.NestingLevel)
		span.Level = span.Nesting + 1
	case strings.HasPrefix(named, "HEADING_"):
		span.Kind = "heading"
		n, _ := strconv.Atoi(strings.TrimPrefix(named, "HEADING_"))
		if n < 1 {
			n = 1
		}
		if n > 6 {
			n = 6
		}
		span.Level = n
	default:
		span.Kind = "paragraph"
	}
	out := []DocsSpan{span}
	return append(out, images...)
}

func paragraphText(p *docs.Paragraph) string {
	heading := p.ParagraphStyle != nil && strings.HasPrefix(p.ParagraphStyle.NamedStyleType, "HEADING_")
	var runs []Run
	for _, e := range p.Elements {
		if e == nil || e.TextRun == nil {
			continue
		}
		text := strings.TrimSuffix(e.TextRun.Content, "\n")
		if text == "" {
			continue
		}
		marks := map[string]string{}
		if st := e.TextRun.TextStyle; st != nil {
			if st.Bold && !heading {
				marks[MarkBold] = "true"
			}
			if st.Italic {
				marks[MarkItalic] = "true"
			}
			if st.Strikethrough {
				marks[MarkStrike] = "true"
			}
			if st.Link != nil && st.Link.URL != "" {
				marks[MarkHref] = st.Link.URL
			}
		}
		runs = append(runs, Run{Text: text, Marks: marks})
	}
	if len(runs) == 0 {
		return ""
	}
	return FormatInline(mergeRuns(runs))
}
