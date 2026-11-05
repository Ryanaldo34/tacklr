package vfs

import (
	"cmp"
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
	BatchUpdate(ctx context.Context, documentID string, req DocsBatch) (DocsBatchResult, error)
}

// DocsSnapshot is the checkout used to build IR and persistHint.
type DocsSnapshot struct {
	DocumentID string
	RevisionID string
	Title      string
	Tabs       []DocTab
	Body       []DocsSpan
	Lists      map[string]DocsListProps
}

// DocsListProps is list metadata from documents.get: the glyph type of each
// nesting level ("" where the level has none).
type DocsListProps struct {
	GlyphTypes []string
}

// DocsSpan is one IR-mapped (or structural) span from a tab body.
type DocsSpan struct {
	TabID      string
	StartIndex int
	EndIndex   int
	Kind       string
	ObjectID   string
	Level      int
	NamedStyle string
	ListID     string
	Nesting    int
	Text       string
	Cells      []DocsCell
}

// DocsCell is one table cell's first paragraph.
type DocsCell struct {
	Row, Col             int
	StartIndex, EndIndex int
	Text                 string
}

// DocsBatch is one documents.batchUpdate call.
type DocsBatch struct {
	RequiredRevisionID string
	TabID              string
	Requests           []DocsRequest
}

// DocsRequest is one batchUpdate request. Exactly one field is set; the
// field names are the Docs API request names.
type DocsRequest struct {
	DeleteContentRange     *DocsRange
	InsertText             *DocsInsertText
	UpdateParagraphStyle   *DocsParagraphStyle
	CreateParagraphBullets *DocsBullets
	InsertTable            *DocsInsertTable
	UpdateTextStyle        *DocsTextStyle
}

// DocsRange is the Docs body indexes [StartIndex, EndIndex) of one tab. An
// empty TabID is the batch's tab.
type DocsRange struct {
	StartIndex, EndIndex int
	TabID                string
}

// DocsLocation is one Docs body index of one tab. An empty TabID is the
// batch's tab.
type DocsLocation struct {
	Index int
	TabID string
}

// DocsInsertText inserts Text at Location.
type DocsInsertText struct {
	Location DocsLocation
	Text     string
}

// DocsParagraphStyle sets the named style (NORMAL_TEXT, HEADING_1, …) of the
// paragraphs in Range.
type DocsParagraphStyle struct {
	Range          DocsRange
	NamedStyleType string
}

// DocsBullets makes the paragraphs in Range a list with BulletPreset.
type DocsBullets struct {
	Range        DocsRange
	BulletPreset string
}

// DocsInsertTable inserts an empty Rows × Columns table at Location.
type DocsInsertTable struct {
	Location      DocsLocation
	Rows, Columns int
}

// DocsTextStyle sets bold, italic, strikethrough and the link of the text in
// Range. An empty Link removes the link.
type DocsTextStyle struct {
	Range                       DocsRange
	Bold, Italic, Strikethrough bool
	Link                        string
}

// DocsBatchResult is writeControl.requiredRevisionId after apply.
type DocsBatchResult struct {
	RevisionID string
}

func snapshotToRich(path string, snap DocsSnapshot) *IR {
	blocks := make([]Block, 0, len(snap.Body))
	locs := make([]blockLocation, 0, len(snap.Body))
	var structural []structuralSpan
	for _, sp := range snap.Body {
		switch sp.Kind {
		case "sectionBreak", "tableOfContents":
			structural = append(structural, structuralSpan{
				tabID: sp.TabID, startIndex: sp.StartIndex, endIndex: sp.EndIndex, kind: sp.Kind,
			})
			continue
		}
		b := spanToBlock(sp, snap.Lists)
		blocks = append(blocks, b)
		locs = append(locs, spanToLocation(sp))
	}
	d := newRichDocument(path, mimeGoogleDocument, blocks, snap.Tabs)
	if rb, ok := asRichBody(d); ok {
		for i := range locs {
			if i < len(rb.tree) {
				locs[i].id = rb.tree[i].ID
			}
		}
	}
	attachPersistHint(d, persistHint{
		fileID: snap.DocumentID, revisionID: snap.RevisionID,
		locations: locs, structural: structural,
	})
	return d
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

func spanToLocation(sp DocsSpan) blockLocation {
	loc := blockLocation{
		tabID: sp.TabID, startIndex: sp.StartIndex, endIndex: sp.EndIndex,
		kind: sp.Kind, objectID: sp.ObjectID,
	}
	for _, c := range sp.Cells {
		loc.cells = append(loc.cells, cellLocation{
			row: c.Row, col: c.Col, startIndex: c.StartIndex, endIndex: c.EndIndex,
		})
	}
	return loc
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

// BatchUpdate implements DocsAPI. Returns only writeControl.requiredRevisionId.
func (g googleDocs) BatchUpdate(ctx context.Context, documentID string, req DocsBatch) (DocsBatchResult, error) {
	call := docs.BatchUpdateDocumentRequest{Requests: make([]*docs.Request, 0, len(req.Requests))}
	for i := range req.Requests {
		call.Requests = append(call.Requests, docsRequestToAPI(req.Requests[i], req.TabID))
	}
	if req.RequiredRevisionID != "" {
		call.WriteControl = &docs.WriteControl{RequiredRevisionID: req.RequiredRevisionID}
	}
	var resp docs.BatchUpdateDocumentResponse
	if err := g.c.Call(ctx, http.MethodPost, "v1/documents/"+url.PathEscape(documentID)+":batchUpdate", nil, call, &resp); err != nil {
		return DocsBatchResult{}, mapDocsError(err)
	}
	rev := ""
	if resp.WriteControl != nil {
		rev = resp.WriteControl.RequiredRevisionID
	}
	return DocsBatchResult{RevisionID: rev}, nil
}

// docsRequestToAPI is r as a Docs API request, on tabID where r names no
// tab.
func docsRequestToAPI(r DocsRequest, tabID string) *docs.Request {
	rng := func(x DocsRange) *docs.Range {
		return &docs.Range{StartIndex: int64(x.StartIndex), EndIndex: int64(x.EndIndex), TabID: cmp.Or(x.TabID, tabID)}
	}
	loc := func(x DocsLocation) *docs.Location {
		return &docs.Location{Index: int64(x.Index), TabID: cmp.Or(x.TabID, tabID)}
	}
	out := &docs.Request{}
	switch {
	case r.DeleteContentRange != nil:
		out.DeleteContentRange = &docs.DeleteContentRangeRequest{Range: rng(*r.DeleteContentRange)}
	case r.InsertText != nil:
		out.InsertText = &docs.InsertTextRequest{Location: loc(r.InsertText.Location), Text: r.InsertText.Text}
	case r.UpdateParagraphStyle != nil:
		out.UpdateParagraphStyle = &docs.UpdateParagraphStyleRequest{
			Range:          rng(r.UpdateParagraphStyle.Range),
			ParagraphStyle: &docs.ParagraphStyle{NamedStyleType: r.UpdateParagraphStyle.NamedStyleType},
			Fields:         "namedStyleType",
		}
	case r.CreateParagraphBullets != nil:
		out.CreateParagraphBullets = &docs.CreateParagraphBulletsRequest{Range: rng(r.CreateParagraphBullets.Range), BulletPreset: r.CreateParagraphBullets.BulletPreset}
	case r.InsertTable != nil:
		out.InsertTable = &docs.InsertTableRequest{Location: loc(r.InsertTable.Location), Rows: int64(r.InsertTable.Rows), Columns: int64(r.InsertTable.Columns)}
	case r.UpdateTextStyle != nil:
		st := &docs.TextStyle{
			Bold:          r.UpdateTextStyle.Bold,
			Italic:        r.UpdateTextStyle.Italic,
			Strikethrough: r.UpdateTextStyle.Strikethrough,
		}
		if r.UpdateTextStyle.Link != "" {
			st.Link = &docs.Link{URL: r.UpdateTextStyle.Link}
		}
		out.UpdateTextStyle = &docs.UpdateTextStyleRequest{Range: rng(r.UpdateTextStyle.Range), TextStyle: st, Fields: "bold,italic,strikethrough,link"}
	}
	return out
}

func mapDocsError(err error) error {
	var gerr *rest.Error
	if !errors.As(err, &gerr) {
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
	case 400:
		msg := strings.ToLower(gerr.Message)
		if strings.Contains(msg, "revision") &&
			(strings.Contains(msg, "requiredrevisionid") ||
				strings.Contains(msg, "must be the latest") ||
				strings.Contains(msg, "not the latest revision")) {
			return ErrConflict
		}
		return ErrInvalidWrite
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
		DocumentID: d.DocumentID,
		RevisionID: d.RevisionID,
		Title:      d.Title,
		Lists:      map[string]DocsListProps{},
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
	base := DocsSpan{
		TabID:      tb.ID,
		StartIndex: int(el.StartIndex),
		EndIndex:   int(el.EndIndex),
	}
	switch {
	case el.SectionBreak != nil:
		base.Kind = "sectionBreak"
		return []DocsSpan{base}
	case el.TableOfContents != nil:
		base.Kind = "tableOfContents"
		return []DocsSpan{base}
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
	span := DocsSpan{
		TabID:      tb.ID,
		StartIndex: int(el.StartIndex),
		EndIndex:   int(el.EndIndex),
		Kind:       "table",
	}
	var rows [][]string
	for r, row := range t.TableRows {
		if row == nil {
			continue
		}
		for c, cell := range row.TableCells {
			if cell == nil {
				continue
			}
			text, ps, pe := firstCellParagraph(cell)
			span.Cells = append(span.Cells, DocsCell{
				Row: r, Col: c, StartIndex: ps, EndIndex: pe, Text: sanitizeCell(text),
			})
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

func firstCellParagraph(cell *docs.TableCell) (text string, start, end int) {
	start, end = int(cell.StartIndex), int(cell.EndIndex)
	for _, el := range cell.Content {
		if el == nil || el.Paragraph == nil {
			continue
		}
		text = paragraphText(el.Paragraph)
		return text, int(el.StartIndex), int(el.EndIndex)
	}
	return "", start, end
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
			img := DocsSpan{
				TabID:      tb.ID,
				StartIndex: int(e.StartIndex),
				EndIndex:   int(e.EndIndex),
				Kind:       "image",
				ObjectID:   oid,
			}
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
	span := DocsSpan{
		TabID:      tb.ID,
		StartIndex: int(el.StartIndex),
		EndIndex:   int(el.EndIndex),
		Text:       text,
	}
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
