package vfs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/api/docs/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// DocsAPI is the Docs subset used by the provider. Tests inject a fake.
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

// DocsListProps is list metadata from documents.get.
type DocsListProps struct {
	Ordered    bool
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

// DocsRequest fields are the real batchUpdate request names.
type DocsRequest struct {
	DeleteContentRange     *docs.DeleteContentRangeRequest
	InsertText             *docs.InsertTextRequest
	UpdateParagraphStyle   *docs.UpdateParagraphStyleRequest
	CreateParagraphBullets *docs.CreateParagraphBulletsRequest
	InsertTable            *docs.InsertTableRequest
	UpdateTextStyle        *docs.UpdateTextStyleRequest
}

// DocsBatchResult is writeControl.requiredRevisionId after apply.
type DocsBatchResult struct {
	RevisionID string
}

// GoogleDocs implements DocsAPI with google.golang.org/api/docs/v1.
type googleDocs struct {
	service *docs.Service
}

// newGoogleDocs builds a Docs service that reads the live TokenHolder.
func newGoogleDocs(ctx context.Context, holder *TokenHolder) (*googleDocs, error) {
	if holder == nil {
		return nil, fmt.Errorf("vfs: docs token required")
	}
	svc, err := docs.NewService(ctx, option.WithTokenSource(holder))
	if err != nil {
		return nil, fmt.Errorf("vfs: docs service: %w", err)
	}
	return &googleDocs{service: svc}, nil
}

// Get implements DocsAPI. Always sets includeTabsContent=true.
func (g googleDocs) Get(ctx context.Context, documentID string) (DocsSnapshot, error) {
	f, err := g.service.Documents.Get(documentID).
		IncludeTabsContent(true).
		Context(ctx).
		Do()
	if err != nil {
		return DocsSnapshot{}, mapDocsError(err)
	}
	return snapshotFromDocument(f), nil
}

// BatchUpdate implements DocsAPI. Returns only writeControl.requiredRevisionId.
func (g googleDocs) BatchUpdate(ctx context.Context, documentID string, req DocsBatch) (DocsBatchResult, error) {
	out := make([]*docs.Request, 0, len(req.Requests))
	for i := range req.Requests {
		out = append(out, docsRequestToAPI(req.Requests[i], req.TabID))
	}
	call := &docs.BatchUpdateDocumentRequest{
		Requests: out,
	}
	if req.RequiredRevisionID != "" {
		call.WriteControl = &docs.WriteControl{RequiredRevisionId: req.RequiredRevisionID}
	}
	resp, err := g.service.Documents.BatchUpdate(documentID, call).Context(ctx).Do()
	if err != nil {
		return DocsBatchResult{}, mapDocsError(err)
	}
	rev := ""
	if resp != nil && resp.WriteControl != nil {
		rev = resp.WriteControl.RequiredRevisionId
	}
	return DocsBatchResult{RevisionID: rev}, nil
}

func docsRequestToAPI(r DocsRequest, tabID string) *docs.Request {
	applyTab := func(loc *docs.Location, rng *docs.Range) {
		if tabID == "" {
			return
		}
		if loc != nil && loc.TabId == "" {
			loc.TabId = tabID
		}
		if rng != nil && rng.TabId == "" {
			rng.TabId = tabID
		}
	}
	if r.DeleteContentRange != nil {
		applyTab(nil, r.DeleteContentRange.Range)
	}
	if r.InsertText != nil {
		applyTab(r.InsertText.Location, nil)
	}
	if r.UpdateParagraphStyle != nil {
		applyTab(nil, r.UpdateParagraphStyle.Range)
	}
	if r.CreateParagraphBullets != nil {
		applyTab(nil, r.CreateParagraphBullets.Range)
	}
	if r.InsertTable != nil {
		applyTab(r.InsertTable.Location, nil)
	}
	if r.UpdateTextStyle != nil {
		applyTab(nil, r.UpdateTextStyle.Range)
	}
	return &docs.Request{
		DeleteContentRange:     r.DeleteContentRange,
		InsertText:             r.InsertText,
		UpdateParagraphStyle:   r.UpdateParagraphStyle,
		CreateParagraphBullets: r.CreateParagraphBullets,
		InsertTable:            r.InsertTable,
		UpdateTextStyle:        r.UpdateTextStyle,
	}
}

func mapDocsError(err error) error {
	if err == nil {
		return nil
	}
	var gerr *googleapi.Error
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
		return err
	default:
		return err
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
	if d == nil {
		return nil
	}
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
					id, title = t.TabProperties.TabId, t.TabProperties.Title
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
	if d == nil {
		return DocsSnapshot{}
	}
	snap := DocsSnapshot{
		DocumentID: d.DocumentId,
		RevisionID: d.RevisionId,
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
				if i == 0 {
					p.Ordered = glyphOrdered(nl.GlyphType)
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

func walkBodySpans(tb tabBody) []DocsSpan {
	if tb.Body == nil {
		return nil
	}
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
			oid := e.InlineObjectElement.InlineObjectId
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
				img.NamedStyle = obj.InlineObjectProperties.EmbeddedObject.ImageProperties.ContentUri
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
		span.ListID = p.Bullet.ListId
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
			if st.Link != nil && st.Link.Url != "" {
				marks[MarkHref] = st.Link.Url
			}
		}
		runs = append(runs, Run{Text: text, Marks: marks})
	}
	if len(runs) == 0 {
		return ""
	}
	return FormatInline(mergeRuns(runs))
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
		if p, ok := lists[sp.ListID]; ok {
			if sp.Nesting < len(p.GlyphTypes) && glyphOrdered(p.GlyphTypes[sp.Nesting]) {
				lt = "ol"
			} else if p.Ordered {
				lt = "ol"
			}
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
