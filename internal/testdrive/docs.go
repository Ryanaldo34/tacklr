package testdrive

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/api/docs/v1"

	"github.com/ryanaldo34/tacklr/vfs"
)

type docState struct {
	snap vfs.DocsSnapshot
}

// SeedDoc sets documents.get state for id.
func (d *FX) SeedDoc(id, rev string, spans []vfs.DocsSpan, tabs []vfs.DocTab) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seedDocLocked(id, rev, spans, tabs)
}

func (d *FX) seedDocLocked(id, rev string, spans []vfs.DocsSpan, tabs []vfs.DocTab) {
	if d.docs == nil {
		d.docs = map[string]*docState{}
	}
	d.docs[id] = &docState{snap: vfs.DocsSnapshot{
		DocumentID: id, RevisionID: rev, Tabs: tabs, Body: spans, Lists: map[string]vfs.DocsListProps{},
	}}
}

// SetDocRev is an out-of-band revision bump (CAS tests).
func (d *FX) SetDocRev(id, rev string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s, ok := d.docs[id]; ok {
		s.snap.RevisionID = rev
	}
}

// FailNextDocWrites makes the next n documents.batchUpdate calls return a revision conflict.
func (d *FX) FailNextDocWrites(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.docConflictLeft = n
}

// FailDocBatch makes documents.batchUpdate return HTTP 400 (ErrInvalidWrite).
func (d *FX) FailDocBatch() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.docBatchFail = true
}

func (d *FX) serveDocs(w http.ResponseWriter, r *http.Request) {
	id, rpc := docsID(r.URL.Path)
	if id == "" {
		writeDriveErr(w, http.StatusNotFound, "not found")
		return
	}
	switch {
	case r.Method == http.MethodGet && rpc == "":
		d.serveDocsGet(w, id)
	case r.Method == http.MethodPost && rpc == "batchUpdate":
		d.serveDocsBatch(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func docsID(path string) (id, rpc string) {
	for _, pref := range []string{"/v1/documents/", "/documents/"} {
		if i := strings.Index(path, pref); i >= 0 {
			rest := path[i+len(pref):]
			id, rpc, _ = strings.Cut(rest, ":")
			id = strings.Trim(id, "/")
			return id, rpc
		}
	}
	return "", ""
}

func (d *FX) serveDocsGet(w http.ResponseWriter, id string) {
	s := d.ensureDoc(id)
	writeDriveJSON(w, http.StatusOK, documentJSON(s.snap))
}

func (d *FX) serveDocsBatch(w http.ResponseWriter, r *http.Request, id string) {
	raw, _ := io.ReadAll(r.Body)
	var body docs.BatchUpdateDocumentRequest
	if err := json.Unmarshal(raw, &body); err != nil {
		writeDriveErr(w, http.StatusBadRequest, "invalid write")
		return
	}
	if d.docBatchFail {
		writeDriveErr(w, http.StatusBadRequest, "invalid write")
		return
	}
	if d.docConflictLeft > 0 {
		d.docConflictLeft--
		writeDriveErr(w, http.StatusBadRequest, "requiredRevisionId must be the latest revision")
		return
	}
	s := d.ensureDoc(id)
	req := vfs.DocsBatch{}
	if body.WriteControl != nil {
		req.RequiredRevisionID = body.WriteControl.RequiredRevisionId
	}
	for _, dr := range body.Requests {
		if dr == nil {
			continue
		}
		req.Requests = append(req.Requests, vfs.DocsRequest{
			DeleteContentRange:     dr.DeleteContentRange,
			InsertText:             dr.InsertText,
			UpdateParagraphStyle:   dr.UpdateParagraphStyle,
			CreateParagraphBullets: dr.CreateParagraphBullets,
			InsertTable:            dr.InsertTable,
			UpdateTextStyle:        dr.UpdateTextStyle,
		})
	}
	if req.RequiredRevisionID != "" && s.snap.RevisionID != "" && req.RequiredRevisionID != s.snap.RevisionID {
		writeDriveErr(w, http.StatusBadRequest, "requiredRevisionId must be the latest revision")
		return
	}
	applyDocsBatch(&s.snap, req)
	next := s.snap.RevisionID + "+1"
	if s.snap.RevisionID == "" {
		next = "R1"
	}
	s.snap.RevisionID = next
	d.DocsBatches = append(d.DocsBatches, req)
	writeDriveJSON(w, http.StatusOK, map[string]any{
		"writeControl": map[string]any{"requiredRevisionId": next},
	})
}

func (d *FX) ensureDoc(id string) *docState {
	if d.docs == nil {
		d.docs = map[string]*docState{}
	}
	s, ok := d.docs[id]
	if !ok {
		s = &docState{snap: vfs.DocsSnapshot{
			DocumentID: id, RevisionID: "R0",
			Body: []vfs.DocsSpan{{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"}},
		}}
		d.docs[id] = s
	}
	return s
}

func documentJSON(snap vfs.DocsSnapshot) map[string]any {
	out := map[string]any{
		"documentId": snap.DocumentID,
		"revisionId": snap.RevisionID,
		"title":      snap.Title,
	}
	if len(snap.Tabs) == 0 {
		out["body"] = map[string]any{"content": spansJSON(snap.Body, "")}
		if lists := listsJSON(snap); lists != nil {
			out["lists"] = lists
		}
		if objs := objectsJSON(snap.Body); objs != nil {
			out["inlineObjects"] = objs
		}
		return out
	}
	var tabs []any
	for _, tab := range snap.Tabs {
		docTab := map[string]any{
			"body": map[string]any{"content": spansJSON(snap.Body, tab.ID)},
		}
		if lists := listsJSON(snap); lists != nil {
			docTab["lists"] = lists
		}
		if objs := objectsJSON(snap.Body); objs != nil {
			docTab["inlineObjects"] = objs
		}
		item := map[string]any{
			"tabProperties": map[string]any{"tabId": tab.ID, "title": tab.Title, "index": tab.Index},
			"documentTab":   docTab,
		}
		tabs = append(tabs, item)
	}
	out["tabs"] = tabs
	return out
}

func spansJSON(spans []vfs.DocsSpan, tabID string) []any {
	var out []any
	for _, sp := range spans {
		if tabID != "" && sp.TabID != "" && sp.TabID != tabID {
			continue
		}
		el := map[string]any{"startIndex": sp.StartIndex, "endIndex": sp.EndIndex}
		switch sp.Kind {
		case "sectionBreak":
			el["sectionBreak"] = map[string]any{}
		case "tableOfContents":
			el["tableOfContents"] = map[string]any{}
		case "table":
			el["table"] = tableJSON(sp)
		case "image":
			el["paragraph"] = map[string]any{
				"elements": []any{map[string]any{
					"startIndex": sp.StartIndex, "endIndex": sp.EndIndex,
					"inlineObjectElement": map[string]any{"inlineObjectId": sp.ObjectID},
				}},
			}
		default:
			p := map[string]any{
				"elements": []any{map[string]any{
					"startIndex": sp.StartIndex, "endIndex": sp.EndIndex,
					"textRun": map[string]any{"content": sp.Text + "\n"},
				}},
			}
			if sp.NamedStyle != "" || sp.Kind == "heading" {
				named := sp.NamedStyle
				if named == "" && sp.Kind == "heading" {
					named = "HEADING_" + strconv.Itoa(max(sp.Level, 1))
				}
				p["paragraphStyle"] = map[string]any{"namedStyleType": named}
			}
			if sp.Kind == "list_item" {
				p["bullet"] = map[string]any{"listId": sp.ListID, "nestingLevel": sp.Nesting}
			}
			el["paragraph"] = p
		}
		out = append(out, el)
	}
	return out
}

func tableJSON(sp vfs.DocsSpan) map[string]any {
	rows, cols := 0, 0
	for _, c := range sp.Cells {
		if c.Row+1 > rows {
			rows = c.Row + 1
		}
		if c.Col+1 > cols {
			cols = c.Col + 1
		}
	}
	grid := make([][]vfs.DocsCell, rows)
	for i := range grid {
		grid[i] = make([]vfs.DocsCell, cols)
	}
	for _, c := range sp.Cells {
		if c.Row < rows && c.Col < cols {
			grid[c.Row][c.Col] = c
		}
	}
	var tableRows []any
	for _, row := range grid {
		var cells []any
		for _, c := range row {
			cells = append(cells, map[string]any{
				"startIndex": c.StartIndex, "endIndex": c.EndIndex,
				"content": []any{map[string]any{
					"startIndex": c.StartIndex, "endIndex": c.EndIndex,
					"paragraph": map[string]any{
						"elements": []any{map[string]any{
							"textRun": map[string]any{"content": c.Text + "\n"},
						}},
					},
				}},
			})
		}
		tableRows = append(tableRows, map[string]any{"tableCells": cells})
	}
	return map[string]any{"tableRows": tableRows}
}

func listsJSON(snap vfs.DocsSnapshot) map[string]any {
	out := map[string]any{}
	for id, p := range snap.Lists {
		levels := make([]any, 0, len(p.GlyphTypes))
		for _, g := range p.GlyphTypes {
			levels = append(levels, map[string]any{"glyphType": g})
		}
		if len(levels) == 0 {
			g := "BULLET"
			if p.Ordered {
				g = "DECIMAL"
			}
			levels = append(levels, map[string]any{"glyphType": g})
		}
		out[id] = map[string]any{"listProperties": map[string]any{"nestingLevels": levels}}
	}
	for _, sp := range snap.Body {
		if sp.Kind == "list_item" && sp.ListID != "" {
			if _, ok := out[sp.ListID]; !ok {
				out[sp.ListID] = map[string]any{"listProperties": map[string]any{
					"nestingLevels": []any{map[string]any{"glyphType": "BULLET"}},
				}}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func objectsJSON(spans []vfs.DocsSpan) map[string]any {
	out := map[string]any{}
	for _, sp := range spans {
		if sp.Kind != "image" || sp.ObjectID == "" {
			continue
		}
		out[sp.ObjectID] = map[string]any{
			"inlineObjectProperties": map[string]any{
				"embeddedObject": map[string]any{
					"title":           sp.Text,
					"imageProperties": map[string]any{"contentUri": sp.NamedStyle},
				},
			},
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func applyDocsBatch(s *vfs.DocsSnapshot, req vfs.DocsBatch) {
	tabOf := func(tab string) string {
		if tab != "" {
			return tab
		}
		return req.TabID
	}
	sameTab := func(spTab, reqTab string) bool {
		if reqTab == "" {
			return true
		}
		return spTab == reqTab || spTab == ""
	}
	for _, r := range req.Requests {
		if del := r.DeleteContentRange; del != nil && del.Range != nil {
			start, end := int(del.Range.StartIndex), int(del.Range.EndIndex)
			tab := tabOf(del.Range.TabId)
			var next []vfs.DocsSpan
			for _, sp := range s.Body {
				if !sameTab(sp.TabID, tab) {
					next = append(next, sp)
					continue
				}
				if sp.Kind == "sectionBreak" && sp.StartIndex == 1 {
					next = append(next, sp)
					continue
				}
				if sp.Kind == "table" {
					changed := false
					for i := range sp.Cells {
						if sp.Cells[i].StartIndex >= start && sp.Cells[i].StartIndex < end {
							sp.Cells[i].Text = ""
							changed = true
						}
					}
					if changed {
						sp.Text = tsvFromCells(sp)
						next = append(next, sp)
						continue
					}
				}
				if sp.StartIndex < end && sp.EndIndex > start {
					continue
				}
				next = append(next, sp)
			}
			s.Body = next
		}
		if ins := r.InsertText; ins != nil && ins.Location != nil {
			idx := int(ins.Location.Index)
			tab := tabOf(ins.Location.TabId)
			text := ins.Text
			filled := false
			for i := range s.Body {
				if s.Body[i].Kind != "table" || !sameTab(s.Body[i].TabID, tab) {
					continue
				}
				for j := range s.Body[i].Cells {
					if s.Body[i].Cells[j].StartIndex == idx {
						s.Body[i].Cells[j].Text = strings.TrimSuffix(text, "\n")
						filled = true
					}
				}
				if filled {
					s.Body[i].Text = tsvFromCells(s.Body[i])
					break
				}
			}
			if filled {
				continue
			}
			raw := strings.TrimSuffix(text, "\n")
			level := 1
			if trimmed := strings.TrimLeft(raw, "\t"); trimmed != raw {
				level = len(raw) - len(trimmed) + 1
				raw = trimmed
			}
			s.Body = append(s.Body, vfs.DocsSpan{
				TabID: tab, StartIndex: idx, EndIndex: idx + 1 + len(text),
				Kind: "paragraph", Text: raw, Level: level,
			})
		}
		if st := r.UpdateParagraphStyle; st != nil && st.ParagraphStyle != nil && st.Range != nil {
			named := st.ParagraphStyle.NamedStyleType
			start := int(st.Range.StartIndex)
			tab := tabOf(st.Range.TabId)
			if strings.HasPrefix(named, "HEADING_") {
				n, _ := strconv.Atoi(strings.TrimPrefix(named, "HEADING_"))
				for i := range s.Body {
					if s.Body[i].StartIndex == start && sameTab(s.Body[i].TabID, tab) {
						s.Body[i].Kind = "heading"
						s.Body[i].Level = n
						s.Body[i].NamedStyle = named
					}
				}
			}
		}
		if b := r.CreateParagraphBullets; b != nil && b.Range != nil {
			start, end := int(b.Range.StartIndex), int(b.Range.EndIndex)
			tab := tabOf(b.Range.TabId)
			for i := range s.Body {
				if !sameTab(s.Body[i].TabID, tab) {
					continue
				}
				if s.Body[i].StartIndex >= start && s.Body[i].StartIndex < end {
					s.Body[i].Kind = "list_item"
					if s.Body[i].ListID == "" {
						s.Body[i].ListID = "list"
					}
					s.Body[i].Nesting = s.Body[i].Level - 1
				}
			}
		}
		if tbl := r.InsertTable; tbl != nil && tbl.Location != nil {
			idx := int(tbl.Location.Index)
			tab := tabOf(tbl.Location.TabId)
			rows, cols := int(tbl.Rows), int(tbl.Columns)
			var cells []vfs.DocsCell
			base := idx + 2
			for row := 0; row < rows; row++ {
				for col := 0; col < cols; col++ {
					ci := base + row*cols*2 + col*2
					cells = append(cells, vfs.DocsCell{Row: row, Col: col, StartIndex: ci, EndIndex: ci + 2})
				}
			}
			end := base + rows*cols*2
			s.Body = append(s.Body,
				vfs.DocsSpan{
					TabID: tab, StartIndex: idx, EndIndex: end,
					Kind: "table", Cells: cells,
				},
				vfs.DocsSpan{
					TabID: tab, StartIndex: end, EndIndex: end + 1,
					Kind: "paragraph",
				},
			)
		}
	}
}

func tsvFromCells(sp vfs.DocsSpan) string {
	rows, cols := 0, 0
	for _, c := range sp.Cells {
		if c.Row+1 > rows {
			rows = c.Row + 1
		}
		if c.Col+1 > cols {
			cols = c.Col + 1
		}
	}
	grid := make([][]string, rows)
	for i := range grid {
		grid[i] = make([]string, cols)
	}
	for _, c := range sp.Cells {
		if c.Row < rows && c.Col < cols {
			grid[c.Row][c.Col] = c.Text
		}
	}
	var b strings.Builder
	for i, row := range grid {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(strings.Join(row, "\t"))
	}
	return b.String()
}
