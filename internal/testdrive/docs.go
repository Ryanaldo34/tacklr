package testdrive

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/google/docs"

	"github.com/ryanaldo34/tacklr/vfs"
)

// SeedDoc sets documents.get state for id.
func (d *FX) SeedDoc(id, rev string, spans []vfs.DocsSpan, tabs []vfs.DocTab) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.docs[id] = &vfs.DocsSnapshot{DocumentID: id, RevisionID: rev, Tabs: tabs, Body: spans}
}

// SetDocRev is an out-of-band revision bump (CAS tests).
func (d *FX) SetDocRev(id, rev string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ensureDoc(id).RevisionID = rev
}

// FailNextDocWrites makes the next n documents.batchUpdate calls return a revision conflict.
func (d *FX) FailNextDocWrites(n int) {
	for range n {
		d.Fail(Fault{Call: "documents.batchUpdate", Status: http.StatusBadRequest, Message: "requiredRevisionId must be the latest revision"})
	}
}

// FailDocBatch makes the next documents.batchUpdate call return HTTP 400 (ErrInvalidWrite).
func (d *FX) FailDocBatch() {
	d.Fail(Fault{Call: "documents.batchUpdate", Status: http.StatusBadRequest, Message: "invalid write"})
}

func (d *FX) serveDocsGet(w http.ResponseWriter, _ *http.Request, id string) {
	writeDriveJSON(w, http.StatusOK, documentJSON(*d.ensureDoc(id)))
}

func (d *FX) serveDocsBatch(w http.ResponseWriter, r *http.Request, id string) {
	var body docs.BatchUpdateDocumentRequest
	_ = json.NewDecoder(r.Body).Decode(&body)
	s := d.ensureDoc(id)
	if body.WriteControl != nil && body.WriteControl.RequiredRevisionID != "" && body.WriteControl.RequiredRevisionID != s.RevisionID {
		writeDriveErr(w, http.StatusBadRequest, "requiredRevisionId must be the latest revision")
		return
	}
	applyDocsBatch(s, body.Requests)
	s.RevisionID += "+1"
	writeDriveJSON(w, http.StatusOK, map[string]any{
		"writeControl": map[string]any{"requiredRevisionId": s.RevisionID},
	})
}

// ensureDoc is the Docs state of id. A Doc that was not seeded (a new one)
// is empty, with one tab, as Docs returns it.
func (d *FX) ensureDoc(id string) *vfs.DocsSnapshot {
	s, ok := d.docs[id]
	if !ok {
		s = &vfs.DocsSnapshot{
			DocumentID: id, RevisionID: "R0",
			Tabs: []vfs.DocTab{{ID: "t.0"}},
			Body: []vfs.DocsSpan{{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"}},
		}
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

// listsJSON is a bulleted list for each list id of the list items.
func listsJSON(snap vfs.DocsSnapshot) map[string]any {
	out := map[string]any{}
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

// applyDocsBatch applies a batchUpdate's requests to s, as the Docs service
// would. Every request names its tab or none (the whole document).
func applyDocsBatch(s *vfs.DocsSnapshot, reqs []*docs.Request) {
	sameTab := func(spTab, reqTab string) bool {
		if reqTab == "" {
			return true
		}
		return spTab == reqTab || spTab == ""
	}
	for _, r := range reqs {
		if del := r.DeleteContentRange; del != nil && del.Range != nil {
			start, end := int(del.Range.StartIndex), int(del.Range.EndIndex)
			tab := del.Range.TabID
			var next []vfs.DocsSpan
			for _, sp := range s.Body {
				switch {
				case !sameTab(sp.TabID, tab), sp.Kind == "sectionBreak" && sp.StartIndex == 1:
					next = append(next, sp)
				case sp.Kind == "table" && start > sp.StartIndex && end < sp.EndIndex:
					// A range inside a table is the content of the cell it
					// starts at.
					for i := range sp.Cells {
						if sp.Cells[i].StartIndex == start {
							sp.Cells[i].Text, sp.Cells[i].EndIndex = "", start+1
						}
					}
					sp.Text = tsvFromCells(sp)
					next = append(next, sp)
				case sp.StartIndex >= end || sp.EndIndex <= start:
					next = append(next, sp)
				}
			}
			s.Body = next
		}
		if ins := r.InsertText; ins != nil && ins.Location != nil {
			idx := int(ins.Location.Index)
			tab := ins.Location.TabID
			text := ins.Text
			filled := false
			for i := range s.Body {
				if s.Body[i].Kind != "table" || !sameTab(s.Body[i].TabID, tab) {
					continue
				}
				for j := range s.Body[i].Cells {
					if c := &s.Body[i].Cells[j]; c.StartIndex == idx {
						c.Text = strings.TrimSuffix(text, "\n")
						c.EndIndex = idx + 1 + len(c.Text)
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
			tab := st.Range.TabID
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
			tab := b.Range.TabID
			for i := range s.Body {
				if !sameTab(s.Body[i].TabID, tab) {
					continue
				}
				if s.Body[i].StartIndex >= start && s.Body[i].StartIndex < end {
					s.Body[i].Kind = "list_item"
					if s.Body[i].ListID == "" {
						s.Body[i].ListID = "list"
					}
					s.Body[i].Nesting = max(s.Body[i].Level-1, 0)
				}
			}
		}
		if tbl := r.InsertTable; tbl != nil && tbl.Location != nil {
			idx := int(tbl.Location.Index)
			tab := tbl.Location.TabID
			rows, cols := int(tbl.Rows), int(tbl.Columns)
			var cells []vfs.DocsCell
			base := idx + 2
			for row := 0; row < rows; row++ {
				for col := 0; col < cols; col++ {
					ci := base + row*cols*2 + col*2
					cells = append(cells, vfs.DocsCell{Row: row, Col: col, StartIndex: ci, EndIndex: ci + 1})
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
