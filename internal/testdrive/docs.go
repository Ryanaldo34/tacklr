package testdrive

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs"
)

// SeedDoc sets documents.get state for id.
func (d *FX) SeedDoc(id string, spans []vfs.DocsSpan, tabs []vfs.DocTab) {
	d.SeedDocJSON(id, documentJSON(vfs.DocsSnapshot{Tabs: tabs, Body: spans}))
}

// SeedDocJSON sets the documents.get reply for id to doc, a JSON value
// as the Docs API sends it.
func (d *FX) SeedDocJSON(id string, doc any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.docs[id] = doc
}

// serveDocsGet answers documents.get. A Doc that was not seeded is empty,
// with one tab, as Docs returns it.
func (d *FX) serveDocsGet(w http.ResponseWriter, _ *http.Request, id string) {
	doc, ok := d.docs[id]
	if !ok {
		doc = documentJSON(vfs.DocsSnapshot{Tabs: []vfs.DocTab{{ID: "t.0"}}})
	}
	writeDriveJSON(w, http.StatusOK, doc)
}

func documentJSON(snap vfs.DocsSnapshot) map[string]any {
	out := map[string]any{"title": snap.Title}
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
		el := map[string]any{}
		switch sp.Kind {
		case "table":
			el["table"] = tableJSON(sp)
		case "image":
			el["paragraph"] = map[string]any{
				"elements": []any{map[string]any{
					"inlineObjectElement": map[string]any{"inlineObjectId": sp.ObjectID},
				}},
			}
		default:
			p := map[string]any{
				"elements": []any{map[string]any{
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

// tableJSON is the table of sp.Text, one row per line and one cell per
// tab.
func tableJSON(sp vfs.DocsSpan) map[string]any {
	var tableRows []any
	for line := range strings.SplitSeq(sp.Text, "\n") {
		var cells []any
		for cell := range strings.SplitSeq(line, "\t") {
			cells = append(cells, map[string]any{
				"content": []any{map[string]any{
					"paragraph": map[string]any{
						"elements": []any{map[string]any{
							"textRun": map[string]any{"content": cell + "\n"},
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
