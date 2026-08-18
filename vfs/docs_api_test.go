package vfs

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestMapDocsError(t *testing.T) {
	if !errors.Is(mapDocsError(&googleapi.Error{Code: 401}), ErrAuthExpired) {
		t.Fatal("401")
	}
	if !errors.Is(mapDocsError(&googleapi.Error{Code: 404}), ErrNotExist) {
		t.Fatal("404")
	}
	err := mapDocsError(&googleapi.Error{Code: 403, Message: "DLP blocked"})
	if !errors.Is(err, ErrPermission) || !strings.Contains(err.Error(), "DLP blocked") {
		t.Fatalf("403: %v", err)
	}
	if !errors.Is(mapDocsError(&googleapi.Error{
		Code: 400, Message: "requiredRevisionId must be the latest revision",
	}), ErrConflict) {
		t.Fatal("revision 400")
	}
	if !errors.Is(mapDocsError(&googleapi.Error{
		Code: 400, Message: "not the latest revision",
	}), ErrConflict) {
		t.Fatal("revision substring 400")
	}
	raw := mapDocsError(&googleapi.Error{Code: 400, Message: "invalid document structure"})
	if errors.Is(raw, ErrConflict) {
		t.Fatal("structure 400 must not be conflict")
	}
}

func TestSnapshotFromDocument_tabsAndImages(t *testing.T) {
	d := &docs.Document{
		DocumentId: "doc1",
		RevisionId: "R0",
		Title:      "Spec",
		Tabs: []*docs.Tab{
			{
				TabProperties: &docs.TabProperties{TabId: "t.a", Title: "Intro", Index: 0},
				DocumentTab: &docs.DocumentTab{
					Body: &docs.Body{Content: []*docs.StructuralElement{
						{StartIndex: 1, EndIndex: 2, SectionBreak: &docs.SectionBreak{}},
						{StartIndex: 2, EndIndex: 8, Paragraph: &docs.Paragraph{
							ParagraphStyle: &docs.ParagraphStyle{NamedStyleType: "HEADING_1"},
							Elements:       []*docs.ParagraphElement{{TextRun: &docs.TextRun{Content: "Intro\n"}}},
						}},
						{StartIndex: 8, EndIndex: 9, Paragraph: &docs.Paragraph{
							Elements: []*docs.ParagraphElement{{
								StartIndex: 8, EndIndex: 9,
								InlineObjectElement: &docs.InlineObjectElement{InlineObjectId: "kix.pic"},
							}},
						}},
					}},
					InlineObjects: map[string]docs.InlineObject{
						"kix.pic": {InlineObjectProperties: &docs.InlineObjectProperties{
							EmbeddedObject: &docs.EmbeddedObject{ImageProperties: &docs.ImageProperties{ContentUri: "https://img"}},
						}},
					},
				},
			},
			{
				TabProperties: &docs.TabProperties{TabId: "t.b", Title: "Appendix", Index: 1},
				DocumentTab: &docs.DocumentTab{
					Body: &docs.Body{Content: []*docs.StructuralElement{
						{StartIndex: 1, EndIndex: 6, Paragraph: &docs.Paragraph{
							Elements: []*docs.ParagraphElement{{TextRun: &docs.TextRun{Content: "More\n"}}},
						}},
					}},
				},
			},
		},
	}
	snap := snapshotFromDocument(d)
	if len(snap.Tabs) != 2 || snap.RevisionID != "R0" {
		t.Fatalf("tabs=%+v rev=%s", snap.Tabs, snap.RevisionID)
	}
	rd := snapshotToRich("/Spec", snap)
	var kinds []string
	for _, b := range rd.Blocks() {
		kinds = append(kinds, b.Kind)
		if b.Kind == BlockKindImage && b.Style.Attributes["object_id"] != "kix.pic" {
			t.Fatalf("image = %+v", b)
		}
	}
	if strings.Join(kinds, ",") != "heading,image,paragraph" {
		t.Fatalf("kinds = %s", kinds)
	}

	tab := &docs.Table{Rows: 1, Columns: 1, TableRows: []*docs.TableRow{{
		TableCells: []*docs.TableCell{{
			StartIndex: 10, EndIndex: 14,
			Content: []*docs.StructuralElement{{
				StartIndex: 11, EndIndex: 13,
				Paragraph: &docs.Paragraph{Elements: []*docs.ParagraphElement{
					{TextRun: &docs.TextRun{Content: "cell\n"}},
				}},
			}},
		}},
	}}}
	tableEl := &docs.StructuralElement{StartIndex: 9, EndIndex: 15, Table: tab}
	got := tableSpan(tableEl, tabBody{})
	if got.Kind != "table" || !strings.Contains(got.Text, "cell") || len(got.Cells) != 1 {
		t.Fatalf("tableSpan = %+v", got)
	}
	if !glyphOrdered("DECIMAL") || glyphOrdered("BULLET") {
		t.Fatal("glyphOrdered")
	}
	if !strings.Contains(rd.Text(), `class="tacklr-tab"`) || !strings.Contains(rd.Text(), "Appendix") {
		t.Fatalf("kernel html = %s", rd.Text())
	}
}

func TestGoogleDocs_httpGetBatchUpdate(t *testing.T) {
	ctx := t.Context()
	var sawWriteControl bool
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/documents/doc1") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"documentId": "doc1", "revisionId": "R0", "title": "Spec",
				"body": map[string]any{"content": []any{
					map[string]any{"startIndex": 1, "endIndex": 6, "paragraph": map[string]any{
						"elements": []any{map[string]any{"textRun": map[string]any{"content": "Hello\n"}}},
					}},
				}},
			})
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":batchUpdate") {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if wc, _ := body["writeControl"].(map[string]any); wc["requiredRevisionId"] == "R0" {
				sawWriteControl = true
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"writeControl": map[string]any{"requiredRevisionId": "R1"},
			})
			return
		}
		http.NotFound(w, r)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	holder := NewTokenHolder(Credential{Token: "tok"})
	hc := &http.Client{Transport: &oauth2.Transport{Source: holder, Base: ts.Client().Transport}}
	svc, err := docs.NewService(ctx, option.WithHTTPClient(hc), option.WithEndpoint(ts.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	api := GoogleDocs{Service: svc}
	snap, err := api.Get(ctx, "doc1")
	if err != nil || snap.DocumentID != "doc1" {
		t.Fatalf("Get = %+v err=%v", snap, err)
	}
	res, err := api.BatchUpdate(ctx, "doc1", DocsBatch{
		RequiredRevisionID: "R0",
		Requests:           []DocsRequest{reqInsert(1, "", "Hi")},
	})
	if err != nil || res.RevisionID != "R1" || !sawWriteControl {
		t.Fatalf("BatchUpdate = %+v saw=%v err=%v", res, sawWriteControl, err)
	}
}
