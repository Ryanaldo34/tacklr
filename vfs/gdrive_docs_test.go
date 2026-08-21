package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

type memDocs struct {
	snaps    map[string]vfs.DocsSnapshot
	rev      map[string]string
	batches  []vfs.DocsBatch
	fail     error
	rejectIf string
}

func newMemDocs(id, rev string, spans []vfs.DocsSpan, tabs []vfs.DocTab) *memDocs {
	return &memDocs{
		snaps: map[string]vfs.DocsSnapshot{
			id: {DocumentID: id, RevisionID: rev, Tabs: tabs, Body: spans, Lists: map[string]vfs.DocsListProps{}},
		},
		rev: map[string]string{id: rev},
	}
}

func (m *memDocs) Get(ctx context.Context, documentID string) (vfs.DocsSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return vfs.DocsSnapshot{}, err
	}
	if m.fail != nil {
		return vfs.DocsSnapshot{}, m.fail
	}
	s, ok := m.snaps[documentID]
	if !ok {
		s = vfs.DocsSnapshot{
			DocumentID: documentID, RevisionID: "R0",
			Body: []vfs.DocsSpan{{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"}},
		}
		if m.snaps == nil {
			m.snaps = map[string]vfs.DocsSnapshot{}
		}
		if m.rev == nil {
			m.rev = map[string]string{}
		}
		m.snaps[documentID] = s
		m.rev[documentID] = "R0"
		return s, nil
	}
	s.RevisionID = m.rev[documentID]
	return s, nil
}

func (m *memDocs) BatchUpdate(ctx context.Context, documentID string, req vfs.DocsBatch) (vfs.DocsBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return vfs.DocsBatchResult{}, err
	}
	if m.rejectIf != "" && req.RequiredRevisionID == m.rejectIf {
		return vfs.DocsBatchResult{}, vfs.ErrConflict
	}
	if cur := m.rev[documentID]; req.RequiredRevisionID != "" && cur != "" && req.RequiredRevisionID != cur {
		return vfs.DocsBatchResult{}, vfs.ErrConflict
	}
	m.batches = append(m.batches, req)
	s := m.snaps[documentID]
	applyDocsBatch(&s, req)
	next := m.rev[documentID] + "+1"
	if m.rev[documentID] == "" {
		next = "R1"
	}
	m.rev[documentID] = next
	s.RevisionID = next
	if m.snaps == nil {
		m.snaps = map[string]vfs.DocsSnapshot{}
	}
	m.snaps[documentID] = s
	return vfs.DocsBatchResult{RevisionID: next}, nil
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
			s.Body = append(s.Body, vfs.DocsSpan{
				TabID: tab, StartIndex: idx, EndIndex: base + rows*cols*2,
				Kind: "table", Cells: cells,
			})
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

func exportZip(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/drive_export_spec.zip")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDrive_exportReadHonestStat(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	api.nodes["doc1"].export = exportZip(t)

	ms := mountDrive(t, api, nil, false)
	st, err := ms.Stat(ctx, "/contracts/Spec")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" || st.Size != 0 {
		t.Fatalf("Stat = %+v err=%v", st, err)
	}
	if api.exports != 0 {
		t.Fatalf("Stat exported %d times", api.exports)
	}
	sheet, err := ms.Stat(ctx, "/contracts/Budget")
	if err != nil || sheet.MediaType != "application/vnd.google-apps.spreadsheet" || sheet.Size != 0 {
		t.Fatalf("sheet Stat = %+v err=%v", sheet, err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/Budget"); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("sheet ReadFile: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/Spec"); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("OpenFile native: %v", err)
	}
	doc, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Text(), "<h1>Spec</h1>") {
		t.Fatalf("projection = %s", doc.Text())
	}
	if doc.MediaType() != "application/vnd.google-apps.document" {
		t.Fatalf("mt = %s", doc.MediaType())
	}

	if !vfs.FuseAvailable() {
		return
	}
	before := api.exports
	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	host := filepath.Join(dir, "contracts", "Spec")
	hst, err := os.Stat(host)
	if err != nil {
		t.Fatal(err)
	}
	if hst.Size() != 0 {
		t.Fatalf("FUSE getattr size=%d want 0", hst.Size())
	}
	if api.exports != before {
		t.Fatalf("getattr exported (exports %d → %d)", before, api.exports)
	}
	got, err := os.ReadFile(host)
	if err != nil || !strings.Contains(string(got), "<h1>Spec</h1>") {
		t.Fatalf("FUSE cat = %q err=%v", got, err)
	}
}

func TestDrive_exportTooLarge(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	api.nodes["doc1"].export = bytesRepeat(vfs.MaxDocsExportBytes + 2)
	ms := mountDrive(t, api, nil, false)
	if _, err := ms.ReadText(ctx, "/contracts/Spec"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
}

func bytesRepeat(n int) []byte {
	return []byte(strings.Repeat("a", n))
}

func TestDrive_writablePlaintextAndTrash(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	docs := newMemDocs("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, docs, true)
	if err := ms.WriteFile(ctx, "/contracts/new.txt", []byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/contracts/new.txt")
	if err != nil || string(got) != "hello\n" {
		t.Fatalf("read new = %q err=%v", got, err)
	}
	if err := ms.MkdirAll(ctx, "/contracts/sub/dir"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Remove(ctx, "/contracts/new.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/new.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed: %v", err)
	}
	if err := ms.Remove(ctx, "/contracts"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("root: %v", err)
	}
	if err := ms.Remove(ctx, "/contracts/dup.txt"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("ambiguous: %v", err)
	}
	if err := ms.WriteFile(ctx, "/contracts/Spec", []byte("x")); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("native PutFile: %v", err)
	}
	plain := vfs.NewTextDocument("/contracts/Spec", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, plain); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("identity WriteDocument: %v", err)
	}
	still, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil || !blockHasText(still, "Hello") {
		t.Fatalf("identity write must not replace Doc IR: %v", err)
	}
	if err := ms.Remove(ctx, "/contracts/Spec"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/contracts/Spec"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed doc Stat: %v", err)
	}
	if _, err := ms.ReadText(ctx, "/contracts/Spec"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed doc ReadText: %v", err)
	}
}

func TestDrive_docsWriteCAS(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	docs := newMemDocs("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, docs, true)

	empty := vfs.NewRichDocument("/contracts/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "Nope"},
	})
	if err := ms.WriteDocument(ctx, empty); !errors.Is(err, vfs.ErrConflict) {
		t.Fatalf("empty hint: %v", err)
	}
	if len(docs.batches) != 0 {
		t.Fatalf("empty hint must not BatchUpdate: %d", len(docs.batches))
	}
	orig, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil || !blockHasText(orig, "Hello") {
		t.Fatalf("after empty hint: %v", err)
	}

	rd, ok := vfs.AsRich(orig)
	if !ok {
		t.Fatalf("type %T", orig)
	}
	before := vfs.ContentToken(orig)
	var paraID string
	for _, b := range rd.Blocks() {
		if b.Kind == vfs.BlockKindParagraph {
			paraID = b.ID
		}
	}
	if err := rd.ReplaceBlock(paraID, "World", false); err != nil {
		t.Fatal(err)
	}
	docs.rev["doc1"] = "R1"
	if err := ms.WriteDocument(ctx, orig); !errors.Is(err, vfs.ErrConflict) {
		t.Fatalf("CAS: %v", err)
	}
	again, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil || !blockHasText(again, "Hello") || vfs.ContentToken(again) != before {
		t.Fatalf("sibling CAS must keep Hello: %v", err)
	}
}

func TestDrive_docsReplaceSucceeds(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	docs := newMemDocs("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, docs, true)
	doc, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	before := vfs.ContentToken(doc)
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	var paraID string
	for _, b := range rd.Blocks() {
		if b.Kind == vfs.BlockKindParagraph {
			paraID = b.ID
		}
	}
	if err := rd.ReplaceBlock(paraID, "World", false); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	if !blockHasText(got, "World") {
		t.Fatalf("blocks after replace: %+v", got.(vfs.Structured).Blocks())
	}
	if vfs.ContentToken(got) == before {
		t.Fatal("ContentToken unchanged after replace")
	}
}

func TestDrive_createAsDoc(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	docs := &memDocs{snaps: map[string]vfs.DocsSnapshot{}, rev: map[string]string{}}
	ms := mountDrive(t, api, docs, true)
	created := vfs.NewRichDocument("/contracts/Policy", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "Hi"},
	})
	if err := ms.WriteDocument(ctx, created); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, n := range api.nodes {
		if n.meta.Name == "Policy" && n.meta.MimeType == "application/vnd.google-apps.document" {
			found = true
			if len(n.body) != 0 {
				t.Fatalf("metadata-only create planted media: %q", n.body)
			}
		}
	}
	if !found {
		t.Fatal("create-as-Doc missing")
	}
	st, err := ms.Stat(ctx, "/contracts/Policy")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("Stat Policy = %+v err=%v", st, err)
	}
	got, err := ms.ReadText(ctx, "/contracts/Policy")
	if err != nil || !blockHasText(got, "Hi") {
		t.Fatalf("create body: %v", err)
	}

	table := vfs.NewRichDocument("/contracts/Grid", "application/vnd.google-apps.document", []vfs.Block{{
		Kind: vfs.BlockKindTable, Text: "A\tB",
		Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "1", "cols": "2"}},
	}})
	if err := ms.WriteDocument(ctx, table); err != nil {
		t.Fatal(err)
	}
	grid, err := ms.ReadText(ctx, "/contracts/Grid")
	if err != nil {
		t.Fatal(err)
	}
	var sawTable bool
	for _, b := range grid.(vfs.Structured).Blocks() {
		if b.Kind == vfs.BlockKindTable && strings.Contains(b.Text, "A") && strings.Contains(b.Text, "B") {
			sawTable = true
		}
	}
	if !sawTable {
		t.Fatalf("table cells not filled: %+v", grid.(vfs.Structured).Blocks())
	}
}

func TestDrive_createDocAppliesInlineMarksInFollowupBatch(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	docs := &memDocs{snaps: map[string]vfs.DocsSnapshot{}, rev: map[string]string{}}
	ms := mountDrive(t, api, docs, true)
	doc := vfs.NewRichDocument("/contracts/Marks", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "See **x**"},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	if len(docs.batches) < 2 {
		t.Fatalf("want insert then style batches, got %d", len(docs.batches))
	}
	var sawBold bool
	for _, r := range docs.batches[len(docs.batches)-1].Requests {
		if st := r.UpdateTextStyle; st != nil && st.TextStyle != nil && st.TextStyle.Bold {
			sawBold = true
		}
	}
	if !sawBold {
		t.Fatalf("follow-up batch missing bold: %+v", docs.batches)
	}
	for _, r := range docs.batches[0].Requests {
		if ins := r.InsertText; ins != nil && strings.Contains(ins.Text, "**") {
			t.Fatalf("insert kept markdown: %q", ins.Text)
		}
		if r.UpdateTextStyle != nil {
			t.Fatal("text style must not share the insert batch")
		}
	}
}

func TestDrive_writeSheetRejected(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	docs := newMemDocs("sheet1", "R0", nil, nil)
	ms := mountDrive(t, api, docs, true)
	if err := ms.WriteDocument(ctx, vfs.NewRichDocument("/contracts/Budget", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "x"},
	})); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("sheet: %v", err)
	}
}

func TestDrive_nestedPlainFilesAndDirs(t *testing.T) {
	ctx := t.Context()
	ms := mountDrive(t, driveTree(), newMemDocs("doc1", "R0", nil, nil), true)
	if err := ms.WriteFile(ctx, "/contracts/a/b/c.txt", []byte("z")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/contracts/a/b/c.txt")
	if err != nil || string(got) != "z" {
		t.Fatalf("read = %q err=%v", got, err)
	}
	ents, err := ms.ReadDir(ctx, "/contracts/a")
	if err != nil || len(ents) == 0 {
		t.Fatalf("readdir: %+v err=%v", ents, err)
	}
	if err := ms.MkdirAll(ctx, "/contracts/d/e"); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/contracts/d/e")
	if err != nil || !st.IsDir {
		t.Fatalf("stat dir %+v err=%v", st, err)
	}
}

func TestDrive_writeDocumentEdges(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	docs := newMemDocs("doc1", "R0", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}, nil)
	ro := mountDrive(t, api, docs, false)
	if err := ro.WriteDocument(ctx, vfs.NewRichDocument("/contracts/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "x"},
	})); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("readonly: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	ms := mountDrive(t, api, docs, true)
	if err := ms.WriteDocument(canceled, vfs.NewRichDocument("/contracts/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "x"},
	})); err == nil {
		t.Fatal("canceled write")
	}
	nested := vfs.NewTextDocument("/contracts/sub/dir/edge.txt", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, nested); err != nil {
		t.Fatal(err)
	}
	gotNested, err := ms.ReadFile(ctx, "/contracts/sub/dir/edge.txt")
	if err != nil || string(gotNested) != "plain" {
		t.Fatalf("nested txt = %q err=%v", gotNested, err)
	}
	txt := vfs.NewTextDocument("/contracts/edge.txt", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, txt); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/contracts/edge.txt")
	if err != nil || string(got) != "plain" {
		t.Fatalf("txt = %q err=%v", got, err)
	}
	doc, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	rd.SetBlocks([]vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "**Spec**", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "See [x](https://e)"},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
}

func TestDrive_docsSetBlocksNestedAndOmitImage(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	spans := []vfs.DocsSpan{
		{TabID: "t.abc", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.abc", StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{TabID: "t.abc", StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
		{TabID: "t.abc", StartIndex: 14, EndIndex: 15, Kind: "image", ObjectID: "kix.pic"},
	}
	docs := newMemDocs("doc1", "R0", spans, []vfs.DocTab{{ID: "t.abc", Title: "Intro", Index: 0}})
	ms := mountDrive(t, api, docs, true)
	doc, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	rd.SetBlocks([]vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.abc"}}},
		{Kind: vfs.BlockKindListItem, Text: "a", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.abc", "list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindListItem, Text: "nested", Style: vfs.StyleMeta{Level: 2, Attributes: map[string]string{"tab_id": "t.abc", "list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindParagraph, Text: "kept", Style: vfs.StyleMeta{Attributes: map[string]string{"tab_id": "t.abc"}}},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	var texts []string
	for _, b := range got.(vfs.Structured).Blocks() {
		kinds = append(kinds, b.Kind)
		texts = append(texts, b.Text)
		if b.Kind == vfs.BlockKindImage {
			t.Fatalf("omitted image still present: %+v", b)
		}
	}
	if !strings.Contains(strings.Join(kinds, ","), "heading") || !strings.Contains(strings.Join(kinds, ","), "list_item") {
		t.Fatalf("kinds = %v", kinds)
	}
	joined := strings.Join(texts, " ")
	if !strings.Contains(joined, "nested") || !strings.Contains(joined, "Title") || !strings.Contains(joined, "kept") {
		t.Fatalf("texts = %v", texts)
	}
}

func TestBindingSpec_writableOptIn(t *testing.T) {
	ro := vfs.BindingSpec(vfs.Binding{Provider: "gdrive", Point: "/c"})
	if !ro.ReadOnly {
		t.Fatal("zero-value must be read-only")
	}
	w := vfs.BindingSpec(vfs.Binding{Provider: "gdrive", Point: "/c", Writable: true})
	if w.ReadOnly {
		t.Fatal("Writable bind must not be read-only")
	}
}

func mountDrive(t *testing.T, api *memDrive, docs vfs.DocsAPI, writable bool) *vfs.MountSession {
	t.Helper()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: vfs.Credential{Token: "t"}, Writable: writable,
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api, Docs: docs}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(t.Context(), vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Writable: writable,
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})); err != nil {
		t.Fatal(err)
	}
	return ms
}

func blockHasText(doc vfs.Textual, want string) bool {
	s, ok := doc.(vfs.Structured)
	if !ok {
		return false
	}
	for _, b := range s.Blocks() {
		if b.Text == want {
			return true
		}
	}
	return false
}
