package vfsindex_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func treeLocal(t *testing.T, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(members...)(context.Background(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

// TestMountIndexer_indexSearchAndNotify: VFS file → brain Document/Chunks →
// search finds content; hash skip; re-index after write; missing path removes.
func TestMountIndexer_indexSearchAndNotify(t *testing.T) {
	ctx := context.Background()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}

	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	idx.Chunking = tinyChunks

	body := "alpha line\nbeta TODO findme\ngamma line\ndelta line\n"

	if err := ms.Route(ctx, "/workspace/work/note.txt").
		WriteFile(ctx, []byte(body)); err != nil {
		t.Fatal(err)
	}

	// IndexPrefix
	stats, err := idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 1 {
		t.Fatalf("stats: %+v", stats)
	}

	parentID := idx.DocumentID("/workspace/work/note.txt")
	doc, err := eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Properties[vfsindex.PropVFSPath] != "/workspace/work/note.txt" {
		t.Fatalf("vfs_path: %+v", doc.Properties)
	}
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// The chunks cover lines 1–4 in order, with no gap or overlap.
	if got := joinChunks(t, eng, scope, children); got != strings.TrimSuffix(body, "\n") {
		t.Fatalf("chunks joined = %q, want the file", got)
	}
	c0, err := eng.Read(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	// Search finds the parent via chunk content
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "findme"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != parentID {
		t.Fatalf("search: %+v", page.Objects)
	}
	// Chunk 0 starts at line 1 and includes TODO
	if c0.Properties[vfsindex.PropStartLine] != float64(1) || !strings.Contains(c0.Content, "TODO") {
		t.Fatalf("chunk0 content: %q", c0.Content)
	}

	// Version skip
	stats, err = idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{})
	if err != nil || stats.Skipped != 1 || stats.Indexed != 0 {
		t.Fatalf("version skip stats: %+v err=%v", stats, err)
	}

	// AfterPersist + SyncScheduler re-index on write
	sched := vfsindex.NewSyncScheduler(idx)
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		return sched.Notify(ctx, path, vfsindex.ReasonSync)
	})

	if err := ms.Route(ctx, "/workspace/work/note.txt").
		WriteFile(ctx, []byte("rewritten uniquephrase\n")); err != nil {
		t.Fatal(err)
	}

	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "uniquephrase"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 {
		t.Fatal("expected re-index after WriteFile hook")
	}
	// old content should shrink to one chunk
	children, err = eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("after rewrite chunks=%d", len(children))
	}

	// Remove file → soft-delete index

	if err := ms.Route(ctx, "/workspace/work/note.txt").
		Remove(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.IndexPath(ctx, "/workspace/work/note.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Read(ctx, scope, parentID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("want not found after remove, got %v", err)
	}

	// Binary skip by extension

	if err := ms.Route(ctx, "/workspace/work/pic.png").
		WriteFile(ctx, []byte{0x89, 'P', 'N', 'G', 0, 1, 2}); err != nil {
		t.Fatal(err)
	}

	stats, err = idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("png skip: %+v err=%v", stats, err)
	}

	// MaxFiles

	if err := ms.Route(ctx, "/workspace/work/c.txt").
		WriteFile(ctx, []byte("c\n")); err != nil {
		t.Fatal(err)
	}

	stats, err = idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{MaxFiles: 1})
	if err != nil || stats.Indexed+stats.Skipped < 1 {
		t.Fatalf("max files: %+v err=%v", stats, err)
	}

	// IndexPath on directory is a no-op
	if _, err := idx.IndexPath(ctx, "/workspace/work"); err != nil {
		t.Fatal(err)
	}

	// A run of blank lines longer than a chunk makes no blank chunk.
	gap := "top-word\n" + strings.Repeat("\n", 100) + "bottom-word\n"
	if err := ms.Route(ctx, "/workspace/work/gap.txt").WriteFile(ctx, []byte(gap)); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.IndexPath(ctx, "/workspace/work/gap.txt"); err != nil {
		t.Fatal(err)
	}
	gapKids, err := eng.ListChildren(ctx, scope, idx.DocumentID("/workspace/work/gap.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range gapKids {
		if obj, err := eng.Read(ctx, scope, k.ID); err != nil || strings.TrimSpace(obj.Content) == "" {
			t.Fatalf("gap file has a blank chunk %q (%v)", obj.Content, err)
		}
	}
	if got := joinChunks(t, eng, scope, gapKids); !strings.Contains(got, "top-word") || !strings.Contains(got, "bottom-word") {
		t.Fatalf("gap file chunks = %q, want both words", got)
	}

	// Constructor guards
	if _, err := vfsindex.NewMountIndexer(nil, eng, scope); err == nil {
		t.Fatal("nil ms")
	}
	if _, err := vfsindex.NewMountIndexer(ms, nil, scope); err == nil {
		t.Fatal("nil eng")
	}
	if _, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{}); err == nil {
		t.Fatal("nil ns")
	}
}

// TestMountIndexer_markdownBlocksChunks: Markdown is chunked by structured
// heading/preamble blocks with stable block_id props.
func TestMountIndexer_markdownBlocksChunks(t *testing.T) {
	ctx := context.Background()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}

	md := "intro line\n\n# Title\n\n## Install\n\npip install x\n\n## API\n\ncall me\n"

	if err := ms.Route(ctx, "/workspace/work/guide.md").
		WriteFile(ctx, []byte(md)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{}); err != nil {
		t.Fatal(err)
	}

	parentID := idx.DocumentID("/workspace/work/guide.md")
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// preamble + title + title/install + title/api
	if len(children) != 4 {
		t.Fatalf("expected 4 block chunks, got %d", len(children))
	}

	byBlock := map[string]brain.RichObject{}
	for _, ch := range children {
		obj, err := eng.Read(ctx, scope, ch.ID)
		if err != nil {
			t.Fatal(err)
		}
		bid, _ := obj.Properties[vfsindex.PropBlockID].(string)
		if bid == "" {
			t.Fatalf("chunk missing block_id: title=%q props=%+v", obj.Title, obj.Properties)
		}
		if obj.Properties[vfsindex.PropHeadingPath] != bid {
			t.Fatalf("heading_path should match block_id: %+v", obj.Properties)
		}
		startL, _ := obj.Properties[vfsindex.PropStartLine].(float64)
		endL, _ := obj.Properties[vfsindex.PropEndLine].(float64)
		if startL < 1 || endL < startL {
			t.Fatalf("block %q start/end lines: start=%v end=%v", bid, startL, endL)
		}
		byBlock[bid] = obj
	}
	install, ok := byBlock["title/install"]
	if !ok {
		t.Fatalf("missing title/install; blocks=%v", keys(byBlock))
	}
	if !strings.Contains(install.Content, "pip install") {
		t.Fatalf("install chunk content: %q", install.Content)
	}
	instStart, _ := install.Properties[vfsindex.PropStartLine].(float64)
	instEnd, _ := install.Properties[vfsindex.PropEndLine].(float64)
	if instStart < 1 || instEnd <= instStart {
		t.Fatalf("install span lines: start=%v end=%v", instStart, instEnd)
	}
	// Content change: the edited block gets a new chunk; the others keep theirs.
	installID, apiID := install.ID, byBlock["title/api"].ID
	parent, err := eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	oldVersion, _ := parent.Properties[vfsindex.PropSourceVersion].(string)
	md2 := "intro line\n\n# Title\n\n## Install\n\npip install y\n\n## API\n\ncall me\n"

	if err := ms.Route(ctx, "/workspace/work/guide.md").
		WriteFile(ctx, []byte(md2)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.IndexPath(ctx, "/workspace/work/guide.md"); err != nil {
		t.Fatal(err)
	}
	parent, err = eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	newVersion, _ := parent.Properties[vfsindex.PropSourceVersion].(string)
	if newVersion == "" || newVersion == oldVersion {
		t.Fatalf("source_version should change after edit: old=%q new=%q", oldVersion, newVersion)
	}
	children, err = eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]uuid.UUID{}
	for _, ch := range children {
		obj, err := eng.Read(ctx, scope, ch.ID)
		if err != nil {
			t.Fatal(err)
		}
		bid, _ := obj.Properties[vfsindex.PropBlockID].(string)
		ids[bid] = obj.ID
		if bid == "title/install" && !strings.Contains(obj.Content, "pip install y") {
			t.Fatalf("install chunk content should update: %q", obj.Content)
		}
	}
	if ids["title/install"] == installID || ids["title/api"] != apiID {
		t.Fatalf("after edit: install %s (was %s), api %s (was %s)", ids["title/install"], installID, ids["title/api"], apiID)
	}

	// A section over Chunking.Max splits into several chunks of that block.
	idx.Chunking = tinyChunks
	big := "# Big\n\n" + strings.Repeat("section body words\n", 20)
	if err := ms.Route(ctx, "/workspace/work/big.md").WriteFile(ctx, []byte(big)); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.IndexPath(ctx, "/workspace/work/big.md"); err != nil {
		t.Fatal(err)
	}
	bigKids, err := eng.ListChildren(ctx, scope, idx.DocumentID("/workspace/work/big.md"))
	if err != nil || len(bigKids) < 2 {
		t.Fatalf("big section chunks = %d %v, want several", len(bigKids), err)
	}
	last, err := eng.Read(ctx, scope, bigKids[len(bigKids)-1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if last.Properties[vfsindex.PropBlockID] != "big" || last.Properties[vfsindex.PropEndLine] != float64(22) {
		t.Fatalf("last chunk of the big section = %+v", last.Properties)
	}
}

// tinyChunks makes a few short lines span several chunks.
var tinyChunks = vfsindex.ChunkSizes{Min: 8, Avg: 16, Max: 32}

// joinChunks is the indexed text of children in order, one line end between chunks.
func joinChunks(t *testing.T, eng *brain.Engine, scope brain.Scope, children []brain.RichObject) string {
	t.Helper()
	parts := make([]string, len(children))
	for i, ch := range children {
		obj, err := eng.Read(t.Context(), scope, ch.ID)
		if err != nil {
			t.Fatal(err)
		}
		parts[i] = obj.Content
	}
	return strings.Join(parts, "\n")
}

// TestMountIndexer_emptyMarkdownLineChunks: empty .md has Blocks() nil, so
// it is chunked like plain text (and has no chunks).
func TestMountIndexer_emptyMarkdownLineChunks(t *testing.T) {
	ctx := context.Background()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	// Empty body → Blocks() nil → plain-text path (no preamble block).

	if err := ms.Route(ctx, "/workspace/work/empty.md").
		WriteFile(ctx, nil); err != nil {
		t.Fatal(err)
	}

	res, err := idx.IndexPath(ctx, "/workspace/work/empty.md")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("empty md index: res=%q err=%v", res, err)
	}
	parentID := idx.DocumentID("/workspace/work/empty.md")
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 0 {
		t.Fatalf("empty md chunks=%d want 0", len(children))
	}
	if _, err := eng.Read(ctx, scope, parentID); err != nil {
		t.Fatal(err)
	}

	// Non-empty, no headings → single preamble block (structure path, not lineChunks).

	if err := ms.Route(ctx, "/workspace/work/plain.md").
		WriteFile(ctx, []byte("line one\nline two\nline three\n")); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.IndexPath(ctx, "/workspace/work/plain.md"); err != nil {
		t.Fatal(err)
	}
	plainID := idx.DocumentID("/workspace/work/plain.md")
	children, err = eng.ListChildren(ctx, scope, plainID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("preamble-only md chunks=%d want 1", len(children))
	}
	obj, err := eng.Read(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Properties[vfsindex.PropBlockID] != "preamble" {
		t.Fatalf("want preamble block_id, props=%+v", obj.Properties)
	}
	if !strings.Contains(obj.Content, "line one") {
		t.Fatalf("preamble content: %q", obj.Content)
	}
}

// TestMountIndexer_IndexFileAndDefaults: Stat-reuse API, binary skip,
// defaults for lines/bytes/kinds, and canceled context.
func TestMountIndexer_IndexFileAndDefaults(t *testing.T) {
	ctx := context.Background()
	ms := treeLocal(t,
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("off", vfs.Local(t.TempDir())).Indexed("none"),
	)
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	// Zero fields → defaults in indexFile
	idx.Chunking = vfsindex.ChunkSizes{}
	idx.MaxIndexBytes = 0
	idx.DocumentKind = ""
	idx.ChunkKind = ""

	if err := ms.Route(ctx, "/workspace/work/a.txt").
		WriteFile(ctx, []byte("hello searchable-phrase\n")); err != nil {
		t.Fatal(err)
	}

	st, err := ms.Route(ctx, "/workspace/work/a.txt").
		Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := idx.IndexFile(ctx, "/workspace/work/a.txt", st)
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("IndexFile: res=%q err=%v", res, err)
	}
	res, err = idx.IndexFile(ctx, "/workspace/work/a.txt", st)
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("version skip via IndexFile: res=%q err=%v", res, err)
	}
	// Directory FileInfo

	dst, err := ms.Route(ctx, "/workspace/work").
		Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexFile(ctx, "/workspace/work", dst)
	if err != nil || res != vfsindex.PathDirectory {
		t.Fatalf("dir IndexFile: res=%q err=%v", res, err)
	}

	if err := ms.Route(ctx, "/workspace/work/pic.bin").
		WriteFile(ctx, []byte{0x00, 0x01, 0xff}); err != nil {
		t.Fatal(err)
	}

	// .bin may still be text-like via detect; use .png extension gate

	if err := ms.Route(ctx, "/workspace/work/x.png").
		WriteFile(ctx, []byte{0x89, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}

	pst, err := ms.Route(ctx, "/workspace/work/x.png").
		Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexFile(ctx, "/workspace/work/x.png", pst)
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("png skip: res=%q err=%v", res, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := idx.IndexFile(canceled, "/workspace/work/a.txt", st); err == nil {
		t.Fatal("want ctx canceled error")
	}
	if _, err := idx.UnindexPath(canceled, "/workspace/work/a.txt"); err == nil {
		t.Fatal("unindex want ctx canceled")
	}
	if _, err := idx.IndexPath(canceled, "/workspace/work/a.txt"); err == nil {
		t.Fatal("IndexPath want ctx canceled")
	}

	// Bad virtual path
	if _, err := idx.IndexPath(ctx, "relative"); err == nil {
		t.Fatal("relative path")
	}
	if _, err := idx.IndexFile(ctx, "relative", st); err == nil {
		t.Fatal("IndexFile relative")
	}
	if _, err := idx.UnindexPath(ctx, "relative"); err == nil {
		t.Fatal("UnindexPath relative")
	}
	if _, err := idx.IndexPrefix(ctx, "relative", vfsindex.IndexOpts{}); err == nil {
		t.Fatal("IndexPrefix relative")
	}

	// Nested walk under prefix

	if err := ms.Route(ctx, "/workspace/work/sub").
		MkdirAll(ctx); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/sub/nested.txt").
		WriteFile(ctx, []byte("nested-unique-token\n")); err != nil {
		t.Fatal(err)
	}

	stats, err := idx.IndexPrefix(ctx, "/workspace/work/sub", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed < 1 {
		t.Fatalf("nested prefix: %+v err=%v", stats, err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "nested-unique-token"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("nested search: %+v err=%v", page.Objects, err)
	}

	if err := ms.Route(ctx, "/workspace/work/binlike.txt").
		WriteFile(ctx, []byte("ok\x00null")); err != nil {
		t.Fatal(err)
	}

	res, err = idx.IndexPath(ctx, "/workspace/work/binlike.txt")
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("NUL in text-like file: res=%q err=%v", res, err)
	}
	for name, body := range map[string][]byte{
		"binlike.md": []byte("# Head\n\nok\x00null"),
		"noutf8.txt": bytes.Repeat([]byte{0x80}, 600),
	} {
		if err := ms.Route(ctx, "/workspace/work/"+name).WriteFile(ctx, body); err != nil {
			t.Fatal(err)
		}
		if res, err := idx.IndexPath(ctx, "/workspace/work/"+name); err != nil || res != vfsindex.PathSkipped {
			t.Fatalf("binary %s: res=%q err=%v", name, res, err)
		}
	}

	idx.MaxIndexBytes = 32
	idx.Chunking = tinyChunks
	long := "head-unique-token\n" + strings.Repeat("wordline\n", 40) + "tail-unique-token\n"

	if err := ms.Route(ctx, "/workspace/work/long.txt").
		WriteFile(ctx, []byte(long)); err != nil {
		t.Fatal(err)
	}

	res, err = idx.IndexPath(ctx, "/workspace/work/long.txt")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("long file: res=%q err=%v", res, err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "head-unique-token"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("truncated file should still index the prefix: %+v err=%v", page.Objects, err)
	}

	// PolicyNone member: IndexPath / IndexPrefix report skipped (no Document written).

	if err := ms.Route(ctx, "/workspace/off/hidden.txt").
		WriteFile(ctx, []byte("hidden-none-token\n")); err != nil {
		t.Fatal(err)
	}

	res, err = idx.IndexPath(ctx, "/workspace/off/hidden.txt")
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("policy none: res=%q err=%v", res, err)
	}
	stats, err = idx.IndexPrefix(ctx, "/workspace/off", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("prefix none: %+v err=%v", stats, err)
	}

	// Byte-only provider has no Document IR — indexer streams the UTF-8 body.
	raw := &memFiles{files: map[string]string{"crlf.txt": "alpha stream-unique\r\nbeta line\n"}}
	idx2, err := vfsindex.NewMountIndexer(treeLocal(t, vfs.At("raw", raw.open)), eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	res, err = idx2.IndexPath(ctx, "/workspace/raw/crlf.txt")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("stream index: res=%q err=%v", res, err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "stream-unique"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("stream search: %+v err=%v", page.Objects, err)
	}
}

func keys(m map[string]brain.RichObject) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestMountIndexer_docsBlockText(t *testing.T) {
	ctx := context.Background()
	doc := vfs.NewRichDocument("/workspace/docs/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Spec", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "unique-doc-phrase"},
	})
	ms := treeLocal(t, vfs.At("docs", richFactory{doc: doc}.Open))
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}

	st, err := ms.Route(ctx, "/workspace/docs/Spec").
		Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := idx.IndexFile(ctx, "/workspace/docs/Spec", st)
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("index: %q err=%v", res, err)
	}
	parent := idx.DocumentID("/workspace/docs/Spec")
	children, err := eng.ListChildren(ctx, brain.Scope{Namespace: ns}, parent)
	if err != nil || len(children) != 2 {
		t.Fatalf("chunks=%d err=%v", len(children), err)
	}
	var sawPhrase bool
	for _, c := range children {
		obj, err := eng.Read(ctx, brain.Scope{Namespace: ns}, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(obj.Content, "unique-doc-phrase") && !strings.Contains(obj.Content, "<html") {
			sawPhrase = true
		}
	}
	if !sawPhrase {
		t.Fatal("expected Block.Text chunk, not projected HTML")
	}
	res, err = idx.IndexFile(ctx, "/workspace/docs/Spec", st)
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("version skip: %q err=%v", res, err)
	}
}

func TestMountIndexer_unindexLeavesFileThenReindexRecovers(t *testing.T) {
	ctx := t.Context()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/a.txt").
		WriteFile(ctx, []byte("recoverable-phrase\n")); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.IndexPath(ctx, "/workspace/work/a.txt"); err != nil {
		t.Fatal(err)
	}
	removed, err := idx.UnindexPath(ctx, "/workspace/work/a.txt")
	if err != nil || !removed {
		t.Fatalf("unindex: removed=%v err=%v", removed, err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/a.txt").
		ReadFile(ctx); err != nil {
		t.Fatalf("vfs file must remain: %v", err)
	}

	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "recoverable-phrase"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) != 0 {
		t.Fatalf("search after unindex: %+v err=%v", page, err)
	}
	if _, err := idx.IndexPath(ctx, "/workspace/work/a.txt"); err != nil {
		t.Fatal(err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "recoverable-phrase"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("reindex recovery: %+v err=%v", page, err)
	}
}

func TestMountIndexer_nonePolicySkippedAndMaxFilesStops(t *testing.T) {
	ctx := t.Context()
	ms := treeLocal(t,
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("off", vfs.Local(t.TempDir())).Indexed("none"),
	)
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/a.txt").
		WriteFile(ctx, []byte("one\n")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/b.txt").
		WriteFile(ctx, []byte("two\n")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/off/x.txt").
		WriteFile(ctx, []byte("hidden\n")); err != nil {
		t.Fatal(err)
	}

	stats, err := idx.IndexPrefix(ctx, "/workspace/off", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("none prefix: %+v err=%v", stats, err)
	}
	stats, err = idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{MaxFiles: 1})
	if err != nil || stats.Indexed != 1 {
		t.Fatalf("max files: %+v err=%v", stats, err)
	}
}

// cancelOnEmbed cancels the index's ctx on its first Embed call (the parent).
type cancelOnEmbed struct{ cancel context.CancelFunc }

func (e cancelOnEmbed) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.cancel()
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1}
	}
	return out, nil
}

func (cancelOnEmbed) Model() string { return "cancel-model" }

func TestMountIndexer_aMarkdownIndexStopsAtTheFirstRejectedOrCancelledSection(t *testing.T) {
	small := "# Small\n\nshort body\n"
	big := "# Big\n\n" + strings.Repeat("section body words\n", 20)
	for name, tc := range map[string]struct {
		body   string
		cancel bool
		want   string
	}{
		"a small section of an unknown kind": {small, false, "Unregistered"},
		"a big section of an unknown kind":   {big, false, "Unregistered"},
		"a big section after a cancel":       {big, true, "context canceled"},
	} {
		ctx, cancel := context.WithCancel(t.Context())
		ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))
		var emb brain.Embedding
		if tc.cancel {
			emb.Embedder = cancelOnEmbed{cancel}
		}
		eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(emb)))
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
			t.Fatal(err)
		}
		idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())})
		if err != nil {
			t.Fatal(err)
		}
		idx.Chunking = tinyChunks
		if !tc.cancel {
			idx.ChunkKind = "Unregistered"
		}
		if err := ms.Route(ctx, "/workspace/work/guide.md").WriteFile(ctx, []byte(tc.body)); err != nil {
			t.Fatal(err)
		}

		_, err = idx.IndexPath(ctx, "/workspace/work/guide.md")
		cancel()

		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: index = %v, want %q", name, err, tc.want)
		}
	}
}

type richFactory struct{ doc vfs.Document }

func (f richFactory) Open(context.Context, string, vfs.Binding) (vfs.Provider, error) {
	return richProvider(f), nil
}

type richProvider struct{ doc vfs.Document }

func (richProvider) Validate(context.Context) error { return nil }
func (p richProvider) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	if name == "" {
		return vfs.FileInfo{Name: ".", IsDir: true}, nil
	}
	return vfs.FileInfo{Name: "Spec", MediaType: "application/vnd.google-apps.document", Version: "v1"}, nil
}
func (richProvider) OpenFile(context.Context, string, int, fs.FileMode) (vfs.File, error) {
	return nil, vfs.ErrNotSupported
}
func (p richProvider) ReadDir(_ context.Context, name string) ([]vfs.DirEntry, error) {
	if name != "" {
		return nil, vfs.ErrNotExist
	}
	return []vfs.DirEntry{{Name: "Spec"}}, nil
}
func (richProvider) Remove(context.Context, string) error                { return vfs.ErrNotSupported }
func (richProvider) MkdirAll(context.Context, string, fs.FileMode) error { return vfs.ErrNotSupported }
func (p richProvider) OpenDocument(_ context.Context, _ string, _ *vfs.ContentRegistry) (vfs.Document, error) {
	return p.doc, nil
}
func (richProvider) WriteDocument(context.Context, string, vfs.Document) error {
	return vfs.ErrNotSupported
}

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}
