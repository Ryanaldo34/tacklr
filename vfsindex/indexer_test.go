package vfsindex_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// TestMountIndexer_indexSearchAndNotify: VFS file → brain Document/Chunks →
// search finds content; hash skip; re-index after write; missing path removes.
func TestMountIndexer_indexSearchAndNotify(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("idx", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}

	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	idx.LinesPerChunk = 2

	body := "alpha line\nbeta TODO findme\ngamma line\ndelta line\n"
	if err := ms.WriteFile(ctx, "/work/note.txt", []byte(body)); err != nil {
		t.Fatal(err)
	}

	// IndexPrefix
	stats, err := idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 1 {
		t.Fatalf("stats: %+v", stats)
	}

	parentID := idx.DocumentID("/work/note.txt")
	doc, err := eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Properties[vfsindex.PropVFSPath] != "/work/note.txt" {
		t.Fatalf("vfs_path: %+v", doc.Properties)
	}
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// 4 lines, 2 per chunk → 2 chunks
	if len(children) != 2 {
		t.Fatalf("chunks=%d %#v", len(children), children)
	}
	// line anchors on first chunk
	c0, err := eng.Read(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if c0.Properties[vfsindex.PropStartLine] != float64(1) || c0.Properties[vfsindex.PropEndLine] != float64(2) {
		t.Fatalf("chunk0 props: %+v content=%q", c0.Properties, c0.Content)
	}
	// Search finds the parent via chunk content
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "findme"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != parentID {
		t.Fatalf("search: %+v", page.Objects)
	}
	// Chunk 0 is lines 1–2 and includes TODO
	if !strings.Contains(c0.Content, "TODO") {
		t.Fatalf("chunk0 content: %q", c0.Content)
	}

	// Hash skip
	stats, err = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
	if err != nil || stats.Skipped != 1 || stats.Indexed != 0 {
		t.Fatalf("hash skip stats: %+v err=%v", stats, err)
	}

	// AfterPersist + SyncScheduler re-index on write
	sched := vfsindex.NewSyncScheduler(idx)
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		return sched.Notify(ctx, path, vfsindex.ReasonSync)
	})
	if err := ms.WriteFile(ctx, "/work/note.txt", []byte("rewritten uniquephrase\n")); err != nil {
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
	if err := ms.Remove(ctx, "/work/note.txt"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexPath(ctx, "/work/note.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Read(ctx, scope, parentID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("want not found after remove, got %v", err)
	}

	// Binary skip by extension
	if err := ms.WriteFile(ctx, "/work/pic.png", []byte{0x89, 'P', 'N', 'G', 0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	stats, err = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("png skip: %+v err=%v", stats, err)
	}

	// MaxFiles
	if err := ms.WriteFile(ctx, "/work/c.txt", []byte("c\n")); err != nil {
		t.Fatal(err)
	}
	stats, err = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{MaxFiles: 1})
	if err != nil || stats.Indexed+stats.Skipped < 1 {
		t.Fatalf("max files: %+v err=%v", stats, err)
	}

	// IndexPath on directory is a no-op
	if err := idx.IndexPath(ctx, "/work"); err != nil {
		t.Fatal(err)
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
	if err := vfsindex.NewSyncScheduler(nil).Notify(ctx, "/work/x", vfsindex.ReasonExplicit); err != nil {
		t.Fatal(err)
	}
}

// TestMountIndexer_markdownBlocksChunks: Markdown is chunked by structured
// heading/preamble blocks with stable block_id props.
func TestMountIndexer_markdownBlocksChunks(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("idx-md", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}

	md := "intro line\n\n# Title\n\n## Install\n\npip install x\n\n## API\n\ncall me\n"
	if err := ms.WriteFile(ctx, "/work/guide.md", []byte(md)); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{}); err != nil {
		t.Fatal(err)
	}

	parentID := idx.DocumentID("/work/guide.md")
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
	// Stable id: re-index same content keeps same chunk UUIDs for block keys
	installID := install.ID
	if err := idx.IndexPath(ctx, "/work/guide.md"); err != nil {
		t.Fatal(err)
	}
	again, err := eng.Read(ctx, scope, installID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Properties[vfsindex.PropBlockID] != "title/install" {
		t.Fatalf("stable block chunk lost: %+v", again.Properties)
	}

	// Content change: same block_id props / chunk UUID; content + parent hash update
	parent, err := eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	oldHash, _ := parent.Properties[vfsindex.PropContentHash].(string)
	md2 := "intro line\n\n# Title\n\n## Install\n\npip install y\n\n## API\n\ncall me\n"
	if err := ms.WriteFile(ctx, "/work/guide.md", []byte(md2)); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexPath(ctx, "/work/guide.md"); err != nil {
		t.Fatal(err)
	}
	parent, err = eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	newHash, _ := parent.Properties[vfsindex.PropContentHash].(string)
	if newHash == "" || newHash == oldHash {
		t.Fatalf("content_hash should change after edit: old=%q new=%q", oldHash, newHash)
	}
	updated, err := eng.Read(ctx, scope, installID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Properties[vfsindex.PropBlockID] != "title/install" {
		t.Fatalf("block_id after reindex: %+v", updated.Properties)
	}
	if !strings.Contains(updated.Content, "pip install y") {
		t.Fatalf("chunk content should update: %q", updated.Content)
	}
}

// TestMountIndexer_markdownLineChunksWhenNoBlocks: structureless markdown
// falls back to line-window chunks (Blocks() empty).
func TestMountIndexer_markdownLineChunksWhenNoBlocks(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("idx-md-lines", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}

	// No ATX headings → Blocks() empty → lineChunksFromText path
	body := "line one\nline two\nline three\n"
	if err := ms.WriteFile(ctx, "/work/notes.md", []byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexPath(ctx, "/work/notes.md"); err != nil {
		t.Fatal(err)
	}

	parentID := idx.DocumentID("/work/notes.md")
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) < 1 {
		t.Fatalf("expected line-window chunks, got %d", len(children))
	}
	obj, err := eng.Read(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(obj.Content, "line one") {
		t.Fatalf("line chunk content: %q", obj.Content)
	}
	startL, _ := obj.Properties[vfsindex.PropStartLine].(float64)
	endL, _ := obj.Properties[vfsindex.PropEndLine].(float64)
	if startL < 1 || endL < startL {
		t.Fatalf("line chunk span: start=%v end=%v", startL, endL)
	}
}

func keys(m map[string]brain.RichObject) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
