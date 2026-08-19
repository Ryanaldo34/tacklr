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

// TestMountIndexer_indexSearchAndNotify: VFS file → brain Document/Chunks →
// search finds content; hash skip; re-index after write; missing path removes.
func TestMountIndexer_indexSearchAndNotify(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("idx", reg)
	if err != nil {
		t.Fatal(err)
	}
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
	// IR line index: trailing \n is an empty last line (5 lines, 2 per chunk → 3).
	if len(children) != 3 {
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
	ms, err := vfs.NewMountSession("idx-md", reg)
	if err != nil {
		t.Fatal(err)
	}
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

// TestMountIndexer_emptyMarkdownLineChunks: empty .md has Blocks() nil, so
// index uses lineChunksFromText (not preamble-only structure).
func TestMountIndexer_emptyMarkdownLineChunks(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("idx-md-empty", reg)
	if err != nil {
		t.Fatal(err)
	}
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

	// Empty body → Blocks() nil → line-window path (no preamble block).
	if err := ms.WriteFile(ctx, "/work/empty.md", nil); err != nil {
		t.Fatal(err)
	}
	res, err := idx.IndexPathResult(ctx, "/work/empty.md")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("empty md index: res=%q err=%v", res, err)
	}
	parentID := idx.DocumentID("/work/empty.md")
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// lineStarts("") still yields one empty window; no block_id on line chunks.
	if len(children) != 1 {
		t.Fatalf("empty md chunks=%d want 1", len(children))
	}
	ch0, err := eng.Read(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, has := ch0.Properties[vfsindex.PropBlockID]; has {
		t.Fatalf("line-window chunk should not set block_id: %+v", ch0.Properties)
	}
	if _, err := eng.Read(ctx, scope, parentID); err != nil {
		t.Fatal(err)
	}

	// Non-empty, no headings → single preamble block (structure path, not lineChunks).
	if err := ms.WriteFile(ctx, "/work/plain.md", []byte("line one\nline two\nline three\n")); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexPath(ctx, "/work/plain.md"); err != nil {
		t.Fatal(err)
	}
	plainID := idx.DocumentID("/work/plain.md")
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

// TestMountIndexer_IndexFileResultAndDefaults: Stat-reuse API, binary skip,
// defaults for lines/bytes/kinds, and canceled context.
func TestMountIndexer_IndexFileResultAndDefaults(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("idx-file-result", reg)
	if err != nil {
		t.Fatal(err)
	}
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
	// Zero fields → defaults in indexFile
	idx.LinesPerChunk = 0
	idx.MaxIndexBytes = 0
	idx.DocumentKind = ""
	idx.ChunkKind = ""

	if err := ms.WriteFile(ctx, "/work/a.txt", []byte("hello searchable-phrase\n")); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/work/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	res, err := idx.IndexFileResult(ctx, "/work/a.txt", st)
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("IndexFileResult: res=%q err=%v", res, err)
	}
	res, err = idx.IndexFileResult(ctx, "/work/a.txt", st)
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("hash skip via IndexFileResult: res=%q err=%v", res, err)
	}

	// Directory FileInfo
	dst, err := ms.Stat(ctx, "/work")
	if err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexFileResult(ctx, "/work", dst)
	if err != nil || res != vfsindex.PathDirectory {
		t.Fatalf("dir IndexFileResult: res=%q err=%v", res, err)
	}

	if err := ms.WriteFile(ctx, "/work/pic.bin", []byte{0x00, 0x01, 0xff}); err != nil {
		t.Fatal(err)
	}
	// .bin may still be text-like via detect; use .png extension gate
	if err := ms.WriteFile(ctx, "/work/x.png", []byte{0x89, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}
	pst, err := ms.Stat(ctx, "/work/x.png")
	if err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexFileResult(ctx, "/work/x.png", pst)
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("png skip: res=%q err=%v", res, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := idx.IndexFileResult(canceled, "/work/a.txt", st); err == nil {
		t.Fatal("want ctx canceled error")
	}
	if _, err := idx.UnindexPath(canceled, "/work/a.txt"); err == nil {
		t.Fatal("unindex want ctx canceled")
	}
	if _, err := idx.IndexPathResult(canceled, "/work/a.txt"); err == nil {
		t.Fatal("IndexPathResult want ctx canceled")
	}

	// Bad virtual path
	if _, err := idx.IndexPathResult(ctx, "relative"); err == nil {
		t.Fatal("relative path")
	}
	if _, err := idx.IndexFileResult(ctx, "relative", st); err == nil {
		t.Fatal("IndexFileResult relative")
	}
	if _, err := idx.UnindexPath(ctx, "relative"); err == nil {
		t.Fatal("UnindexPath relative")
	}
	if _, err := idx.IndexPrefix(ctx, "relative", vfsindex.IndexOpts{}); err == nil {
		t.Fatal("IndexPrefix relative")
	}

	// Nested walk under prefix
	if err := ms.MkdirAll(ctx, "/work/sub"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/sub/nested.txt", []byte("nested-unique-token\n")); err != nil {
		t.Fatal(err)
	}
	stats, err := idx.IndexPrefix(ctx, "/work/sub", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed < 1 {
		t.Fatalf("nested prefix: %+v err=%v", stats, err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "nested-unique-token"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("nested search: %+v err=%v", page.Objects, err)
	}

	// Null byte in a .txt file → stream binary skip (not indexed as text)
	if err := ms.WriteFile(ctx, "/work/binlike.txt", []byte("ok\x00null")); err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexPathResult(ctx, "/work/binlike.txt")
	if err != nil {
		t.Fatal(err)
	}
	// skipped or indexed empty — either way no crash; prefer skipped
	if res != vfsindex.PathSkipped && res != vfsindex.PathIndexed {
		t.Fatalf("binary-ish: %q", res)
	}

	// MaxIndexBytes truncates stream chunking
	idx.MaxIndexBytes = 32
	idx.LinesPerChunk = 2
	long := strings.Repeat("wordline\n", 40)
	if err := ms.WriteFile(ctx, "/work/long.txt", []byte(long)); err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexPathResult(ctx, "/work/long.txt")
	if err != nil || (res != vfsindex.PathIndexed && res != vfsindex.PathSkipped) {
		t.Fatalf("long file: res=%q err=%v", res, err)
	}

	// PolicyNone mount: IndexPath / IndexPrefix report skipped (no Document written).
	if err := ms.Mount(ctx, vfs.MountSpec{
		Point: "/off", Profile: "scratch", IndexPolicy: vfsindex.PolicyNone,
		Params: map[string]string{"subpath": "off"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/off/hidden.txt", []byte("hidden-none-token\n")); err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexPathResult(ctx, "/off/hidden.txt")
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("policy none: res=%q err=%v", res, err)
	}
	stats, err = idx.IndexPrefix(ctx, "/off", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("prefix none: %+v err=%v", stats, err)
	}

	// Byte-only provider has no Document IR — indexer streams the UTF-8 body.
	bytesReg := vfs.NewBackendRegistry()
	if err := bytesReg.Register(streamFactory{files: map[string][]byte{
		"crlf.txt": []byte("alpha stream-unique\r\nbeta line\n"),
	}}); err != nil {
		t.Fatal(err)
	}
	ms2, err := vfs.NewMountSession("idx-stream", bytesReg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms2.Mount(ctx, vfs.MountSpec{Point: "/raw", Profile: "bytes"}); err != nil {
		t.Fatal(err)
	}
	idx2, err := vfsindex.NewMountIndexer(ms2, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	idx2.LinesPerChunk = 1
	res, err = idx2.IndexPathResult(ctx, "/raw/crlf.txt")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("stream index: res=%q err=%v", res, err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "stream-unique"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("stream search: %+v err=%v", page.Objects, err)
	}
}

// streamFactory is a Provider with Open/Stat only — no documentBackend.
// Indexer must hash+chunk via the byte stream.
type streamFactory struct{ files map[string][]byte }

func (streamFactory) Profile() string { return "bytes" }

func (f streamFactory) Open(context.Context, string, vfs.MountSpec) (vfs.Provider, error) {
	return streamProvider(f), nil
}

type streamProvider struct{ files map[string][]byte }

func (streamProvider) Validate(context.Context) error { return nil }

func (p streamProvider) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	if name == "" {
		return vfs.FileInfo{Name: ".", IsDir: true}, nil
	}
	data, ok := p.files[name]
	if !ok {
		return vfs.FileInfo{}, vfs.ErrNotExist
	}
	return vfs.FileInfo{Name: name, Size: int64(len(data)), MediaType: "text/plain"}, nil
}

func (p streamProvider) OpenFile(_ context.Context, name string, _ int, _ fs.FileMode) (vfs.File, error) {
	data, ok := p.files[name]
	if !ok {
		return nil, vfs.ErrNotExist
	}
	return &streamFile{r: bytes.NewReader(data), fi: vfs.FileInfo{
		Name: name, Size: int64(len(data)), MediaType: "text/plain",
	}}, nil
}

func (p streamProvider) ReadDir(_ context.Context, name string) ([]vfs.DirEntry, error) {
	if name != "" {
		return nil, vfs.ErrNotExist
	}
	out := make([]vfs.DirEntry, 0, len(p.files))
	for n := range p.files {
		out = append(out, vfs.DirEntry{Name: n})
	}
	return out, nil
}

func (streamProvider) Remove(context.Context, string) error { return vfs.ErrNotSupported }
func (streamProvider) MkdirAll(context.Context, string, fs.FileMode) error {
	return vfs.ErrNotSupported
}

type streamFile struct {
	r  *bytes.Reader
	fi vfs.FileInfo
}

func (f *streamFile) Close() error                { return nil }
func (f *streamFile) Stat() (vfs.FileInfo, error) { return f.fi, nil }
func (f *streamFile) Read(p []byte) (int, error)  { return f.r.Read(p) }

func keys(m map[string]brain.RichObject) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestMountIndexer_docsBlockText(t *testing.T) {
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	doc := vfs.NewRichDocument("/docs/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Spec", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "unique-doc-phrase"},
	})
	if err := reg.Register(richFactory{doc: doc}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("idx-docs", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/docs", Profile: "rich"}); err != nil {
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
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: &ns})
	if err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/docs/Spec")
	if err != nil {
		t.Fatal(err)
	}
	res, err := idx.IndexFileResult(ctx, "/docs/Spec", st)
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("index: %q err=%v", res, err)
	}
	parent := idx.DocumentID("/docs/Spec")
	children, err := eng.ListChildren(ctx, brain.Scope{Namespace: &ns}, parent)
	if err != nil || len(children) != 2 {
		t.Fatalf("chunks=%d err=%v", len(children), err)
	}
	var sawPhrase bool
	for _, c := range children {
		obj, err := eng.Read(ctx, brain.Scope{Namespace: &ns}, c.ID)
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
	res, err = idx.IndexFileResult(ctx, "/docs/Spec", st)
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("hash skip: %q err=%v", res, err)
	}
}

type richFactory struct{ doc vfs.Document }

func (richFactory) Profile() string { return "rich" }
func (f richFactory) Open(context.Context, string, vfs.MountSpec) (vfs.Provider, error) {
	return richProvider(f), nil
}

type richProvider struct{ doc vfs.Document }

func (richProvider) Validate(context.Context) error { return nil }
func (p richProvider) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	if name == "" {
		return vfs.FileInfo{Name: ".", IsDir: true}, nil
	}
	return vfs.FileInfo{Name: "Spec", MediaType: "application/vnd.google-apps.document"}, nil
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
