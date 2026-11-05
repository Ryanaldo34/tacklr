package vfsindex_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/faultproxy"
	"github.com/ryanaldo34/tacklr/internal/s3test"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/awss3"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func treeLocal(t *testing.T, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(members...)(context.Background(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// TestMountIndexer_indexSearchAndNotify: VFS file → brain Document/Chunks →
// search finds content; hash skip; re-index after write; missing path removes.
func TestMountIndexer_indexSearchAndNotify(t *testing.T) {
	t.Parallel()
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
	ns := storetest.Namespace(t, "id", uuid.NewString())
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

	if res, err := idx.IndexPath(ctx, "/workspace/work/note.txt"); err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("index = %v %v", res, err)
	}

	parentID := idx.DocumentID("/workspace/work/note.txt")
	doc, err := eng.Get(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Properties[vfsindex.PropVFSPath] != "/workspace/work/note.txt" {
		t.Fatalf("vfs_path: %+v", doc.Properties)
	}
	children, err := listChildren(ctx, eng, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// The chunks cover lines 1–4 in order, with no gap or overlap.
	if got := joinChunks(t, eng, scope, children); got != strings.TrimSuffix(body, "\n") {
		t.Fatalf("chunks joined = %q, want the file", got)
	}
	c0, err := eng.Get(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	// Search finds the parent via chunk content
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "findme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(page.Results.Objects())) == 0 || slices.Collect(page.Results.Objects())[0].ID != parentID {
		t.Fatalf("search: %+v", slices.Collect(page.Results.Objects()))
	}
	// Chunk 0 starts at line 1 and includes TODO
	if c0.Properties[vfsindex.PropStartLine] != float64(1) || !strings.Contains(c0.Content, "TODO") {
		t.Fatalf("chunk0 content: %q", c0.Content)
	}

	// Version skip
	if res, err := idx.IndexPath(ctx, "/workspace/work/note.txt"); err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("version skip = %v %v", res, err)
	}

	// A rewrite re-indexes with IndexPath.
	if err := ms.Route(ctx, "/workspace/work/note.txt").
		WriteFile(ctx, []byte("rewritten uniquephrase\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.IndexPath(ctx, "/workspace/work/note.txt"); err != nil {
		t.Fatal(err)
	}

	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "uniquephrase"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(page.Results.Objects())) == 0 {
		t.Fatal("expected the rewrite indexed")
	}
	// old content should shrink to one chunk
	children, err = listChildren(ctx, eng, scope, parentID)
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
	if _, err := eng.Get(ctx, scope, parentID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("want not found after remove, got %v", err)
	}

	// Binary skip by extension

	if err := ms.Route(ctx, "/workspace/work/pic.png").
		WriteFile(ctx, []byte{0x89, 'P', 'N', 'G', 0, 1, 2}); err != nil {
		t.Fatal(err)
	}

	if res, err := idx.IndexPath(ctx, "/workspace/work/pic.png"); err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("png = %v %v, want skipped", res, err)
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
	gapKids, err := listChildren(ctx, eng, scope, idx.DocumentID("/workspace/work/gap.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range gapKids {
		if obj, err := eng.Get(ctx, scope, k.ID); err != nil || strings.TrimSpace(obj.Content) == "" {
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
	t.Parallel()
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
	ns := storetest.Namespace(t, "id", uuid.NewString())
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

	if _, err := idx.IndexPath(ctx, "/workspace/work/guide.md"); err != nil {
		t.Fatal(err)
	}

	parentID := idx.DocumentID("/workspace/work/guide.md")
	children, err := listChildren(ctx, eng, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// preamble + title + title/install + title/api
	if len(children) != 4 {
		t.Fatalf("expected 4 block chunks, got %d", len(children))
	}

	byBlock := map[string]brain.Object{}
	for _, ch := range children {
		obj, err := eng.Get(ctx, scope, ch.ID)
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
	parent, err := eng.Get(ctx, scope, parentID)
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
	parent, err = eng.Get(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	newVersion, _ := parent.Properties[vfsindex.PropSourceVersion].(string)
	if newVersion == "" || newVersion == oldVersion {
		t.Fatalf("source_version should change after edit: old=%q new=%q", oldVersion, newVersion)
	}
	children, err = listChildren(ctx, eng, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]uuid.UUID{}
	for _, ch := range children {
		obj, err := eng.Get(ctx, scope, ch.ID)
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
	bigKids, err := listChildren(ctx, eng, scope, idx.DocumentID("/workspace/work/big.md"))
	if err != nil || len(bigKids) < 2 {
		t.Fatalf("big section chunks = %d %v, want several", len(bigKids), err)
	}
	last, err := eng.Get(ctx, scope, bigKids[len(bigKids)-1].ID)
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
		obj, err := eng.Get(t.Context(), scope, ch.ID)
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
	t.Parallel()
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
	ns := storetest.Namespace(t, "id", uuid.NewString())
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
	children, err := listChildren(ctx, eng, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 0 {
		t.Fatalf("empty md chunks=%d want 0", len(children))
	}
	if _, err := eng.Get(ctx, scope, parentID); err != nil {
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
	children, err = listChildren(ctx, eng, scope, plainID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("preamble-only md chunks=%d want 1", len(children))
	}
	obj, err := eng.Get(ctx, scope, children[0].ID)
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
	t.Parallel()
	ctx := context.Background()
	ms := treeLocal(t,
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("off", vfs.Local(t.TempDir())).NotIndexed(),
	)
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "id", uuid.NewString())
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
	var page brain.Page

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
		"noutf8.md":  bytes.Repeat([]byte{0x80}, 600),
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
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "head-unique-token"})
	if err != nil || len(slices.Collect(page.Results.Objects())) == 0 {
		t.Fatalf("truncated file should still index the prefix: %+v err=%v", slices.Collect(page.Results.Objects()), err)
	}

	// A NotIndexed member: IndexPath reports skipped (no Document written).

	if err := ms.Route(ctx, "/workspace/off/hidden.txt").
		WriteFile(ctx, []byte("hidden-none-token\n")); err != nil {
		t.Fatal(err)
	}

	res, err = idx.IndexPath(ctx, "/workspace/off/hidden.txt")
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("policy none: res=%q err=%v", res, err)
	}
	hst, err := ms.Route(ctx, "/workspace/off/hidden.txt").Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res, err := idx.IndexFile(ctx, "/workspace/off/hidden.txt", hst); err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("IndexFile on a NotIndexed member: res=%q err=%v", res, err)
	}

	// CRLF line ends stream like LF ones.
	if err := ms.Route(ctx, "/workspace/work/crlf.txt").
		WriteFile(ctx, []byte("alpha stream-unique\r\nbeta line\n")); err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexPath(ctx, "/workspace/work/crlf.txt")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("stream index: res=%q err=%v", res, err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "stream-unique"})
	if err != nil || len(slices.Collect(page.Results.Objects())) == 0 {
		t.Fatalf("stream search: %+v err=%v", slices.Collect(page.Results.Objects()), err)
	}
}

func keys(m map[string]brain.Object) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestMountIndexer_aMarkdownIndexStopsAtTheFirstRejectedOrCancelledSection(t *testing.T) {
	t.Parallel()
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
			emb.Embedder = &hookEmbedder{hook: cancel}
		}
		eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(emb)))
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
			t.Fatal(err)
		}
		idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: storetest.Namespace(t, "id", uuid.NewString())})
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

// hostBackend is a host's own provider: its files carry no version, its
// File has no Read, and it decodes Markdown itself into blocks without a
// line span.
type hostBackend struct{}

func (hostBackend) Validate(context.Context) error { return nil }

func (hostBackend) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: name, Size: 5, Mode: 0o644, MediaType: vfs.DetectMediaType(name, nil)}, nil
}

func (hostBackend) OpenFile(_ context.Context, name string, _ int, _ fs.FileMode) (vfs.File, error) {
	return hostFile{name: name}, nil
}

func (hostBackend) ReadDir(context.Context, string) ([]vfs.DirEntry, error) { return nil, nil }
func (hostBackend) Remove(context.Context, string) error                    { return vfs.ErrReadOnly }
func (hostBackend) MkdirAll(context.Context, string, fs.FileMode) error     { return vfs.ErrReadOnly }

func (hostBackend) OpenDocument(_ context.Context, name string, _ *vfs.ContentRegistry) (vfs.Document, error) {
	return spanless{vfs.NewTextDocument(name, "text/markdown", "utf-8", "# Notes\n\nbody\n")}, nil
}

func (hostBackend) WriteDocument(context.Context, string, vfs.Document, *vfs.ContentRegistry) error {
	return vfs.ErrReadOnly
}

// hostFile is an open host file that can only be closed and stat'd.
type hostFile struct{ name string }

func (hostFile) Close() error { return nil }
func (f hostFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: 5, Mode: 0o644, MediaType: "text/plain"}, nil
}

// spanless is a host Markdown document whose one block has no line span.
type spanless struct{ *vfs.IR }

func (spanless) Blocks() []vfs.Block {
	return []vfs.Block{{ID: "notes", Kind: vfs.BlockKindHeading, Text: "Notes"}}
}

// TestMountIndexer_aHostBackendIndexesWhatItCanRead: a host backend without
// versions always reads. A block without a line span indexes as an empty
// chunk, and a file the backend cannot read fails.
func TestMountIndexer_aHostBackendIndexesWhatItCanRead(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms := treeLocal(t, vfs.At("host", func(context.Context, string, mount.Binding) (vfs.Provider, error) { return hostBackend{}, nil }))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: storetest.Namespace(t, "id", uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}

	first, firstErr := idx.IndexPath(ctx, "/workspace/host/notes.md")
	again, againErr := idx.IndexPath(ctx, "/workspace/host/notes.md")
	_, plainErr := idx.IndexPath(ctx, "/workspace/host/plain.txt")

	if firstErr != nil || againErr != nil || first != vfsindex.PathIndexed || again != vfsindex.PathIndexed {
		t.Errorf("index of a host Markdown file twice = %q %v, %q %v, want indexed both times", first, firstErr, again, againErr)
	}
	if plainErr == nil || !strings.Contains(plainErr.Error(), "not readable") {
		t.Errorf("index of a host file without Read = %v, want not readable", plainErr)
	}
}

// TestMountIndexer_aFailingBackendOrStoreFailsTheIndex: a read, stat or open
// the backend refuses, a body the server cuts in the middle, and a store
// whose pool is closed each fail the call with that error.
func TestMountIndexer_aFailingBackendOrStoreFailsTheIndex(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	closed, err := pgxpool.New(ctx, "postgres://tacklr@127.0.0.1:9/tacklr")
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	closedStore, err := postgres.Open(closed, postgres.Options{Schema: "closed", EmbeddingDim: 3})
	if err != nil {
		t.Fatal(err)
	}
	closedEngine, err := brain.NewEngine(closedStore)
	if err != nil {
		t.Fatal(err)
	}
	indexer := func(t *testing.T, ms *vfs.MountSession, eng *brain.Engine) *vfsindex.MountIndexer {
		t.Helper()
		idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: storetest.Namespace(t, "id", uuid.NewString())})
		if err != nil {
			t.Fatal(err)
		}
		return idx
	}
	memory := func(t *testing.T) *brain.Engine {
		t.Helper()
		eng, err := brain.NewEngine(brain.NewMemoryStore())
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
			t.Fatal(err)
		}
		return eng
	}
	// locked is a local mount whose name has no permissions.
	locked := func(t *testing.T, name string) *vfs.MountSession {
		t.Helper()
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "d"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "d", "x.txt"), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "guide.md"), []byte("# Guide\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filepath.Join(dir, name), 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, name), 0o755) })
		return treeLocal(t, vfs.At("work", vfs.Local(dir)))
	}

	for _, tc := range []struct {
		name string
		act  func(t *testing.T) error
		want string
	}{
		{name: "a body the server cuts in the middle", want: "unexpected EOF", act: func(t *testing.T) error {
			client, bucket := s3test.Start(t)
			body := strings.Repeat("a line of text to index\n", 4096)
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: new("log.txt"), Body: strings.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
			proxy, proxied := s3test.Proxy(t)
			proxy.Set(faultproxy.Rule{Match: func(r *http.Request) bool { return r.Method == http.MethodGet }, Fault: faultproxy.Cut(8 << 10)})
			ms := treeLocal(t, vfs.At("work", vfs.S3(awss3.API{Client: proxied}, bucket)))
			_, err := indexer(t, ms, memory(t)).IndexPath(ctx, "/workspace/work/log.txt")
			return err
		}},
		{name: "a directory without permissions", want: vfs.ErrPermission.Error(), act: func(t *testing.T) error {
			_, err := indexer(t, locked(t, "d"), memory(t)).IndexPath(ctx, "/workspace/work/d/x.txt")
			return err
		}},
		{name: "a Markdown file without permissions", want: vfs.ErrPermission.Error(), act: func(t *testing.T) error {
			_, err := indexer(t, locked(t, "guide.md"), memory(t)).IndexPath(ctx, "/workspace/work/guide.md")
			return err
		}},
		{name: "an unindex on a closed store", want: "closed pool", act: func(t *testing.T) error {
			_, err := indexer(t, treeLocal(t, vfs.At("work", vfs.Local(t.TempDir()))), closedEngine).UnindexPath(ctx, "/workspace/work/x.txt")
			return err
		}},
		{name: "an index of a removed file on a closed store", want: "closed pool", act: func(t *testing.T) error {
			_, err := indexer(t, treeLocal(t, vfs.At("work", vfs.Local(t.TempDir()))), closedEngine).IndexPath(ctx, "/workspace/work/x.txt")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.act(t)

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
