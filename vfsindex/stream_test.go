package vfsindex_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// memFiles is a byte-only provider (no document path) whose Version follows
// content and a touch counter. Reads return 256 bytes at a time; midRead
// runs once, after the first 1 KiB of a read.
type memFiles struct {
	mu      sync.Mutex
	files   map[string]string
	touches int
	opens   int
	midRead func()
	openErr error // OpenFile fails
	readErr error // Read fails after the first 1 KiB
	noRead  bool  // OpenFile returns a handle without Read
	// unversioned makes Stat report no Version, as a backend that cannot tell.
	unversioned bool
}

func (m *memFiles) open(context.Context, string, vfs.Binding) (vfs.Provider, error) { return m, nil }

func (m *memFiles) put(name, body string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = body
}

func (m *memFiles) touch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.touches++
}

func (m *memFiles) openCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opens
}

func (*memFiles) Validate(context.Context) error { return nil }

func (m *memFiles) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name == "" {
		return vfs.FileInfo{Name: ".", IsDir: true}, nil
	}
	body, ok := m.files[name]
	if !ok {
		return vfs.FileInfo{}, vfs.ErrNotExist
	}
	sum := sha256.Sum256([]byte(body))
	fi := vfs.FileInfo{Name: name, Size: int64(len(body)), MediaType: vfs.DetectMediaType(name, nil)}
	if !m.unversioned {
		fi.Version = fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:8]), m.touches)
	}
	return fi, nil
}

func (m *memFiles) OpenFile(ctx context.Context, name string, _ int, _ fs.FileMode) (vfs.File, error) {
	st, err := m.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opens++
	switch {
	case m.openErr != nil:
		return nil, m.openErr
	case m.noRead:
		return statOnly{st}, nil
	}
	return &slowFile{data: m.files[name], fi: st, m: m}, nil
}

// statOnly is an open file that cannot be read.
type statOnly struct{ fi vfs.FileInfo }

func (statOnly) Close() error                  { return nil }
func (f statOnly) Stat() (vfs.FileInfo, error) { return f.fi, nil }

func (m *memFiles) ReadDir(context.Context, string) ([]vfs.DirEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]vfs.DirEntry, 0, len(m.files))
	for n := range m.files {
		out = append(out, vfs.DirEntry{Name: n})
	}
	return out, nil
}

func (*memFiles) Remove(context.Context, string) error                { return vfs.ErrNotSupported }
func (*memFiles) MkdirAll(context.Context, string, fs.FileMode) error { return vfs.ErrNotSupported }

type slowFile struct {
	data string
	off  int
	fi   vfs.FileInfo
	m    *memFiles
}

func (f *slowFile) Close() error                { return nil }
func (f *slowFile) Stat() (vfs.FileInfo, error) { return f.fi, nil }
func (f *slowFile) Read(p []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	if f.off > 1<<10 && f.m.readErr != nil {
		return 0, f.m.readErr
	}
	n := copy(p, f.data[f.off:min(f.off+256, len(f.data))])
	f.off += n
	if f.off > 1<<10 && f.m.midRead != nil {
		hook := f.m.midRead
		f.m.midRead = nil
		hook()
	}
	return n, nil
}

// countEmbedder counts the texts it embeds.
type countEmbedder struct {
	mu    sync.Mutex
	texts int
}

func (e *countEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.texts += len(texts)
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 0}
	}
	return out, nil
}

func (*countEmbedder) Model() string { return "count-model" }

func (e *countEmbedder) take() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.texts
	e.texts = 0
	return n
}

// memHarness indexes a memFiles mount into a fresh engine.
type memHarness struct {
	idx   *vfsindex.MountIndexer
	eng   *brain.Engine
	store *brain.MemoryStore
	emb   *countEmbedder
}

// memIndexer mounts files at /workspace/raw and indexes into a fresh engine.
func memIndexer(t *testing.T, files *memFiles) memHarness {
	t.Helper()
	ms := treeLocal(t, vfs.At("raw", files.open))
	emb := &countEmbedder{}
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	return memHarness{idx: idx, eng: eng, store: store, emb: emb}
}

func numberedLines(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "line %03d records a distinct fact about topic %d\n", i, i*7)
	}
	return b.String()
}

func TestMountIndexer_anEditReembedsOnlyTheChunksAroundIt(t *testing.T) {
	ctx := t.Context()
	body := numberedLines(400)
	files := &memFiles{files: map[string]string{"big.txt": body}}
	h := memIndexer(t, files)
	idx, eng, store, emb := h.idx, h.eng, h.store, h.emb
	const vpath = "/workspace/raw/big.txt"
	if _, err := idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	first := emb.take()
	edited := "inserted-needle line at the top\n" + body
	files.put("big.txt", edited)

	_, err := idx.IndexPath(ctx, vpath)
	second := emb.take()
	kids, _ := store.ListChildren(ctx, idx.Scope, idx.DocumentID(vpath))
	page, _ := eng.Search(ctx, idx.Scope, brain.SearchRequest{Query: "inserted-needle"}, brain.NewSearchContext())

	if err != nil || first < 8 {
		t.Fatalf("index = %v, first run embedded %d texts, want one per chunk", err, first)
	}
	// The parent, the chunk with the new line, and at most one neighbor.
	if second > 3 {
		t.Errorf("edit embedded %d texts of %d, want at most 3", second, first)
	}
	if got := joinStored(kids); got != strings.TrimSuffix(edited, "\n") {
		t.Errorf("chunks no longer cover the file in order")
	}
	if end := kids[len(kids)-1].Properties[vfsindex.PropEndLine]; end != float64(401) {
		t.Errorf("last chunk ends at line %v, want 401 after the insert", end)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != idx.DocumentID(vpath) {
		t.Errorf("search for the inserted line = %v", page.Objects)
	}
}

func TestMountIndexer_anUnchangedVersionSkipsTheFileUnreadAndANewVersionOfTheSameTextEmbedsOnlyTheParent(t *testing.T) {
	ctx := t.Context()
	files := &memFiles{files: map[string]string{"a.txt": numberedLines(100)}}
	h := memIndexer(t, files)
	idx, emb := h.idx, h.emb
	const vpath = "/workspace/raw/a.txt"
	if _, err := idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	opens := files.openCount()
	emb.take()

	same, sameErr := idx.IndexPath(ctx, vpath)
	reads := files.openCount() - opens
	files.touch()
	touched, touchErr := idx.IndexPath(ctx, vpath)

	if sameErr != nil || same != vfsindex.PathSkipped || reads != 0 {
		t.Errorf("same version = %q %v after %d opens, want skipped unread", same, sameErr, reads)
	}
	if touchErr != nil || touched != vfsindex.PathIndexed || emb.take() != 1 {
		t.Errorf("new version, same text = %q %v, want indexed with only the parent embedded", touched, touchErr)
	}
}

func TestMountIndexer_aBackendWithoutVersionsOrDocumentsRereadsButRewritesNothing(t *testing.T) {
	ctx := t.Context()
	// Markdown on a byte-only backend: no document path, so it streams.
	files := &memFiles{unversioned: true, files: map[string]string{"notes.md": "# Notes\n\nunversioned-token body\n"}}
	h := memIndexer(t, files)
	const vpath = "/workspace/raw/notes.md"
	if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	h.emb.take()

	again, err := h.idx.IndexPath(ctx, vpath)
	embedded := h.emb.take()
	page, _ := h.eng.Search(ctx, h.idx.Scope, brain.SearchRequest{Query: "unversioned-token"}, brain.NewSearchContext())

	if err != nil || again != vfsindex.PathIndexed || embedded != 1 {
		t.Errorf("re-index without a version = %q %v, embedded %d texts, want indexed with only the parent embedded", again, err, embedded)
	}
	if len(page.Objects) != 1 {
		t.Errorf("search = %v, want the notes", page.Objects)
	}
}

func TestMountIndexer_aReindexThatAnotherIndexerCommitsFirstIsRetried(t *testing.T) {
	ctx := t.Context()
	files := &memFiles{files: map[string]string{"a.txt": numberedLines(100)}}
	h := memIndexer(t, files)
	idx, store := h.idx, h.store
	const vpath = "/workspace/raw/a.txt"
	latest := "rewritten while it was read\n" + numberedLines(50)
	// Partway through the first read, the file changes and another index commits.
	files.midRead = func() {
		files.put("a.txt", latest)
		if _, err := idx.IndexPath(ctx, vpath); err != nil {
			t.Errorf("inner index: %v", err)
		}
	}

	_, err := idx.IndexPath(ctx, vpath)
	kids, _ := store.ListChildren(ctx, idx.Scope, idx.DocumentID(vpath))

	if err != nil {
		t.Fatalf("index after another indexer committed first = %v, want a retry", err)
	}
	if got := joinStored(kids); got != strings.TrimSuffix(latest, "\n") {
		t.Errorf("chunks after the retry do not hold the latest text")
	}
}

func TestMountIndexer_chunkSizesOutOfOrderFailTheIndex(t *testing.T) {
	files := &memFiles{files: map[string]string{"a.txt": "text\n"}}
	idx := memIndexer(t, files).idx
	idx.Chunking = vfsindex.ChunkSizes{Min: 64, Avg: 32}

	_, err := idx.IndexPath(t.Context(), "/workspace/raw/a.txt")

	if err == nil || !strings.Contains(err.Error(), "Min < Avg <= Max") {
		t.Fatalf("index with Min > Avg = %v", err)
	}
}

func TestMountIndexer_aFileThatCannotBeReadOrChunkedFailsTheIndexAndKeepsThePreviousOne(t *testing.T) {
	const vpath = "/workspace/raw/a.txt"
	for name, tc := range map[string]struct {
		arrange func(context.CancelFunc, *memFiles, memHarness)
		want    string
	}{
		"the file fails to open": {func(_ context.CancelFunc, m *memFiles, _ memHarness) { m.openErr = errors.New("gateway timeout") }, "gateway timeout"},
		"the file has no reader": {func(_ context.CancelFunc, m *memFiles, _ memHarness) { m.noRead = true }, "not readable"},
		"a read fails midway":    {func(_ context.CancelFunc, m *memFiles, _ memHarness) { m.readErr = errors.New("connection reset") }, "connection reset"},
		"the caller cancels":     {func(cancel context.CancelFunc, m *memFiles, _ memHarness) { m.midRead = cancel }, "context canceled"},
		"the chunk kind is unknown": {func(_ context.CancelFunc, _ *memFiles, h memHarness) {
			if err := h.eng.ApplyKinds(t.Context(), vfsindex.MountIndexKinds()...); err != nil {
				t.Fatal(err)
			}
			h.idx.ChunkKind = "Unregistered"
		}, "Unregistered"},
	} {
		ctx, cancel := context.WithCancel(t.Context())
		files := &memFiles{files: map[string]string{"a.txt": numberedLines(100)}}
		h := memIndexer(t, files)
		if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
			t.Fatal(err)
		}
		files.put("a.txt", numberedLines(3000))
		tc.arrange(cancel, files, h)

		_, err := h.idx.IndexPath(ctx, vpath)
		kids, _ := h.store.ListChildren(t.Context(), h.idx.Scope, h.idx.DocumentID(vpath))
		cancel()

		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: index = %v, want %q", name, err, tc.want)
		}
		if got := joinStored(kids); got != strings.TrimSuffix(numberedLines(100), "\n") {
			t.Errorf("%s: the previous chunks were replaced", name)
		}
	}
}

func TestMountIndexer_anAuditFailureOnTheVersionCheckFailsTheIndex(t *testing.T) {
	ctx := t.Context()
	ms := treeLocal(t, vfs.At("raw", (&memFiles{files: map[string]string{"a.txt": "text\n"}}).open))
	audit := func(_ context.Context, ev brain.AuditEvent) error {
		if ev.Op == brain.AuditRead {
			return errors.New("audit sink down")
		}
		return nil
	}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(audit))
	if err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.IndexPath(ctx, "/workspace/raw/a.txt"); err != nil {
		t.Fatal(err)
	}

	_, err = idx.IndexPath(ctx, "/workspace/raw/a.txt")

	if err == nil || !strings.Contains(err.Error(), "audit sink down") {
		t.Fatalf("re-index with a failing audit sink = %v", err)
	}
}

func joinStored(kids []brain.Object) string {
	parts := make([]string, len(kids))
	for i, k := range kids {
		parts[i] = k.Content
	}
	return strings.Join(parts, "\n")
}
