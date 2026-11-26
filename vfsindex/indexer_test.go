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

// tinyChunks makes a few short lines span several chunks.
var tinyChunks = vfsindex.ChunkSizes{Min: 8, Avg: 16, Max: 32}

// TestNewMountIndexer_needsASessionAnEngineAndANamespace: an indexer
// without a mount session, a brain engine or a scope namespace does not
// build.
func TestNewMountIndexer_needsASessionAnEngineAndANamespace(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	_, noSession := vfsindex.NewMountIndexer(nil, h.eng, h.idx.Scope)
	_, noEngine := vfsindex.NewMountIndexer(h.ms, nil, h.idx.Scope)
	_, noNamespace := vfsindex.NewMountIndexer(h.ms, h.eng, brain.Scope{})

	if noSession == nil || noEngine == nil || noNamespace == nil {
		t.Fatalf("session = %v, engine = %v, namespace = %v, want each refused", noSession, noEngine, noNamespace)
	}
}

// TestMountIndexer_indexesAFileSoSearchFindsItsDocument: an indexed file is
// a Document at its path whose chunks hold the file, and a search for a
// word in it finds the Document.
func TestMountIndexer_indexesAFileSoSearchFindsItsDocument(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	const vpath = "/workspace/work/note.txt"
	body := "alpha line\nbeta TODO findme\ngamma line\ndelta line\n"
	h.write(t, "note.txt", body)

	res, err := h.idx.IndexPath(ctx, vpath)

	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("index = %q %v", res, err)
	}
	doc, err := h.eng.Get(ctx, h.idx.Scope, h.idx.DocumentID(vpath))
	if err != nil || doc.Properties[vfsindex.PropVFSPath] != vpath {
		t.Fatalf("document = %+v %v, want one at %s", doc, err, vpath)
	}
	if h.stored(vpath) != strings.TrimSuffix(body, "\n") {
		t.Fatalf("chunks = %q, want the file", h.stored(vpath))
	}
	page, err := h.eng.Search(ctx, h.idx.Scope, brain.SearchRequest{Query: "findme"})
	if hits := slices.Collect(page.Results.Objects()); err != nil || len(hits) == 0 || hits[0].ID != doc.ID {
		t.Fatalf("search = %+v %v, want the document", hits, err)
	}
}

// TestMountIndexer_skipsAFileWhoseVersionIsIndexed: an index of a file
// whose version is already indexed is skipped, by path or by its stat.
func TestMountIndexer_skipsAFileWhoseVersionIsIndexed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	const vpath = "/workspace/work/a.txt"
	h.write(t, "a.txt", "hello searchable-phrase\n")
	if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	st, err := h.ms.Route(ctx, vpath).Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}

	byPath, pathErr := h.idx.IndexPath(ctx, vpath)
	byStat, statErr := h.idx.IndexFile(ctx, vpath, st)

	if pathErr != nil || statErr != nil || byPath != vfsindex.PathSkipped || byStat != vfsindex.PathSkipped {
		t.Fatalf("by path = %q %v, by stat = %q %v, want skipped", byPath, pathErr, byStat, statErr)
	}
}

// TestMountIndexer_aRewriteReindexes: an index after the file changes
// replaces its chunks with the new text.
func TestMountIndexer_aRewriteReindexes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	h.idx.Chunking = tinyChunks
	const vpath = "/workspace/work/note.txt"
	h.write(t, "note.txt", "alpha line\nbeta line\ngamma line\n")
	if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	h.write(t, "note.txt", "rewritten uniquephrase\n")

	res, err := h.idx.IndexPath(ctx, vpath)

	if err != nil || res != vfsindex.PathIndexed || h.stored(vpath) != "rewritten uniquephrase" {
		t.Fatalf("reindex = %q %v, chunks %q, want the rewrite", res, err, h.stored(vpath))
	}
}

// TestMountIndexer_anEditToOneSectionKeepsTheOtherSectionsChunks: a
// Markdown edit gives the edited section a new chunk and a new source
// version, and the other sections keep theirs.
func TestMountIndexer_anEditToOneSectionKeepsTheOtherSectionsChunks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	const vpath = "/workspace/work/guide.md"
	sections := func() (map[string]uuid.UUID, string) {
		t.Helper()
		kids, err := listChildren(ctx, h.eng, h.idx.Scope, h.idx.DocumentID(vpath))
		if err != nil {
			t.Fatal(err)
		}
		ids := map[string]uuid.UUID{}
		for _, k := range kids {
			obj, err := h.eng.Get(ctx, h.idx.Scope, k.ID)
			if err != nil {
				t.Fatal(err)
			}
			ids[obj.Properties[vfsindex.PropBlockID].(string)] = obj.ID
		}
		doc, err := h.eng.Get(ctx, h.idx.Scope, h.idx.DocumentID(vpath))
		if err != nil {
			t.Fatal(err)
		}
		version, _ := doc.Properties[vfsindex.PropSourceVersion].(string)
		return ids, version
	}
	h.write(t, "guide.md", "intro line\n\n# Title\n\n## Install\n\npip install x\n\n## API\n\ncall me\n")
	if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	before, beforeVersion := sections()
	h.write(t, "guide.md", "intro line\n\n# Title\n\n## Install\n\npip install y\n\n## API\n\ncall me\n")

	_, err := h.idx.IndexPath(ctx, vpath)
	after, afterVersion := sections()

	if err != nil || afterVersion == "" || afterVersion == beforeVersion {
		t.Fatalf("reindex = %v, source version %q -> %q, want a new version", err, beforeVersion, afterVersion)
	}
	if after["title/install"] == before["title/install"] || after["title/api"] != before["title/api"] || after["preamble"] != before["preamble"] {
		t.Fatalf("chunks %v -> %v, want only the install section new", before, after)
	}
}

// TestMountIndexer_aRemovedFileIsUnindexed: an index of a path whose file
// is gone removes its Document.
func TestMountIndexer_aRemovedFileIsUnindexed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	const vpath = "/workspace/work/note.txt"
	h.write(t, "note.txt", "soon gone\n")
	if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	if err := h.ms.Route(ctx, vpath).Remove(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := h.idx.IndexPath(ctx, vpath)
	_, getErr := h.eng.Get(ctx, h.idx.Scope, h.idx.DocumentID(vpath))

	if err != nil || res != vfsindex.PathRemoved || !errors.Is(getErr, brain.ErrNotFound) {
		t.Fatalf("index = %q %v, then get = %v, want removed", res, err, getErr)
	}
}

// TestMountIndexer_skipsWhatIsNotTextOrNotIndexed: an image, text or
// Markdown with a NUL byte or invalid UTF-8, and a file on a NotIndexed
// member are skipped, and a directory is a directory.
func TestMountIndexer_skipsWhatIsNotTextOrNotIndexed(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		file string
		body []byte
		want vfsindex.PathIndexResult
	}{
		"an image":                      {"work/x.png", []byte{0x89, 'P', 'N', 'G'}, vfsindex.PathSkipped},
		"text with a NUL":               {"work/binlike.txt", []byte("ok\x00null"), vfsindex.PathSkipped},
		"Markdown with a NUL":           {"work/binlike.md", []byte("# Head\n\nok\x00null"), vfsindex.PathSkipped},
		"text that is not UTF-8":        {"work/noutf8.txt", bytes.Repeat([]byte{0x80}, 600), vfsindex.PathSkipped},
		"Markdown that is not UTF-8":    {"work/noutf8.md", bytes.Repeat([]byte{0x80}, 600), vfsindex.PathSkipped},
		"a file on a NotIndexed member": {"off/hidden.txt", []byte("hidden-none-token\n"), vfsindex.PathSkipped},
		"a directory":                   {"work/dir/", nil, vfsindex.PathDirectory},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			h := newHarness(t)
			vpath := "/workspace/" + strings.TrimSuffix(c.file, "/")
			if strings.HasSuffix(c.file, "/") {
				if err := h.ms.Route(ctx, vpath).MkdirAll(ctx); err != nil {
					t.Fatal(err)
				}
			} else if err := h.ms.Route(ctx, vpath).WriteFile(ctx, c.body); err != nil {
				t.Fatal(err)
			}
			st, err := h.ms.Route(ctx, vpath).Stat(ctx)
			if err != nil {
				t.Fatal(err)
			}

			byPath, pathErr := h.idx.IndexPath(ctx, vpath)
			byStat, statErr := h.idx.IndexFile(ctx, vpath, st)

			if pathErr != nil || statErr != nil || byPath != c.want || byStat != c.want {
				t.Fatalf("by path = %q %v, by stat = %q %v, want %q", byPath, pathErr, byStat, statErr, c.want)
			}
		})
	}
}

// TestMountIndexer_refusesARelativePathAndAnEndedContext: every call fails
// for a path that is not absolute, and for a context that ended.
func TestMountIndexer_refusesARelativePathAndAnEndedContext(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	h := newHarness(t)
	h.write(t, "a.txt", "text\n")
	st, err := h.ms.Route(ctx, "/workspace/work/a.txt").Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]func(context.Context, string) error{
		"IndexPath":   func(ctx context.Context, p string) error { _, err := h.idx.IndexPath(ctx, p); return err },
		"IndexFile":   func(ctx context.Context, p string) error { _, err := h.idx.IndexFile(ctx, p, st); return err },
		"UnindexPath": func(ctx context.Context, p string) error { _, err := h.idx.UnindexPath(ctx, p); return err },
	}

	for name, call := range calls {
		relativeErr := call(ctx, "relative")
		canceledErr := call(canceled, "/workspace/work/a.txt")

		if relativeErr == nil || !errors.Is(canceledErr, context.Canceled) {
			t.Errorf("%s: relative = %v, canceled = %v", name, relativeErr, canceledErr)
		}
	}
}

// TestMountIndexer_zeroSettingsTakeTheDefaultsAndALongFileIndexesItsPrefix:
// an indexer with no chunk sizes, byte limit or kinds indexes with the
// defaults, and a file over MaxIndexBytes indexes its first bytes.
func TestMountIndexer_zeroSettingsTakeTheDefaultsAndALongFileIndexesItsPrefix(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	h.idx.Chunking, h.idx.MaxIndexBytes, h.idx.DocumentKind, h.idx.ChunkKind = vfsindex.ChunkSizes{}, 0, "", ""
	h.write(t, "a.txt", "hello defaults\n")
	if res, err := h.idx.IndexPath(ctx, "/workspace/work/a.txt"); err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("index with defaults = %q %v", res, err)
	}
	h.idx.MaxIndexBytes, h.idx.Chunking = 32, tinyChunks
	h.write(t, "long.txt", "head-unique-token\n"+strings.Repeat("wordline\n", 40)+"tail-unique-token\n")

	res, err := h.idx.IndexPath(ctx, "/workspace/work/long.txt")

	stored := h.stored("/workspace/work/long.txt")
	if err != nil || res != vfsindex.PathIndexed || !strings.Contains(stored, "head-unique-token") || strings.Contains(stored, "tail-unique-token") {
		t.Fatalf("index = %q %v, chunks %q, want the head only", res, err, stored)
	}
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
