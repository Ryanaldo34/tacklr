package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// indexFixture is a brain with the index kinds, a workspace with a work
// mount, and the scope the index tools write in.
type indexFixture struct {
	eng   *brain.Engine
	ms    *vfs.MountSession
	scope brain.Scope
}

func newIndexFixture(t *testing.T, opts ...brain.EngineOption) indexFixture {
	t.Helper()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(t.Context(), vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ms := mustMountTree(t, "vfs-idx-tools", vfs.At("work", vfs.Local(t.TempDir())), vfs.At("off", vfs.Local(t.TempDir())).NotIndexed())
	return indexFixture{eng: eng, ms: ms, scope: brain.Scope{Namespace: storetest.Namespace(t, "id", uuid.NewString())}}
}

// tools are index_file and unindex on the fixture, with readers resolving
// each file's source ACL.
func (f indexFixture) tools(readers vfsindex.ReadersFunc) map[string]tool {
	v := vfsIndexTools{idx: &vfsindex.MountIndexer{VFS: f.ms, Brain: f.eng, Scope: f.scope, Readers: readers}}
	return map[string]tool{"index_file": handle(v.indexFile), "unindex": handle(v.unindex)}
}

func (f indexFixture) write(t *testing.T, files map[string]string) {
	t.Helper()
	for p, body := range files {
		if err := f.ms.Route(t.Context(), p).WriteFile(t.Context(), []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
}

func titles(t *testing.T, eng *brain.Engine, scope brain.Scope, query string) []string {
	t.Helper()
	page, err := eng.Search(t.Context(), scope, brain.SearchRequest{Query: query})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, o := range slices.Collect(page.Results.Objects()) {
		out = append(out, o.Properties[vfsindex.PropVFSPath].(string))
	}
	return out
}

// TestIndexTools_indexSkipUnindex: index_file makes a file searchable;
// indexing the same bytes again is skipped; unindex takes it out of search
// and keeps the file; indexing again restores it. A batch that names a
// directory indexes nothing.
func TestIndexTools_indexSkipUnindex(t *testing.T) {
	f := newIndexFixture(t)
	const path = "/workspace/work/note.txt"
	f.write(t, map[string]string{path: "alpha line\nbeta TODO findme-xyz\ngamma\n", "/workspace/work/batch.txt": "batch-unique-phrase-zzz\n"})
	tools := f.tools(nil)
	ctx := t.Context()

	_, noPath := tools["index_file"](ctx, `{}`)
	indexed, indexErr := tools["index_file"](ctx, `{"path":"`+path+`"}`)
	found := titles(t, f.eng, f.scope, "findme-xyz")
	skipped, _ := tools["index_file"](ctx, `{"path":"`+path+`"}`)
	unindexed, unindexErr := tools["unindex"](ctx, `{"path":"`+path+`"}`)
	gone := titles(t, f.eng, f.scope, "findme-xyz")
	_, statErr := f.ms.Route(ctx, path).Stat(ctx)
	noop, _ := tools["unindex"](ctx, `{"path":"`+path+`"}`)
	reindexed, _ := tools["index_file"](ctx, `{"path":"`+path+`"}`)
	back := titles(t, f.eng, f.scope, "findme-xyz")
	_, dirInBatch := tools["index_file"](ctx, `{"paths":["/workspace/work/batch.txt","/workspace/work"]}`)
	partial := titles(t, f.eng, f.scope, "batch-unique-phrase-zzz")

	if noPath == nil || !strings.Contains(noPath.Error(), "path or paths") {
		t.Errorf("index without a path = %v", noPath)
	}
	if indexErr != nil || !strings.Contains(indexed, "indexed path="+path) || !slices.Contains(found, path) {
		t.Errorf("index = %q %v, then search %v, want the file found", indexed, indexErr, found)
	}
	if !strings.Contains(skipped, "skipped path="+path) {
		t.Errorf("index unchanged bytes = %q, want skipped", skipped)
	}
	if unindexErr != nil || !strings.Contains(unindexed, "unindexed path="+path) || len(gone) != 0 || statErr != nil {
		t.Errorf("unindex = %q %v, then search %v and stat %v, want it out of search and the file kept", unindexed, unindexErr, gone, statErr)
	}
	if !strings.Contains(noop, "noop path="+path) {
		t.Errorf("unindex twice = %q, want noop", noop)
	}
	if !strings.Contains(reindexed, "indexed path="+path) || !slices.Contains(back, path) {
		t.Errorf("index after unindex = %q, then search %v, want it searchable again", reindexed, back)
	}
	if dirInBatch == nil || !strings.Contains(dirInBatch.Error(), "directory") || len(partial) != 0 {
		t.Errorf("batch with a directory = %v, indexed %v, want it rejected whole", dirInBatch, partial)
	}
}

// TestIndexTools_rejectBadRequestsAndReportEachFailure: index_file and
// unindex reject a path that is not absolute, a batch past the limit, a
// missing file and a mount that is not indexed; a file whose readers cannot
// be found is an error line in a batch that indexes the rest; a brain that
// cannot be read fails unindex.
func TestIndexTools_rejectBadRequestsAndReportEachFailure(t *testing.T) {
	var failReads atomic.Bool
	f := newIndexFixture(t, brain.WithAudit(func(_ context.Context, ev brain.AuditEvent) error {
		if failReads.Load() && ev.Op == brain.AuditRead {
			return errors.New("audit store down")
		}
		return nil
	}))
	f.write(t, map[string]string{"/workspace/work/open.txt": "open body\n", "/workspace/work/locked.txt": "locked body\n", "/workspace/off/a.txt": "a\n"})
	tools := f.tools(func(_ context.Context, p string, _ vfs.FileInfo) ([]string, error) {
		if strings.HasSuffix(p, "locked.txt") {
			return nil, errors.New("acl lookup failed")
		}
		return nil, nil
	})
	ctx := t.Context()
	nine, _ := json.Marshal(map[string]any{"paths": slices.Repeat([]string{"/workspace/work/open.txt"}, maxIndexFilePaths+1)})

	rejected := map[string]error{}
	_, rejected["a relative path"] = tools["index_file"](ctx, `{"path":"work/open.txt"}`)
	_, rejected["a relative path in a batch"] = tools["index_file"](ctx, `{"paths":["/workspace/work/open.txt","work/x"]}`)
	_, rejected["a batch past the limit"] = tools["index_file"](ctx, string(nine))
	_, rejected["a missing file"] = tools["index_file"](ctx, `{"path":"/workspace/work/none.txt"}`)
	_, rejected["a relative unindex"] = tools["unindex"](ctx, `{"path":"work/open.txt"}`)
	_, offMount := tools["index_file"](ctx, `{"path":"/workspace/off/a.txt"}`)
	batch, batchErr := tools["index_file"](ctx, `{"paths":["/workspace/work/open.txt","/workspace/work/locked.txt"]}`)
	failReads.Store(true)
	_, rejected["an unindex the brain cannot read"] = tools["unindex"](ctx, `{"path":"/workspace/work/open.txt"}`)

	for name, err := range rejected {
		if err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if offMount == nil || !strings.Contains(offMount.Error(), "indexing is off for the mount of /workspace/off/a.txt") {
		t.Errorf("index_file on a mount that is not indexed = %v", offMount)
	}
	lines := strings.Split(batch, "\n")
	if batchErr != nil || len(lines) != 2 || !strings.HasPrefix(lines[0], "indexed") || !strings.Contains(lines[1], "error path=/workspace/work/locked.txt") {
		t.Fatalf("batch = %q %v, want the open file indexed and an error line for the locked one", batch, batchErr)
	}
}
