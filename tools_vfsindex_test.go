package tacklr

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// vfsIndexHarness builds a turn with Brain, a workspace, and a SearchNamespace.
// edit, when set, changes the options before the turn is built.
func vfsIndexHarness(t *testing.T, edit func(*AgentOptions)) (*TurnManager, *vfs.MountSession, *brain.Engine, brain.Namespace) {
	t.Helper()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(t.Context(), vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	ms := mustMountTree(t, "vfs-idx-tools", vfs.At("work", vfs.Local(t.TempDir())))
	opts := AgentOptions{
		sessionID:       "vfs-idx-tools",
		mountSession:    ms,
		Model:           &scriptedModel{},
		Brain:           eng,
		SearchNamespace: ns,
		AllowTools:      []string{"write"},
	}
	if edit != nil {
		edit(&opts)
	}
	h := mustNewTurnManager(t, opts)
	t.Cleanup(h.Close)
	return h, ms, eng, ns
}

func activatePlan(t *testing.T, h *TurnManager) {
	t.Helper()
	h.session.Plan.set([]Todo{{Title: "t", Description: "d", Status: TodoStatusPending}})
	if !h.session.Plan.HasActive() {
		t.Fatal("plan not active")
	}
}

func runWriteTool(t *testing.T, h *TurnManager, tool *Tool, argsJSON string) (string, error) {
	t.Helper()
	out, err := h.toolRunner.Run(context.Background(), ToolInvocation{
		Tool:     tool,
		ArgsJSON: argsJSON,
		Runtime:  turnRuntime(h),
	})
	return out.Output, err
}

// TestVFSIndexTools_indexSearchUnindex: index_file makes a file searchable,
// by its path and within it; indexing the same bytes again is skipped;
// unindex takes it out of search and keeps the file; indexing again restores
// it. A batch that names a directory indexes nothing.
func TestVFSIndexTools_indexSearchUnindex(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, nil)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: ns}
	const path = "/workspace/work/note.txt"
	for p, body := range map[string]string{path: "alpha line\nbeta TODO findme-xyz\ngamma\n", "/workspace/work/batch.txt": "batch-unique-phrase-zzz\n"} {
		if err := ms.Route(ctx, p).WriteFile(ctx, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	index := func(args string) (string, error) { return runWriteTool(t, h, h.findTool("index_file", ""), args) }
	unindex := func(args string) (string, error) { return runWriteTool(t, h, h.findTool("unindex", ""), args) }
	search := func(args string) (string, error) {
		out, err := h.findTool("search", "").invoke(ctx, args, turnRuntime(h))
		return out.Output, err
	}

	_, noPath := index(`{}`)
	indexed, indexErr := index(`{"path":"` + path + `"}`)
	found, foundErr := search(`{"query":"findme-xyz"}`)
	within, withinErr := search(`{"query":"findme-xyz","within":"` + path + `"}`)
	_, withinUnindexed := search(`{"query":"x","within":"/workspace/work/missing.txt"}`)
	skipped, _ := index(`{"path":"` + path + `"}`)
	unindexed, unindexErr := unindex(`{"path":"` + path + `"}`)
	gone := titlesOfPage(mustSearch(t, eng, scope, "findme-xyz"))
	_, statErr := ms.Route(ctx, path).Stat(ctx)
	noop, _ := unindex(`{"path":"` + path + `"}`)
	reindexed, _ := index(`{"path":"` + path + `"}`)
	back := mustSearch(t, eng, scope, "findme-xyz")
	_, dirInBatch := index(`{"paths":["/workspace/work/batch.txt","/workspace/work"]}`)
	partial := titlesOfPage(mustSearch(t, eng, scope, "batch-unique-phrase-zzz"))

	if noPath == nil || !strings.Contains(noPath.Error(), "path or paths") {
		t.Errorf("index without a path = %v", noPath)
	}
	if indexErr != nil || !strings.Contains(indexed, "indexed path="+path) {
		t.Errorf("index = %q %v", indexed, indexErr)
	}
	if foundErr != nil || !strings.Contains(found, path) {
		t.Errorf("search after index = %q %v, want the file's path", found, foundErr)
	}
	if withinErr != nil || !strings.Contains(within, "beta TODO findme-xyz") {
		t.Errorf("search within the file = %q %v, want the matching part", within, withinErr)
	}
	if withinUnindexed == nil || !strings.Contains(withinUnindexed.Error(), "not indexed") {
		t.Errorf("search within an unindexed path = %v", withinUnindexed)
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
	if hits := slices.Collect(back.Results.Objects()); !strings.Contains(reindexed, "indexed path="+path) || len(hits) == 0 || hits[0].Properties[vfsindex.PropVFSPath] != path {
		t.Errorf("index after unindex = %q, then search %+v, want it searchable again", reindexed, hits)
	}
	if dirInBatch == nil || !strings.Contains(dirInBatch.Error(), "directory") || len(partial) != 0 {
		t.Errorf("batch with a directory = %v, indexed %v, want it rejected whole", dirInBatch, partial)
	}
}

// TestVFSIndexTools_indexedFileACLFollowsSearchPrincipals: IndexReaders puts
// the file's source sharing on the index, and search returns it only to a
// session whose SearchPrincipals include one of those readers.
func TestVFSIndexTools_indexedFileACLFollowsSearchPrincipals(t *testing.T) {
	ctx := context.Background()
	readers := func(context.Context, string, vfs.FileInfo) ([]string, error) { return []string{"group:legal"}, nil }
	h, ms, eng, ns := vfsIndexHarness(t, func(o *AgentOptions) {
		o.IndexReaders = readers
		o.SearchPrincipals = []string{"user:ann", "group:legal"}
	})
	activatePlan(t, h)
	if err := ms.Route(ctx, "/workspace/work/nda.txt").WriteFile(ctx, []byte("mutual nda terms\n")); err != nil {
		t.Fatal(err)
	}

	if _, err := runWriteTool(t, h, h.findTool("index_file", ""), `{"path":"/workspace/work/nda.txt"}`); err != nil {
		t.Fatal(err)
	}
	legal, err := h.findTool("search", "").invoke(ctx, `{"query":"nda terms"}`, turnRuntime(h))
	outsider, _ := eng.Search(ctx, brain.Scope{Namespace: ns, Principals: []string{"user:mallory"}}, brain.SearchRequest{Query: "nda terms"})

	if err != nil || !strings.Contains(legal.Output, "nda.txt") {
		t.Fatalf("legal search: %v %s", err, legal.Output)
	}
	if len(slices.Collect(outsider.Results.Objects())) != 0 {
		t.Fatalf("outsider search = %+v", slices.Collect(outsider.Results.Objects()))
	}
}

func TestAgentOptions_brainRequiresASearchNamespace(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	opts := AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: eng}

	err = opts.Validate()

	if err == nil || !strings.Contains(err.Error(), "SearchNamespace is required") {
		t.Fatalf("validate = %v", err)
	}
}

// TestVFSIndexTools_refuseAMountThatIsNotIndexed: index_file on a NotIndexed
// mount fails the whole call, naming the path.
func TestVFSIndexTools_refuseAMountThatIsNotIndexed(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ms := mustMountTree(t, "not-indexed", vfs.At("work", vfs.Local(t.TempDir())), vfs.At("off", vfs.Local(t.TempDir())).NotIndexed())
	h := mustNewTurnManager(t, AgentOptions{sessionID: "not-indexed", mountSession: ms, Model: &scriptedModel{}, Brain: eng, SearchNamespace: mustNS(t, "id", uuid.NewString())})
	t.Cleanup(h.Close)
	activatePlan(t, h)
	if err := ms.Route(ctx, "/workspace/off/a.txt").WriteFile(ctx, []byte("a\n")); err != nil {
		t.Fatal(err)
	}

	_, err = runWriteTool(t, h, h.findTool("index_file", ""), `{"path":"/workspace/off/a.txt"}`)

	if err == nil || !strings.Contains(err.Error(), "indexing is off for the mount of /workspace/off/a.txt") {
		t.Fatalf("index_file on a mount that is not indexed = %v", err)
	}
}
