package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func vfsIndexHarness(t *testing.T, withNS bool) (*TurnManager, *vfs.MountSession, *brain.Engine, brain.Namespace) {
	t.Helper()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly())
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
		UnattendedWrite: true,
	}
	if withNS {
		opts.SearchNamespace = ns
	}
	h := mustNewTurnManager(t, opts)
	t.Cleanup(h.Close)
	return h, ms, eng, ns
}

func activatePlan(t *testing.T, h *TurnManager) {
	t.Helper()
	h.session.Plan.Set([]Todo{{Title: "t", Description: "d", Status: TodoStatusPending}})
	if !h.session.Plan.HasActive() {
		t.Fatal("plan not active")
	}
}

func runWriteTool(t *testing.T, h *TurnManager, tool *Tool, argsJSON string) (string, error) {
	t.Helper()
	out, _, err := h.toolRunner.Run(context.Background(), ToolInvocation{
		Tool:     tool,
		ArgsJSON: argsJSON,
		Runtime:  turnRuntime(h),
	})
	return out, err
}

func waitSearchHit(t *testing.T, eng *brain.Engine, scope brain.Scope, query string, timeout time.Duration) brain.RichObject {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: query}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Objects) > 0 {
			return page.Objects[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("search %q: no hit within %v", query, timeout)
	return brain.RichObject{}
}

// TestVFSIndexTools_indexSearchUnindex: index_file → searchable vfs_path;
// hash skip; unindex soft-deletes mirror; VFS file remains.
// The work mount is selective (empty IndexPolicy), so WriteFile does not
// auto-index; the first index_file writes, the second is a hash skip.
func TestVFSIndexTools_indexSearchUnindex(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, true)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: ns}

	indexTool := h.findTool("index_file", "")
	unindexTool := h.findTool("unindex", "")
	if indexTool == nil || unindexTool == nil {
		t.Fatal("index_file and unindex required when Brain, VFS, and namespace are set")
	}

	body := "alpha line\nbeta TODO findme-xyz\ngamma\n"

	if err := ms.Route(ctx, "/workspace/work/note.txt").
		WriteFile(ctx, []byte(body)); err != nil {
		t.Fatal(err)
	}

	if _, err := runWriteTool(t, h, indexTool, `{}`); err == nil || !strings.Contains(err.Error(), "path or paths") {
		t.Fatalf("index without path: %v", err)
	}

	out, err := runWriteTool(t, h, indexTool, `{"path":"/workspace/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "indexed path=/workspace/work/note.txt") {
		t.Fatalf("index: %q", out)
	}

	hit := waitSearchHit(t, eng, scope, "findme-xyz", 3*time.Second)
	if hit.Properties[vfsindex.PropVFSPath] != "/workspace/work/note.txt" {
		t.Fatalf("vfs_path: %+v", hit.Properties)
	}

	out, err = runWriteTool(t, h, indexTool, `{"path":"/workspace/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "skipped path=/workspace/work/note.txt") {
		t.Fatalf("skip: %q", out)
	}

	out, err = runWriteTool(t, h, unindexTool, `{"path":"/workspace/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "unindexed path=/workspace/work/note.txt") {
		t.Fatalf("unindex: %q", out)
	}

	if _, err := ms.Route(ctx, "/workspace/work/note.txt").
		Stat(ctx); err != nil {
		t.Fatal("VFS file must remain after unindex")
	}

	out, err = runWriteTool(t, h, unindexTool, `{"path":"/workspace/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "noop path=/workspace/work/note.txt") {
		t.Fatalf("noop: %q", out)
	}

	// Recovery: unindex resets hash-skip so the next index_file writes again.
	out, err = runWriteTool(t, h, indexTool, `{"path":"/workspace/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "indexed path=/workspace/work/note.txt") {
		t.Fatalf("reindex: %q", out)
	}
	hit2 := waitSearchHit(t, eng, scope, "findme-xyz", 3*time.Second)
	if hit2.Properties[vfsindex.PropVFSPath] != "/workspace/work/note.txt" {
		t.Fatalf("reindex vfs_path: %+v", hit2.Properties)
	}

	// Directory in a batch rejects before any IndexPath (no partial index).

	if err := ms.Route(ctx, "/workspace/work/batch-only.txt").
		WriteFile(ctx, []byte("batch-unique-phrase-zzz\n")); err != nil {
		t.Fatal(err)
	}

	_, err = runWriteTool(t, h, indexTool, `{"paths":["/workspace/work/batch-only.txt","/workspace/work"]}`)
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("index_file directory in batch: %v", err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "batch-unique-phrase-zzz"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) > 0 {
		t.Fatalf("partial batch index: %+v", page.Objects[0].Properties)
	}
}

// TestVFSIndexTools_selectiveIndexSearchReadAndTrack: index_file makes the
// file searchable. A later write is visible through read. The turn does not
// re-index that write.
func TestVFSIndexTools_selectiveIndexSearchReadAndTrack(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, true)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: ns}

	spec, err := ms.SpecAt("/workspace/work/x")
	if err != nil {
		t.Fatal(err)
	}
	if vfsindex.NormalizePolicy(spec.IndexPolicy) != vfsindex.PolicySelective {
		t.Fatalf("default policy: %q", spec.IndexPolicy)
	}

	body := "line one\nline two unique-phrase-selective-aaa\nline three\n"

	if err := ms.Route(ctx, "/workspace/work/sel.txt").
		WriteFile(ctx, []byte(body)); err != nil {
		t.Fatal(err)
	}

	if _, err := runWriteTool(t, h, h.findTool("index_file", ""), `{"path":"/workspace/work/sel.txt"}`); err != nil {
		t.Fatal(err)
	}
	hit := waitSearchHit(t, eng, scope, "unique-phrase-selective-aaa", 3*time.Second)
	if hit.Properties[vfsindex.PropVFSPath] != "/workspace/work/sel.txt" {
		t.Fatalf("search vfs_path: %+v", hit.Properties)
	}
	search := h.findTool("search", "")
	if search == nil {
		t.Fatal("search required")
	}
	sout, err := search.invoke(ctx, `{"query":"unique-phrase-selective-aaa"}`, turnRuntime(h))
	if err != nil || !strings.Contains(sout.output, "/workspace/work/sel.txt") {
		t.Fatalf("search after index: %q err=%v", sout.output, err)
	}
	readTool := h.findTool("read", "")
	if readTool == nil {
		t.Fatal("read required")
	}
	if h.findTool("read_object", "") == nil {
		t.Fatal("read_object required when Brain is on")
	}
	readOut, err := readTool.invoke(ctx, `{"path":"/workspace/work/sel.txt","start":1,"end":10}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(readOut.output, "unique-phrase-selective-aaa") {
		t.Fatalf("read after search: %s", readOut.output)
	}

	if err := ms.Route(ctx, "/workspace/work/sel.txt").
		WriteFile(ctx, []byte("unique-phrase-selective-bbb\n")); err != nil {
		t.Fatal(err)
	}
	readOut, err = readTool.invoke(ctx, `{"path":"/workspace/work/sel.txt","start":1,"end":10}`, turnRuntime(h))
	if err != nil || !strings.Contains(readOut.output, "unique-phrase-selective-bbb") {
		t.Fatalf("read after write: %v %s", err, readOut.output)
	}
}

// TestKnowledgeSaveSearchRead: save_* writes a brain record. Update-by-object_id
// rewrites that record. search and read_object return it.
func TestKnowledgeSaveSearchRead(t *testing.T) {
	ctx := context.Background()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly(), brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Discovery", IsParent: true},
		brain.KindSpec{Kind: "Fact", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, append(vfsindex.MountIndexKinds(),
		brain.KindSpec{Kind: "Discovery", IsParent: true},
		brain.KindSpec{Kind: "Fact", IsParent: true},
	)...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	h := mustNewTurnManager(t, AgentOptions{
		sessionID: "save-mem",
		Model:     &scriptedModel{},
		Brain:     eng, SearchNamespace: ns,
		BrainWriteKinds: brain.WriteKinds{Discovery: "Discovery", Fact: "Fact"},
	})
	t.Cleanup(h.Close)
	activatePlan(t, h)

	save := h.findTool("save_discovery", "")
	if save == nil {
		t.Fatal("save_discovery required")
	}
	if _, err := save.invoke(ctx, `{"content":"x"}`, turnRuntime(h)); err == nil || !strings.Contains(err.Error(), "title is required") {
		t.Fatalf("empty title: %v", err)
	}
	out, err := save.invoke(ctx, `{"title":"latency finding","content":"p99 under 40ms in canary"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		ID      string `json:"id"`
		Kind    string `json:"kind"`
		Title   string `json:"title"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(out.output), &res); err != nil {
		t.Fatal(err)
	}
	if res.ID == "" || res.Kind != "Discovery" || res.Title != "latency finding" || !strings.Contains(res.Content, "p99 under 40ms") {
		t.Fatalf("save result: %+v raw=%s", res, out.output)
	}
	id, err := uuid.Parse(res.ID)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := eng.Get(ctx, brain.Scope{Namespace: ns}, id)
	if err != nil || !strings.Contains(obj.Content, "p99 under 40ms in canary") || obj.Kind != "Discovery" {
		t.Fatalf("engine object: %+v err=%v", obj, err)
	}
	readObj := h.findTool("read_object", "")
	if readObj == nil {
		t.Fatal("read_object required")
	}
	rl, err := readObj.invoke(ctx, `{"object_id":"`+res.ID+`"}`, turnRuntime(h))
	if err != nil || !strings.Contains(rl.output, "p99 under 40ms") {
		t.Fatalf("read_object: %v %s", err, rl.output)
	}
	found := h.findTool("find_objects", "")
	if found == nil {
		t.Fatal("find_objects required")
	}
	fpage, err := found.invoke(ctx, `{"query":"latency finding"}`, turnRuntime(h))
	if err != nil || !strings.Contains(fpage.output, res.ID) {
		t.Fatalf("find_objects: %v %s", err, fpage.output)
	}

	updArgs, err := json.Marshal(map[string]any{
		"title":     "latency finding",
		"object_id": res.ID,
		"content":   "p95 under 20ms after fix",
	})
	if err != nil {
		t.Fatal(err)
	}
	out2, err := save.invoke(ctx, string(updArgs), turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var res2 struct {
		ID      string `json:"id"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(out2.output), &res2); err != nil {
		t.Fatal(err)
	}
	if res2.ID != res.ID || !strings.Contains(res2.Content, "p95 under 20ms") {
		t.Fatalf("update: %+v", res2)
	}
	obj2, err := eng.Get(ctx, brain.Scope{Namespace: ns}, id)
	if err != nil || !strings.Contains(obj2.Content, "p95 under 20ms") {
		t.Fatalf("updated engine: %+v err=%v", obj2, err)
	}
}

// TestRun_workspaceResearchTurn: host config (prompt, window, policy, watchdog,
// VFS, brain, namespace) plus a model that decides the next tool from the window.
func TestRun_workspaceResearchTurn(t *testing.T) {
	ctx := context.Background()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly(), brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Discovery", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, append(vfsindex.MountIndexKinds(),
		brain.KindSpec{Kind: "Discovery", IsParent: true},
	)...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	ms := mustMountTree(t, "research-turn", vfs.At("work", vfs.Local(t.TempDir())))

	wd := &recordingWatchdog{}
	strategy := &scriptedModel{}
	// The model keeps its own next-action (window pressure may drop tool text).
	var next int
	strategy.InvokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		prompt := strategy.LastSystemPrompt()
		if strings.Contains(strings.ToLower(prompt), "summarize the entire message history") {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "WINDOW_SUMMARY", IsComplete: true}
			return
		}
		if strings.Contains(prompt, "produce a handoff") {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "HANDOFF: index done, wrap-up remains", IsComplete: true}
			return
		}
		next++
		switch next {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("p1", "create_plan", `{"plan":"index then wrap up","todos":[{"title":"index","status":"pending","description":"write and index"},{"title":"wrap-up","status":"pending","description":"report"}]}`),
			}, IsComplete: true}
		case 2:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("w1", "write", `{"path":"/workspace/work/research.md","content":"# Notes\n\nunique-research-token for later search\n"}`),
			}, IsComplete: true}
		case 3:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("r1", "read", `{"path":"/workspace/work/research.md","start":1,"end":10}`),
			}, IsComplete: true}
		case 4:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("i0", "index_file", `{"path":"/workspace/work"}`),
			}, IsComplete: true}
		case 5:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("i1", "index_file", `{"path":"/workspace/work/research.md"}`),
			}, IsComplete: true}
		case 6:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("c1", "search", `{"query":"unique-research-token"}`),
			}, IsComplete: true}
		case 7:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("s1", "save_discovery", `{"title":"research token","content":"unique-research-token lives in /work/research.md"}`),
			}, IsComplete: true}
		case 8:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("q1", "search", `{"query":"unique-research-token"}`),
			}, IsComplete: true}
		case 9:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("t1", "complete_todo", `{"title":"index"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "indexed the note and saved a discovery", IsComplete: true}
		}
	}

	h := mustNewTurnManager(t, AgentOptions{
		sessionID:       "research-turn",
		MaxWindowSize:   400,
		SystemPrompt:    "You are a research agent. Prefer tools over guessing.",
		MaxTurnRequests: 20,
		ContextPolicy:   ContextPolicy{PressureRatio: 0.6, CompressFraction: 0.5},
		WatchDog:        wd,
		mountSession:    ms,
		Brain:           eng,
		SearchNamespace: ns,
		BrainWriteKinds: brain.WriteKinds{Discovery: "Discovery"},
		UnattendedWrite: true,
		Model:           strategy,
	})
	t.Cleanup(h.Close)
	if h.session.VFS != ms {
		t.Fatal("session VFS must be the host MountSession")
	}
	if _, ok := h.session.Search.Namespace(); !ok {
		t.Fatal("SearchNamespace must be set")
	}

	rt := turnRuntime(h)
	mustInvoke := func(name, args string) string {
		t.Helper()
		tool := h.findTool(name, "")
		if tool == nil {
			t.Fatalf("missing tool %s", name)
		}
		res, err := tool.invoke(ctx, args, rt)
		if err != nil && name != "index_file" {
			t.Fatalf("%s: %v", name, err)
		}
		if name == "index_file" && err != nil {
			return err.Error()
		}
		return res.output
	}
	mustInvoke("create_plan", `{"plan":"index then wrap up","todos":[{"title":"index","status":"pending","description":"write and index"},{"title":"wrap-up","status":"pending","description":"report"}]}`)
	mustInvoke("write", `{"path":"/workspace/work/research.md","content":"# Notes\n\nunique-research-token for later search\n"}`)
	readOut := mustInvoke("read", `{"path":"/workspace/work/research.md","start":1,"end":10}`)
	if !strings.Contains(readOut, "unique-research-token") {
		t.Fatalf("read: %s", readOut)
	}
	dirErr := mustInvoke("index_file", `{"path":"/workspace/work"}`)
	if !strings.Contains(strings.ToLower(dirErr), "directory") {
		t.Fatalf("expected directory error, got %q", dirErr)
	}
	idx := mustInvoke("index_file", `{"path":"/workspace/work/research.md"}`)
	if !strings.Contains(idx, "indexed path=/workspace/work/research.md") {
		t.Fatalf("index: %s", idx)
	}
	mustInvoke("search", `{"query":"unique-research-token"}`)
	save := mustInvoke("save_discovery", `{"title":"research token","content":"unique-research-token lives in /work/research.md"}`)
	if !strings.Contains(save, `"id"`) || !strings.Contains(save, "research token") {
		t.Fatalf("save: %s", save)
	}

	if body, err := ms.Route(ctx, "/workspace/work/research.md").
		ReadFile(ctx); err != nil || !strings.Contains(string(body), "unique-research-token") {
		t.Fatalf("vfs body: %s err=%v", body, err)
	}

	_ = waitSearchHit(t, eng, brain.Scope{Namespace: ns}, "unique-research-token", 3*time.Second)
	if h.session.Plan.Document() != "index then wrap up" {
		t.Fatalf("plan doc: %q", h.session.Plan.Document())
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestPathNativeGraphLinkExpand(t *testing.T) {
	ctx := context.Background()
	ms := mustMountTree(t, "path-graph", vfs.At("work", vfs.Local(t.TempDir())))
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	h := mustNewTurnManager(t, AgentOptions{
		sessionID:    "path-graph",
		mountSession: ms, Model: &scriptedModel{},
		Brain: eng, SearchNamespace: ns,
	})
	t.Cleanup(h.Close)
	activatePlan(t, h)

	if err := ms.Route(ctx, "/workspace/work/a.md").
		WriteFile(ctx, []byte("# A\n\napi doc\n")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/b.md").
		WriteFile(ctx, []byte("# B\n\nauth fact\n")); err != nil {
		t.Fatal(err)
	}

	idx := h.findTool("index_file", "")
	if _, err := runWriteTool(t, h, idx, `{"paths":["/workspace/work/a.md","/workspace/work/b.md"]}`); err != nil {
		t.Fatal(err)
	}

	link := h.findTool("link", "")
	if link == nil {
		t.Fatal("link required")
	}
	lout, err := link.invoke(ctx, `{
		"from":"/workspace/work/a.md","to":"/workspace/work/b.md",
		"relation_type":"references","note":"JWT section"
	}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lout.output, `/workspace/work/a.md`) || !strings.Contains(lout.output, `/workspace/work/b.md`) {
		t.Fatalf("link paths: %s", lout.output)
	}

	expand := h.findTool("expand", "")
	eout, err := expand.invoke(ctx, `{"path":"/workspace/work/a.md","relation_types":["references"]}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(eout.output, "/workspace/work/b.md") || !strings.Contains(eout.output, "JWT section") {
		t.Fatalf("expand path neighbor: %s", eout.output)
	}

	fl := h.findTool("find_links", "")
	if fl == nil {
		t.Fatal("find_links required with MemoryGraph")
	}
	fout, err := fl.invoke(ctx, `{"relation_type":"references","query":"JWT"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fout.output, "from_path") || !strings.Contains(fout.output, "to_path") {
		t.Fatalf("find_links path fields: %s", fout.output)
	}
	if !strings.Contains(fout.output, "/workspace/work/a.md") || !strings.Contains(fout.output, "/workspace/work/b.md") {
		t.Fatalf("find_links endpoints: %s", fout.output)
	}
	if _, err := fl.invoke(ctx, `{"relation_type":"","query":"JWT"}`, turnRuntime(h)); err == nil {
		t.Fatal("find_links requires relation_type")
	}
	if _, err := fl.invoke(ctx, `{"relation_type":"references","query":""}`, turnRuntime(h)); err == nil {
		t.Fatal("find_links requires query")
	}
}
