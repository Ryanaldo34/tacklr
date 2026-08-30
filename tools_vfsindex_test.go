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
		SessionID:       "vfs-idx-tools",
		MountSession:    ms,
		Model:           &mockStrategy{},
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
// WriteFile may race with AfterPersist async index, so index_file may return
// indexed or skipped; search + unindex outcomes are the contract.
func TestVFSIndexTools_indexSearchUnindex(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, true)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: ns}

	indexTool := h.findTool("index_file", "")
	unindexTool := h.findTool("unindex", "")
	if indexTool == nil || unindexTool == nil {
		t.Fatal("index_file and unindex required when Brain+VFS+ns")
	}

	body := "alpha line\nbeta TODO findme-xyz\ngamma\n"
	if err := ms.WriteFile(ctx, "/workspace/work/note.txt", []byte(body)); err != nil {
		t.Fatal(err)
	}

	if _, err := runWriteTool(t, h, indexTool, `{}`); err == nil || !strings.Contains(err.Error(), "path or paths") {
		t.Fatalf("index without path: %v", err)
	}

	out, err := runWriteTool(t, h, indexTool, `{"path":"/workspace/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "indexed path=/workspace/work/note.txt") && !strings.Contains(out, "skipped path=/workspace/work/note.txt") {
		t.Fatalf("index: %q", out)
	}

	hit := waitSearchHit(t, eng, scope, "findme-xyz", 3*time.Second)
	if hit.Properties[vfsindex.PropVFSPath] != "/workspace/work/note.txt" {
		t.Fatalf("vfs_path: %+v", hit.Properties)
	}

	// Second index same hash → skipped (async or explicit already wrote).
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
	if _, err := ms.Stat(ctx, "/workspace/work/note.txt"); err != nil {
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
	if err := ms.WriteFile(ctx, "/workspace/work/batch-only.txt", []byte("batch-unique-phrase-zzz\n")); err != nil {
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

// TestVFSIndexTools_selectiveIndexSearchReadAndTrack: index_file → search vfs_path
// → read live text; WriteFile after track reindexes the new phrase.
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
	if err := ms.WriteFile(ctx, "/workspace/work/sel.txt", []byte(body)); err != nil {
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

	if err := ms.WriteFile(ctx, "/workspace/work/sel.txt", []byte("unique-phrase-selective-bbb\n")); err != nil {
		t.Fatal(err)
	}
	_ = waitSearchHit(t, eng, scope, "unique-phrase-selective-bbb", 3*time.Second)
}

// TestVFSIndexTools_prefixAutoIndex: prefix policy indexes on persist without index_file.
func TestVFSIndexTools_prefixAutoIndex(t *testing.T) {
	ctx := context.Background()
	ms := mustMountTree(t, "policy-prefix", vfs.At("work", vfs.Local(t.TempDir())).Indexed("  Prefix  "))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "policy-prefix",
		MountSession: ms, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
	})
	t.Cleanup(h.Close)

	if err := ms.WriteFile(ctx, "/workspace/work/auto.txt", []byte("prefix-auto-phrase-xyz\n")); err != nil {
		t.Fatal(err)
	}
	_ = waitSearchHit(t, eng, brain.Scope{Namespace: ns}, "prefix-auto-phrase-xyz", 3*time.Second)
}

// TestKnowledgeSaveSearchRead: save_* writes an Engram on the brain Provider
// (not scratch /memory). Update-by-object_id rewrites the same path.
func TestKnowledgeSaveSearchRead(t *testing.T) {
	ctx := context.Background()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(g), brain.WithKinds(
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
	ms := mustMountTree(t, "save-mem",
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("engram", brain.Open(eng, brain.Scope{Namespace: ns})),
	)
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "save-mem",
		MountSession: ms, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
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
		Path     string `json:"path"`
		Rev      string `json:"rev"`
		ObjectID string `json:"object_id"`
		Kind     string `json:"kind"`
	}
	if err := json.Unmarshal([]byte(out.output), &res); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Path, "/workspace/engram/discovery/") || res.Rev == "" || res.ObjectID == "" || res.Kind != "Discovery" {
		t.Fatalf("save result: %+v raw=%s", res, out.output)
	}
	body, err := ms.ReadFile(ctx, res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "p99 under 40ms") || !strings.Contains(string(body), "domain: Discovery") {
		t.Fatalf("VFS body: %s", body)
	}
	id, err := uuid.Parse(res.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := eng.Get(ctx, brain.Scope{Namespace: ns}, id)
	if err != nil || !strings.Contains(obj.Content, "p99 under 40ms in canary") || obj.Kind != "Discovery" {
		t.Fatalf("engine object: %+v err=%v", obj, err)
	}

	readTool := h.findTool("read", "")
	rl, err := readTool.invoke(ctx, `{"path":`+jsonString(res.Path)+`,"start":1,"end":20}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rl.output, "p99 under 40ms") {
		t.Fatalf("read: %s", rl.output)
	}

	updArgs, err := json.Marshal(map[string]any{
		"title":     "latency finding",
		"object_id": res.ObjectID,
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
		Path     string `json:"path"`
		ObjectID string `json:"object_id"`
	}
	if err := json.Unmarshal([]byte(out2.output), &res2); err != nil {
		t.Fatal(err)
	}
	if res2.Path != res.Path || res2.ObjectID != res.ObjectID {
		t.Fatalf("update path/id changed: create=%+v update=%+v", res, res2)
	}
	body2, err := ms.ReadFile(ctx, res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body2), "p95 under 20ms") {
		t.Fatalf("updated VFS body: %s", body2)
	}
	obj2, err := eng.Get(ctx, brain.Scope{Namespace: ns}, id)
	if err != nil || !strings.Contains(obj2.Content, "p95 under 20ms") {
		t.Fatalf("updated engine: %+v err=%v", obj2, err)
	}
	rl2, err := readTool.invoke(ctx, `{"path":`+jsonString(res.Path)+`,"start":1,"end":20}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rl2.output, "p95 under 20ms") {
		t.Fatalf("read after update: %s", rl2.output)
	}
}

// TestKnowledgeSave_rootsMount: save_discovery writes /discovery/{slug}.md (ModeRoots).
func TestKnowledgeSave_rootsMount(t *testing.T) {
	ctx := context.Background()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(g), brain.WithKinds(
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
	ms := mustMountTreeReq(t, "save-roots", vfs.Request{Bindings: []vfs.Binding{{
		Params: map[string]string{
			vfs.ParamName: "discovery",
			"mode":        brain.ModeRoots,
			"kind":        "Discovery",
		},
	}}},
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("discovery", brain.Open(eng, brain.Scope{Namespace: ns})).Profile("brain").Indexed(vfsindex.PolicyNone),
	)
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "save-roots",
		MountSession: ms, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
		BrainWriteKinds: brain.WriteKinds{Discovery: "Discovery"},
	})
	t.Cleanup(h.Close)
	activatePlan(t, h)

	save := h.findTool("save_discovery", "")
	if save == nil {
		t.Fatal("save_discovery required")
	}
	out, err := save.invoke(ctx, `{"title":"latency finding","content":"p99 under 40ms in canary"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Path     string `json:"path"`
		ObjectID string `json:"object_id"`
	}
	if err := json.Unmarshal([]byte(out.output), &res); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Path, "/workspace/discovery/") || !strings.HasSuffix(res.Path, ".md") {
		t.Fatalf("roots save path: %+v", res)
	}
	body, err := ms.ReadFile(ctx, res.Path)
	if err != nil || !strings.Contains(string(body), "p99 under 40ms") {
		t.Fatalf("ReadFile: %s err=%v", body, err)
	}
	id, err := uuid.Parse(res.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := eng.Get(ctx, brain.Scope{Namespace: ns}, id)
	if err != nil || !strings.Contains(obj.Content, "p99 under 40ms") || obj.Kind != "Discovery" {
		t.Fatalf("engine object: %+v err=%v", obj, err)
	}
}

// TestRun_workspaceResearchTurn: host config (prompt, window, policy, watchdog,
// VFS, brain, namespace) plus a model that decides the next tool from the window.
func TestRun_workspaceResearchTurn(t *testing.T) {
	ctx := context.Background()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(g), brain.WithKinds(
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
	ms := mustMountTree(t, "research-turn",
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("engram", brain.Open(eng, brain.Scope{Namespace: ns})),
	)

	wd := &recordingWatchdog{}
	strategy := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return contentTokenEstimate(msgs), nil
		},
	}
	// The model keeps its own next-action (window pressure may drop tool text).
	var next int
	strategy.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		strategy.mu.Lock()
		prompt := ""
		if n := len(strategy.systemPrompts); n > 0 {
			prompt = strategy.systemPrompts[n-1]
		}
		strategy.mu.Unlock()
		if strings.Contains(prompt, "summarize the entire message history") {
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
		SessionID: "research-turn",
		Config: Config{
			MaxWindowSize:   400,
			SystemPrompt:    "You are a research agent. Prefer tools over guessing.",
			MaxTurnRequests: 20,
		},
		ContextPolicy:   ContextPolicy{PressureRatio: 0.6, CompressFraction: 0.5},
		WatchDog:        wd,
		MountSession:    ms,
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
	if !strings.Contains(save, "object_id") {
		t.Fatalf("save: %s", save)
	}
	if body, err := ms.ReadFile(ctx, "/workspace/work/research.md"); err != nil || !strings.Contains(string(body), "unique-research-token") {
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
	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "path-graph",
		MountSession: ms, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
	})
	t.Cleanup(h.Close)
	activatePlan(t, h)

	if err := ms.WriteFile(ctx, "/workspace/work/a.md", []byte("# A\n\napi doc\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/b.md", []byte("# B\n\nauth fact\n")); err != nil {
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
