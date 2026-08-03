package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/stores"
)

func TestBrainTools_hostNamespaceScopedRead(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	other := uuid.New()
	docID := uuid.New()

	if err := store.Put(brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo",
		Summary: "Q3", Content: "full body", ContentType: "text/plain",
		NamespaceID: ns, Properties: map[string]any{"stage": "negotiation"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutKind(brain.ObjectKind{
		Kind: "Document", Description: "docs", IsParent: true,
		FilterableFields: json.RawMessage(`[]`),
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	h := NewAgent(ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
	})
	gotNS, ok := h.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("SearchNamespace from options: %v %v", gotNS, ok)
	}

	readTool := h.findTool("read", "")
	schemaTool := h.findTool("schema", "")
	if readTool == nil || schemaTool == nil {
		t.Fatal("brain tools must be injected when Brain is configured")
	}

	out, err := readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var rich brain.RichObject
	if err := json.Unmarshal([]byte(out.output), &rich); err != nil {
		t.Fatalf("read JSON: %v\n%s", err, out.output)
	}
	if rich.Content != "full body" || rich.Title != "Deal memo" || rich.ID != docID {
		t.Fatalf("rich object: %+v", rich)
	}

	h.SetSearchNamespace(other)
	_, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, h.runtime)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want not found under other namespace, got %v", err)
	}

	h.ClearSearchNamespace()
	out, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "full body") {
		t.Fatalf("cleared namespace read: %s", out.output)
	}

	sout, err := schemaTool.invoke(ctx, `{"kind":"Document"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var schema brain.SchemaResult
	if err := json.Unmarshal([]byte(sout.output), &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Kinds) != 1 || schema.Kinds[0].Kind != "Document" {
		t.Fatalf("schema: %+v", schema)
	}

	_, err = readTool.invoke(ctx, `{"object_id":"not-a-uuid"}`, h.runtime)
	if err == nil {
		t.Fatal("invalid object_id must fail")
	}
}

func TestBrainTools_searchFindExactContinueAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	var firstParent uuid.UUID
	for i := 0; i < 4; i++ {
		parent := uuid.New()
		if i == 0 {
			firstParent = parent
		}
		part := uuid.New()
		pos := 1
		if err := store.Put(brain.Object{
			ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(brain.Object{
			ID: part, Kind: "Chunk", Title: "knowledge-chunk", Content: "shared knowledge base retrieval material item",
			ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		DefaultLimit: 2, MaxLimit: 50, Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}
	sessStore := stores.NewInMemoryStore()
	h := NewAgent(ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
		Store:           sessStore,
		SessionID:       "brain-sc-1",
	})

	searchTool := h.findTool("search", "")
	findTool := h.findTool("find_exact", "")
	contTool := h.findTool("continue", "")
	if searchTool == nil || findTool == nil || contTool == nil {
		t.Fatal("search, find_exact, continue required")
	}

	out, err := searchTool.invoke(ctx, `{"query":"knowledge base retrieval","limit":2}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var page brain.SearchPage
	if err := json.Unmarshal([]byte(out.output), &page); err != nil {
		t.Fatal(err)
	}
	if page.ResultSetID == uuid.Nil || len(page.Objects) == 0 || !page.HasMore {
		t.Fatalf("page: %+v", page)
	}

	if err := h.checkpointSession(ctx); err != nil {
		t.Fatal(err)
	}
	h2, err := NewAgentFromSession(ctx, "brain-sc-1", AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
		Store:           sessStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	out2, err := h2.findTool("continue", "").invoke(ctx, `{"result_set_id":"`+page.ResultSetID.String()+`","limit":2}`, h2.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var page2 brain.SearchPage
	if err := json.Unmarshal([]byte(out2.output), &page2); err != nil {
		t.Fatal(err)
	}
	if page2.ResultSetID != page.ResultSetID {
		t.Fatalf("result set id changed after load")
	}

	fout, err := findTool.invoke(ctx, `{"query":"`+firstParent.String()+`"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var fpage brain.SearchPage
	if err := json.Unmarshal([]byte(fout.output), &fpage); err != nil {
		t.Fatal(err)
	}
	if len(fpage.Objects) != 1 || fpage.Objects[0].ID != firstParent {
		t.Fatalf("find_exact uuid: %+v", fpage.Objects)
	}
}

func TestBrainTools_expandChildren(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	child := uuid.New()
	pos := 1
	_ = store.Put(brain.Object{ID: parent, Kind: "Document", Title: "P", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(brain.Object{
		ID: child, Kind: "Chunk", Title: "C", Content: "secret",
		ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	h := NewAgent(ctx, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: &ns,
	})
	tool := h.findTool("expand", "")
	if tool == nil {
		t.Fatal("expand tool required")
	}
	out, err := tool.invoke(ctx, `{"object_id":"`+parent.String()+`"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var res brain.ExpandResult
	if err := json.Unmarshal([]byte(out.output), &res); err != nil {
		t.Fatal(err)
	}
	if res.Mode != "children" || len(res.Objects) != 1 || res.Objects[0].ID != child {
		t.Fatalf("%+v", res)
	}
	if res.Objects[0].Content != "" {
		t.Fatal("no content on expand")
	}
}

func TestWorkerInheritsBrainAndNamespace(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	docID := uuid.New()
	if err := store.Put(brain.Object{
		ID: docID, Kind: "Document", Title: "Shared", Content: "worker-visible",
		NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	// part for worker search isolation
	parent := uuid.New()
	part := uuid.New()
	pos := 1
	_ = store.Put(brain.Object{ID: parent, Kind: "Document", Title: "P", NamespaceID: ns})
	_ = store.Put(brain.Object{
		ID: part, Kind: "Chunk", Content: "worker search isolation token",
		ParentID: &parent, Position: &pos, NamespaceID: ns,
	})

	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	parentH := NewAgent(ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})

	// Parent search populates parent SearchContext only.
	if _, err := parentH.findTool("search", "").invoke(ctx, `{"query":"worker search isolation"}`, parentH.runtime); err != nil {
		t.Fatal(err)
	}
	parentRS, err := parentH.searchCtx.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(parentRS) == 0 {
		t.Fatal("parent should have result set after search")
	}

	worker := parentH.newWorkerHarness(ctx, "researcher", "spawn_tc1", parentH.subagents["researcher"])

	gotNS, ok := worker.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("worker namespace %v %v, want %v", gotNS, ok, ns)
	}
	if worker.searchCtx == nil || worker.searchCtx == parentH.searchCtx {
		t.Fatal("worker must own a distinct SearchContext")
	}
	// Worker inherits namespace but must not copy the parent's active ResultSet.
	wraw, err := worker.searchCtx.Export()
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		ResultSet *brain.ResultSet `json:"result_set"`
	}
	if len(wraw) > 0 {
		if err := json.Unmarshal(wraw, &env); err != nil {
			t.Fatal(err)
		}
	}
	if env.ResultSet != nil {
		t.Fatal("new worker must not copy parent ResultSet")
	}

	readTool := worker.findTool("read", "")
	if readTool == nil {
		t.Fatal("worker must inherit brain read tool")
	}
	out, err := readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, worker.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "worker-visible") {
		t.Fatalf("worker read: %s", out.output)
	}

	parentH.ClearSearchNamespace()
	gotNS, ok = worker.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("worker namespace after parent clear: %v %v", gotNS, ok)
	}
}
