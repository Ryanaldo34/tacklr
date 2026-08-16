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
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func TestBrainTools_saveDiscoveryAndLink(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Discovery", IsParent: true},
		brain.KindSpec{Kind: "Fact", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	h := mustNewAgent(t, ctx, AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
		Brain:  eng,
		BrainWriteKinds: brain.WriteKinds{
			Discovery: "Discovery",
			Fact:      "Fact",
			// Memory empty → tool not registered
		},
		SearchNamespace: &ns,
	})

	saveDisc := h.findTool("save_discovery", "")
	saveFact := h.findTool("save_fact", "")
	linkTool := h.findTool("link", "")
	if saveDisc == nil || saveFact == nil || linkTool == nil {
		t.Fatal("save_discovery, save_fact, link required")
	}

	findObj := h.findTool("find_objects", "")
	if findObj == nil {
		t.Fatal("find_objects required with MemoryGraph")
	}

	out, err := saveDisc.invoke(ctx, `{"title":"finding","content":"learned X"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var a brain.RichObject
	if err := json.Unmarshal([]byte(out.output), &a); err != nil {
		t.Fatal(err)
	}
	if a.Kind != "Discovery" || a.Title != "finding" || a.ID == uuid.Nil {
		t.Fatalf("discovery: %+v", a)
	}

	fout, err := findObj.invoke(ctx, `{"query":"finding","kinds":["Discovery"],"limit":5}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fout.output, a.ID.String()) {
		t.Fatalf("find_objects should return saved discovery: %s", fout.output)
	}
	if _, err := findObj.invoke(ctx, `{"query":"  "}`, turnRuntime(h)); err == nil {
		t.Fatal("find_objects requires query")
	}

	out2, err := saveFact.invoke(ctx, `{"title":"fact-a","content":"true claim"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var b brain.RichObject
	if err := json.Unmarshal([]byte(out2.output), &b); err != nil {
		t.Fatal(err)
	}

	// Update discovery
	out3, err := saveDisc.invoke(ctx, `{"object_id":"`+a.ID.String()+`","title":"finding-v2","content":"updated"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var a2 brain.RichObject
	if err := json.Unmarshal([]byte(out3.output), &a2); err != nil {
		t.Fatal(err)
	}
	if a2.ID != a.ID || a2.Title != "finding-v2" {
		t.Fatalf("update: %+v", a2)
	}

	// Invalid evidence_id is a distinct link validation path.
	if _, err := linkTool.invoke(ctx, `{
		"from_id":"`+a.ID.String()+`",
		"to_id":"`+b.ID.String()+`",
		"relation_type":"references",
		"evidence_id":"not-a-uuid"
	}`, turnRuntime(h)); err == nil || !strings.Contains(err.Error(), "evidence_id") {
		t.Fatalf("invalid evidence_id: %v", err)
	}
	lout, err := linkTool.invoke(ctx, `{
		"from_id":"`+a.ID.String()+`",
		"to_id":"`+b.ID.String()+`",
		"relation_type":"references",
		"note":"supports finding",
		"status":"active",
		"role":"source",
		"confidence":0.8,
		"evidence_id":"`+a.ID.String()+`"
	}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lout.output, "supports finding") || !strings.Contains(lout.output, "linked") {
		t.Fatalf("link output: %s", lout.output)
	}
	if !strings.Contains(lout.output, "source") || !strings.Contains(lout.output, a.ID.String()) {
		t.Fatalf("link meta fields: %s", lout.output)
	}
	expandTool := h.findTool("expand", "")
	if expandTool == nil {
		t.Fatal("expand required")
	}
	eout, err := expandTool.invoke(ctx, `{"object_id":"`+a.ID.String()+`","relation_types":["references"]}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(eout.output, b.ID.String()) || !strings.Contains(eout.output, "supports finding") {
		t.Fatalf("expand should return neighbor with note: %s", eout.output)
	}
	readTool := h.findTool("read_object", "")
	rout, err := readTool.invoke(ctx, `{"object_id":"`+a.ID.String()+`"}`, turnRuntime(h))
	if err != nil || !strings.Contains(rout.output, "updated") {
		t.Fatalf("read after save: %v %v", err, rout)
	}
}

func TestBrainTools_hostNamespaceScopedRead(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	other := uuid.New()
	docID := uuid.New()

	if err := store.Put(context.Background(), brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo",
		Summary: "Q3", Content: "full body", ContentType: "text/plain",
		NamespaceID: ns, Properties: map[string]any{"stage": "negotiation"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutKind(ctx, brain.ObjectKind{
		Kind: "Document", Description: "docs", IsParent: true,
		FilterableFields: json.RawMessage(`[]`),
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	h := mustNewAgent(t, ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
	})
	gotNS, ok := h.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("SearchNamespace from options: %v %v", gotNS, ok)
	}

	readTool := h.findTool("read_object", "")
	schemaTool := h.findTool("schema", "")
	if readTool == nil || schemaTool == nil {
		t.Fatal("brain tools must be injected when Brain is configured")
	}

	out, err := readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, turnRuntime(h))
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
	_, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, turnRuntime(h))
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want not found under other namespace, got %v", err)
	}

	h.ClearSearchNamespace()
	out, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "full body") {
		t.Fatalf("cleared namespace read: %s", out.output)
	}

	sout, err := schemaTool.invoke(ctx, `{"kind":"Document"}`, turnRuntime(h))
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

	_, err = readTool.invoke(ctx, `{"object_id":"not-a-uuid"}`, turnRuntime(h))
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
		if err := store.Put(context.Background(), brain.Object{
			ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(context.Background(), brain.Object{
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
	h := mustNewAgent(t, ctx, AgentOptions{
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

	out, err := searchTool.invoke(ctx, `{"query":"knowledge base retrieval","limit":2}`, turnRuntime(h))
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
	out2, err := h2.findTool("continue", "").invoke(ctx, `{"result_set_id":"`+page.ResultSetID.String()+`","limit":2}`, turnRuntime(h2))
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

	fout, err := findTool.invoke(ctx, `{"query":"`+firstParent.String()+`"}`, turnRuntime(h))
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

	// scope_ids bad UUID
	if _, err := searchTool.invoke(ctx, `{"query":"x","scope_ids":["not-a-uuid"]}`, turnRuntime(h)); err == nil {
		t.Fatal("bad scope_ids")
	}
	// valid scope_ids restrict neighborhood
	scopeArgs, _ := json.Marshal(map[string]any{
		"query": "knowledge", "scope_ids": []string{firstParent.String()}, "limit": 5,
	})
	if _, err := searchTool.invoke(ctx, string(scopeArgs), turnRuntime(h)); err != nil {
		t.Fatalf("scope_ids search: %v", err)
	}
	// schema unknown kind
	schemaTool := h.findTool("schema", "")
	if schemaTool != nil {
		if _, err := schemaTool.invoke(ctx, `{"kind":"NoSuchKindXYZ"}`, turnRuntime(h)); err == nil {
			t.Fatal("schema unknown kind")
		}
		// list all kinds
		if _, err := schemaTool.invoke(ctx, `{}`, turnRuntime(h)); err != nil {
			t.Fatalf("schema all: %v", err)
		}
	}
	// continue with bad result set
	if _, err := contTool.invoke(ctx, `{"result_set_id":"`+uuid.New().String()+`"}`, turnRuntime(h)); err == nil {
		t.Fatal("continue unknown set")
	}
	if _, err := contTool.invoke(ctx, `{"result_set_id":"bad"}`, turnRuntime(h)); err == nil {
		t.Fatal("continue bad uuid")
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
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", Title: "P", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(context.Background(), brain.Object{
		ID: child, Kind: "Chunk", Title: "C", Content: "secret",
		ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewAgent(t, ctx, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: &ns,
	})
	tool := h.findTool("expand", "")
	if tool == nil {
		t.Fatal("expand tool required")
	}
	out, err := tool.invoke(ctx, `{"object_id":"`+parent.String()+`"}`, turnRuntime(h))
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

// TestBrainTools_expandMultiHopAndFindLinks: graph multi-hop expand and find_links
// tool outcomes when MemoryGraph provides edge search.
func TestBrainTools_expandMultiHopAndFindLinks(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	ns := uuid.New()
	now := time.Now().UTC()
	factID, dealID, buyerID := uuid.New(), uuid.New(), uuid.New()
	for _, o := range []brain.Object{
		{ID: factID, Kind: "Fact", Title: "Risk note", NamespaceID: ns, UpdatedAt: now},
		{ID: dealID, Kind: "Deal", Title: "Acme", NamespaceID: ns, UpdatedAt: now},
		{ID: buyerID, Kind: "Person", Title: "Pat Buyer", NamespaceID: ns, UpdatedAt: now},
	} {
		if err := store.Put(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.AddEdge(ctx, factID, dealID, "about", brain.EdgeMeta{Note: "fact about deal"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, dealID, buyerID, "has_buyer", brain.EdgeMeta{Note: "economic buyer primary"}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasEdgeSearch() {
		t.Fatal("MemoryGraph must enable edge search")
	}
	h := mustNewAgent(t, ctx, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: &ns,
	})
	expand := h.findTool("expand", "")
	findLinks := h.findTool("find_links", "")
	if expand == nil || findLinks == nil {
		t.Fatal("expand and find_links required with graph edge search")
	}

	// Multi-hop: fact --about--> deal --has_buyer--> buyer
	out, err := expand.invoke(ctx, `{"object_id":"`+factID.String()+`","relation_types":["about","has_buyer"],"max_hops":2}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var res brain.ExpandResult
	if err := json.Unmarshal([]byte(out.output), &res); err != nil {
		t.Fatal(err)
	}
	ids := map[uuid.UUID]bool{}
	for _, o := range res.Objects {
		ids[o.ID] = true
	}
	if !ids[dealID] {
		t.Fatalf("multi-hop expand missing deal: %+v", res.Objects)
	}
	if !ids[buyerID] {
		t.Fatalf("multi-hop expand missing buyer: %+v", res.Objects)
	}

	lout, err := findLinks.invoke(ctx, `{"relation_type":"has_buyer","query":"economic buyer","limit":10}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	var links brain.FindLinksResult
	if err := json.Unmarshal([]byte(lout.output), &links); err != nil {
		t.Fatal(err)
	}
	if len(links.Links) != 1 || links.Links[0].From.ID != dealID || links.Links[0].To.ID != buyerID {
		t.Fatalf("find_links: %+v", links.Links)
	}
}

// TestBrainTools_searchNamespaceIsolation: search under host namespace must not
// surface objects stored only in another namespace.
func TestBrainTools_searchNamespaceIsolation(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	nsA, nsB := uuid.New(), uuid.New()
	now := time.Now().UTC()
	secretParent := uuid.New()
	secretPart := uuid.New()
	pos := 1
	_ = store.Put(ctx, brain.Object{ID: secretParent, Kind: "Document", Title: "Secret deal", NamespaceID: nsA, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{
		ID: secretPart, Kind: "Chunk", Title: "chunk", Content: "namespace isolation secret token xyzzy",
		ParentID: &secretParent, Position: &pos, NamespaceID: nsA, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	// Agent scoped to nsB only.
	h := mustNewAgent(t, ctx, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: &nsB,
	})
	search := h.findTool("search", "")
	read := h.findTool("read_object", "")
	if search == nil || read == nil {
		t.Fatal("search and read_object required")
	}
	out, err := search.invoke(ctx, `{"query":"namespace isolation secret token xyzzy","limit":10}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.output, secretParent.String()) || strings.Contains(out.output, "xyzzy") {
		t.Fatalf("search leaked nsA object into nsB: %s", out.output)
	}
	_, err = read.invoke(ctx, `{"object_id":"`+secretParent.String()+`"}`, turnRuntime(h))
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("read foreign ns want not found, got %v", err)
	}
}

func TestWorkerInheritsBrainAndNamespace(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	docID := uuid.New()
	if err := store.Put(context.Background(), brain.Object{
		ID: docID, Kind: "Document", Title: "Shared", Content: "worker-visible",
		NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	// part for worker search isolation
	parent := uuid.New()
	part := uuid.New()
	pos := 1
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", Title: "P", NamespaceID: ns})
	_ = store.Put(context.Background(), brain.Object{
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
	parentH := mustNewAgent(t, ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})

	worker, err := parentH.newWorkerHarness(ctx, "researcher", "spawn_tc1", parentH.subagents["researcher"])
	if err != nil {
		t.Fatal(err)
	}

	gotNS, ok := worker.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("worker namespace %v %v, want %v", gotNS, ok, ns)
	}

	readTool := worker.findTool("read_object", "")
	searchTool := worker.findTool("search", "")
	if readTool == nil || searchTool == nil {
		t.Fatal("worker must inherit brain read_object and search")
	}
	out, err := readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, turnRuntime(worker))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "worker-visible") {
		t.Fatalf("worker read: %s", out.output)
	}
	sout, err := searchTool.invoke(ctx, `{"query":"worker search isolation token"}`, turnRuntime(worker))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sout.output, parent.String()) && !strings.Contains(sout.output, "worker search isolation") {
		t.Fatalf("worker search: %s", sout.output)
	}

	parentH.ClearSearchNamespace()
	gotNS, ok = worker.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("worker namespace after parent clear: %v %v", gotNS, ok)
	}
}

// TestBrainTools_engramPathGraph: write two Engrams, link by path, expand/find_links
// return neighbor paths; unindexed /work artifact fails until index_file.
func TestBrainTools_engramPathGraph(t *testing.T) {
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("engram-graph", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true},
		brain.KindSpec{Kind: "Person", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, append(vfsindex.MountIndexKinds(),
		brain.KindSpec{Kind: "Deal", IsParent: true},
		brain.KindSpec{Kind: "Person", IsParent: true},
	)...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	mustMountBrain(ctx, t, reg, ms, eng, ns, vfs.MountSpec{})
	h := mustNewAgent(t, ctx, AgentOptions{
		SessionID: "engram-graph", Store: stores.NewInMemoryStore(),
		MountSession: ms, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: &ns,
	})
	t.Cleanup(h.Close)
	activatePlan(t, h)

	if err := ms.WriteFile(ctx, "/engram/deal/acme.md", []byte("---\ndomain: Deal\nslug: acme\n---\n\nDeal body.\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/engram/person/sam.md", []byte("---\ndomain: Person\nslug: sam\n---\n\nBuyer.\n")); err != nil {
		t.Fatal(err)
	}

	link := h.findTool("link", "")
	expand := h.findTool("expand", "")
	findLinks := h.findTool("find_links", "")
	if link == nil || expand == nil || findLinks == nil {
		t.Fatal("link/expand/find_links required")
	}
	lout, err := link.invoke(ctx, `{
		"from":"/engram/deal/acme.md","to":"/engram/person/sam.md",
		"relation_type":"has_contact","role":"buyer","note":"primary buyer"
	}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lout.output, "/engram/deal/acme.md") || !strings.Contains(lout.output, "/engram/person/sam.md") {
		t.Fatalf("link paths: %s", lout.output)
	}

	eout, err := expand.invoke(ctx, `{"path":"/engram/deal/acme.md","relation_types":["has_contact"]}`, turnRuntime(h))
	if err != nil || !strings.Contains(eout.output, "/engram/person/sam.md") {
		t.Fatalf("expand neighbor path: %v %s", err, eout.output)
	}

	fout, err := findLinks.invoke(ctx, `{"relation_type":"has_contact","query":"primary"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fout.output, "from_path") || !strings.Contains(fout.output, "to_path") {
		t.Fatalf("find_links path fields: %s", fout.output)
	}
	if !strings.Contains(fout.output, "/engram/deal/acme.md") || !strings.Contains(fout.output, "/engram/person/sam.md") {
		t.Fatalf("find_links endpoints: %s", fout.output)
	}

	if err := ms.WriteFile(ctx, "/work/doc.md", []byte("# Doc\n\nartifact\n")); err != nil {
		t.Fatal(err)
	}
	_, err = link.invoke(ctx, `{
		"from":"/work/doc.md","to":"/engram/deal/acme.md","relation_type":"about"
	}`, turnRuntime(h))
	if err == nil || !strings.Contains(err.Error(), "not indexed") {
		t.Fatalf("unindexed artifact: %v", err)
	}
	idx := h.findTool("index_file", "")
	if _, err := runWriteTool(t, h, idx, `{"path":"/work/doc.md"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := link.invoke(ctx, `{
		"from":"/work/doc.md","to":"/engram/deal/acme.md","relation_type":"about"
	}`, turnRuntime(h)); err != nil {
		t.Fatal(err)
	}

	unlink := h.findTool("unlink", "")
	if unlink == nil {
		t.Fatal("unlink required")
	}
	if _, err := unlink.invoke(ctx, `{
		"from":"/engram/deal/acme.md","to":"/engram/person/sam.md","relation_type":"has_contact"
	}`, turnRuntime(h)); err != nil {
		t.Fatal(err)
	}
	eout2, err := expand.invoke(ctx, `{"path":"/engram/deal/acme.md","relation_types":["has_contact"]}`, turnRuntime(h))
	if err != nil || strings.Contains(eout2.output, "/engram/person/sam.md") {
		t.Fatalf("after unlink: %v %s", err, eout2.output)
	}
}
