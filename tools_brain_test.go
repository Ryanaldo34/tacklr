package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
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
	ns := brain.MustNamespace("id", uuid.NewString())
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
		Brain:  eng,
		BrainWriteKinds: brain.WriteKinds{
			Discovery: "Discovery",
			Fact:      "Fact",
			// Memory empty → tool not registered
		},
		SearchNamespace: ns,
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
	ns := brain.MustNamespace("id", uuid.NewString())
	other := brain.MustNamespace("org", "other")
	docID := uuid.New()

	if err := store.Put(context.Background(), brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo",
		Summary: "Q3", Content: "full body", ContentType: "text/plain",
		Namespace: ns, Properties: map[string]any{"stage": "negotiation"},
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

	h := mustNewTurnManager(t, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: ns,
	})
	gotNS, ok := h.session.Search.Namespace()
	if !ok || !gotNS.Equal(ns) {
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

	h.session.Search.SetNamespace(other)
	_, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, turnRuntime(h))
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want not found under other namespace, got %v", err)
	}

	h.session.Search.ClearNamespace()
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
	ns := brain.MustNamespace("id", uuid.NewString())
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
			ID: parent, Kind: "Document", Title: "Doc", Namespace: ns, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(context.Background(), brain.Object{
			ID: part, Kind: "Chunk", Title: "knowledge-chunk", Content: "shared knowledge base retrieval material item",
			ParentID: &parent, Position: &pos, Namespace: ns, UpdatedAt: now,
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
	h := mustNewTurnManager(t, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: ns,
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

	h2 := reloadHarness(t, h, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: ns,
		SessionID:       "brain-sc-1",
	})
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
	ns := brain.MustNamespace("id", uuid.NewString())
	now := time.Now().UTC()
	parent := uuid.New()
	child := uuid.New()
	pos := 1
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", Title: "P", Namespace: ns, UpdatedAt: now})
	_ = store.Put(context.Background(), brain.Object{
		ID: child, Kind: "Chunk", Title: "C", Content: "secret",
		ParentID: &parent, Position: &pos, Namespace: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
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
	ns := brain.MustNamespace("id", uuid.NewString())
	now := time.Now().UTC()
	factID, dealID, buyerID := uuid.New(), uuid.New(), uuid.New()
	for _, o := range []brain.Object{
		{ID: factID, Kind: "Fact", Title: "Risk note", Namespace: ns, UpdatedAt: now},
		{ID: dealID, Kind: "Deal", Title: "Acme", Namespace: ns, UpdatedAt: now},
		{ID: buyerID, Kind: "Person", Title: "Pat Buyer", Namespace: ns, UpdatedAt: now},
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
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
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
	nsA, nsB := brain.MustNamespace("org", "a"), brain.MustNamespace("org", "b")
	now := time.Now().UTC()
	secretParent := uuid.New()
	secretPart := uuid.New()
	pos := 1
	_ = store.Put(ctx, brain.Object{ID: secretParent, Kind: "Document", Title: "Secret deal", Namespace: nsA, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{
		ID: secretPart, Kind: "Chunk", Title: "chunk", Content: "namespace isolation secret token xyzzy",
		ParentID: &secretParent, Position: &pos, Namespace: nsA, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	// Agent scoped to nsB only.
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 1024}, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: nsB,
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

	westParent := uuid.New()
	westPart := uuid.New()
	_ = store.Put(ctx, brain.Object{ID: westParent, Kind: "Document", Title: "West deal", Namespace: brain.MustNamespace("org", "b", "workspace", "west"), UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{
		ID: westPart, Kind: "Chunk", Title: "chunk", Content: "per-call workspace west token plugh",
		ParentID: &westParent, Position: &pos, Namespace: brain.MustNamespace("org", "b", "workspace", "west"), UpdatedAt: now,
	})
	narrow, err := search.invoke(ctx, `{"query":"per-call workspace west token plugh","limit":10,"namespace":[{"name":"workspace","value":"west"}]}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(narrow.output, westParent.String()) {
		t.Fatalf("workspace=west search should hit west object: %s", narrow.output)
	}
	east, err := search.invoke(ctx, `{"query":"per-call workspace west token plugh","limit":10,"namespace":[{"name":"workspace","value":"east"}]}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(east.output, westParent.String()) || strings.Contains(east.output, "plugh") {
		t.Fatalf("workspace=east search leaked west object: %s", east.output)
	}
	if _, err := search.invoke(ctx, `{"query":"x","namespace":[{"name":"org","value":"a"}]}`, turnRuntime(h)); err == nil || !strings.Contains(err.Error(), "outside host scope") {
		t.Fatalf("conflicting org: %v", err)
	}
}

func TestWorkerInheritsBrainAndNamespace(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := brain.MustNamespace("id", uuid.NewString())
	docID := uuid.New()
	if err := store.Put(context.Background(), brain.Object{
		ID: docID, Kind: "Document", Title: "Shared", Content: "worker-visible",
		Namespace: ns,
	}); err != nil {
		t.Fatal(err)
	}
	// part for worker search isolation
	parent := uuid.New()
	part := uuid.New()
	pos := 1
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", Title: "P", Namespace: ns})
	_ = store.Put(context.Background(), brain.Object{
		ID: part, Kind: "Chunk", Content: "worker search isolation token",
		ParentID: &parent, Position: &pos, Namespace: ns,
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
	parentOpts := AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: ns,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	}
	parentH := mustNewTurnManager(t, parentOpts)
	t.Cleanup(parentH.Close)
	workerOpts := parentOpts.WithSpecialist(parentH.specialists["researcher"])
	workerOpts.SearchNamespace = ns
	workerOpts.SessionID = "w/researcher/spawn_tc1"
	worker := mustNewTurnManager(t, workerOpts)
	t.Cleanup(worker.Close)

	gotNS, ok := worker.session.Search.Namespace()
	if !ok || !gotNS.Equal(ns) {
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

	parentH.session.Search.ClearNamespace()
	gotNS, ok = worker.session.Search.Namespace()
	if !ok || !gotNS.Equal(ns) {
		t.Fatalf("worker namespace after parent clear: %v %v", gotNS, ok)
	}
}

// TestBrainTools_engramPathGraph: write two Engrams, link by path, expand/find_links
// return neighbor paths; unindexed /work artifact fails until index_file.
func TestBrainTools_engramPathGraph(t *testing.T) {
	ctx := context.Background()
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
	ns := brain.MustNamespace("id", uuid.NewString())
	ms := mustMountTree(t, "engram-graph",
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("engram", brain.Open(eng, brain.Scope{Namespace: ns})),
	)
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "engram-graph",
		MountSession: ms, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: ns,
	})
	t.Cleanup(h.Close)
	activatePlan(t, h)

	if err := ms.WriteFile(ctx, "/workspace/engram/deal/acme.md", []byte("---\ndomain: Deal\nslug: acme\n---\n\nDeal body.\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/engram/person/sam.md", []byte("---\ndomain: Person\nslug: sam\n---\n\nBuyer.\n")); err != nil {
		t.Fatal(err)
	}

	link := h.findTool("link", "")
	expand := h.findTool("expand", "")
	findLinks := h.findTool("find_links", "")
	if link == nil || expand == nil || findLinks == nil {
		t.Fatal("link/expand/find_links required")
	}
	lout, err := link.invoke(ctx, `{
		"from":"/workspace/engram/deal/acme.md","to":"/workspace/engram/person/sam.md",
		"relation_type":"has_contact","role":"buyer","note":"primary buyer"
	}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lout.output, "/workspace/engram/deal/acme.md") || !strings.Contains(lout.output, "/workspace/engram/person/sam.md") {
		t.Fatalf("link paths: %s", lout.output)
	}

	eout, err := expand.invoke(ctx, `{"path":"/workspace/engram/deal/acme.md","relation_types":["has_contact"]}`, turnRuntime(h))
	if err != nil || !strings.Contains(eout.output, "/workspace/engram/person/sam.md") {
		t.Fatalf("expand neighbor path: %v %s", err, eout.output)
	}

	fout, err := findLinks.invoke(ctx, `{"relation_type":"has_contact","query":"primary"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fout.output, "from_path") || !strings.Contains(fout.output, "to_path") {
		t.Fatalf("find_links path fields: %s", fout.output)
	}
	if !strings.Contains(fout.output, "/workspace/engram/deal/acme.md") || !strings.Contains(fout.output, "/workspace/engram/person/sam.md") {
		t.Fatalf("find_links endpoints: %s", fout.output)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/doc.md", []byte("# Doc\n\nartifact\n")); err != nil {
		t.Fatal(err)
	}
	_, err = link.invoke(ctx, `{
		"from":"/workspace/work/doc.md","to":"/workspace/engram/deal/acme.md","relation_type":"about"
	}`, turnRuntime(h))
	if err == nil || !strings.Contains(err.Error(), "not indexed") {
		t.Fatalf("unindexed artifact: %v", err)
	}
	idx := h.findTool("index_file", "")
	if _, err := runWriteTool(t, h, idx, `{"path":"/workspace/work/doc.md"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := link.invoke(ctx, `{
		"from":"/workspace/work/doc.md","to":"/workspace/engram/deal/acme.md","relation_type":"about"
	}`, turnRuntime(h)); err != nil {
		t.Fatal(err)
	}

	unlink := h.findTool("unlink", "")
	if unlink == nil {
		t.Fatal("unlink required")
	}
	if _, err := unlink.invoke(ctx, `{
		"from":"/workspace/engram/deal/acme.md","to":"/workspace/engram/person/sam.md","relation_type":"has_contact"
	}`, turnRuntime(h)); err != nil {
		t.Fatal(err)
	}
	eout2, err := expand.invoke(ctx, `{"path":"/workspace/engram/deal/acme.md","relation_types":["has_contact"]}`, turnRuntime(h))
	if err != nil || strings.Contains(eout2.output, "/workspace/engram/person/sam.md") {
		t.Fatalf("after unlink: %v %s", err, eout2.output)
	}
}

func TestBrainTools_expandAndUnlinkValidationErrors(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(brain.NewMemoryGraph()))
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
		Brain:  eng,
	})
	expand := h.findTool("expand", "")
	unlink := h.findTool("unlink", "")
	if expand == nil || unlink == nil {
		t.Fatal("expand and unlink required with brain")
	}
	rt := turnRuntime(h)

	if _, err := expand.invoke(ctx, `{}`, rt); err == nil || !strings.Contains(err.Error(), "expand") {
		t.Fatalf("expand missing ref = %v", err)
	}
	if _, err := unlink.invoke(ctx, `{"relation_type":"about"}`, rt); err == nil || !strings.Contains(err.Error(), "unlink") {
		t.Fatalf("unlink missing ref = %v", err)
	}
	if _, err := unlink.invoke(ctx, `{
		"from_id":"not-a-uuid","to_id":"not-a-uuid","relation_type":"about"
	}`, rt); err == nil || !strings.Contains(err.Error(), "from") {
		t.Fatalf("unlink invalid uuid = %v", err)
	}
}

type failGetStore struct {
	*brain.MemoryStore
	err error
}

func (s failGetStore) Get(ctx context.Context, scope brain.Scope, id uuid.UUID) (brain.Object, error) {
	if s.err != nil {
		return brain.Object{}, s.err
	}
	return s.MemoryStore.Get(ctx, scope, id)
}

func TestBrainTools_resolveFileRefPropagatesStoreFailure(t *testing.T) {
	ctx := context.Background()
	mem := brain.NewMemoryStore()
	ns := brain.MustNamespace("id", uuid.NewString())
	id := uuid.New()
	if err := mem.Put(ctx, brain.Object{
		ID: id, Kind: "Document", Title: "memo", Namespace: ns, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(failGetStore{MemoryStore: mem, err: errors.New("brain store down")})
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: ns,
	})
	expand := h.findTool("expand", "")
	if expand == nil {
		t.Fatal("expand required")
	}
	_, err = expand.invoke(ctx, `{"object_id":"`+id.String()+`"}`, turnRuntime(h))
	if err == nil || !strings.Contains(err.Error(), "brain store down") {
		t.Fatalf("want store failure propagated, got %v", err)
	}
}
