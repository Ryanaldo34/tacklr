package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// kindEnum is the enum of the tool's kind property; none when it is free.
func kindEnum(t *testing.T, tool *Tool) []any {
	t.Helper()
	props, _ := tool.parameters["properties"].(map[string]any)
	kind, _ := props["kind"].(map[string]any)
	enum, _ := kind["enum"].([]any)
	return enum
}

func toolNames(h *TurnManager) []string {
	var out []string
	for _, tool := range h.tools {
		switch tool.Name() {
		case "search", "open", "related", "save", "link":
			out = append(out, tool.Name())
		}
	}
	slices.Sort(out)
	return out
}

func TestBrainTools_registerTheToolsTheEngineCanServeWithKindChoices(t *testing.T) {
	t.Parallel()
	catalog, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Fact", IsParent: true}, brain.KindSpec{Kind: "Deal", IsParent: true}, brain.KindSpec{Kind: "Chunk", IsPart: true}))
	if err != nil {
		t.Fatal(err)
	}
	open, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", uuid.NewString())
	withKinds := mustNewTurnManager(t, AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: catalog, SearchNamespace: ns, BrainWriteKinds: []string{"Fact", " ", "Fact"}})
	withGraph := mustNewTurnManager(t, AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: open, SearchNamespace: ns})

	if got := toolNames(withKinds); !slices.Equal(got, []string{"open", "related", "save", "search"}) {
		t.Errorf("store-only tools with a writable kind = %v, want search, open, related and save", got)
	}
	if got := toolNames(withGraph); !slices.Equal(got, []string{"link", "open", "related", "search"}) {
		t.Errorf("tools with a graph and no writable kind = %v, want search, open, related and link", got)
	}
	if got := kindEnum(t, withKinds.findTool("search", "")); !slices.Equal(got, []any{"Deal", "Fact", nil}) {
		t.Errorf("search kind enum = %v, want the record kinds (not the part kind), or null", got)
	}
	if got := kindEnum(t, withKinds.findTool("save", "")); !slices.Equal(got, []any{"Fact"}) {
		t.Errorf("save kind enum = %v, want the writable kind", got)
	}
	if got := kindEnum(t, withGraph.findTool("search", "")); got != nil {
		t.Errorf("open-mode search kind enum = %v, want a free string", got)
	}
}

func TestBrainTools_saveSearchOpenAndRelatedServeOneSession(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{Kind: "Fact", IsParent: true, Fields: []brain.FieldSpec{{Name: "topic", Type: brain.FieldTypeString}}},
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true}),
		brain.WithConfig(brain.EngineConfig{DefaultLimit: 2}))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", uuid.NewString())
	west := append(ns.Clone(), brain.Attr{Name: "workspace", Value: "west"})
	doc, err := eng.PutTree(ctx, brain.Scope{Namespace: west}, brain.Object{Kind: "Document", Title: "Runbook"},
		brain.Parts(brain.Object{Kind: "Chunk", Content: "restart the knowledge service"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := putFixture(store, brain.Object{ID: uuid.New(), Kind: "Fact", Title: "foreign knowledge", Namespace: mustNS(t, "org", uuid.NewString())}); err != nil {
		t.Fatal(err)
	}
	opts := AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: eng, SearchNamespace: ns, BrainWriteKinds: []string{"Fact"}, sessionID: "brain-tools-1"}
	h := mustNewTurnManager(t, opts)
	call := func(h *TurnManager, name, args string) (string, error) {
		out, err := h.findTool(name, "").invoke(ctx, args, turnRuntime(h))
		return out.output, err
	}
	var saved []brain.RichObject
	for _, title := range []string{"knowledge one", "knowledge two", "knowledge three"} {
		out, err := call(h, "save", `{"kind":"Fact","title":"`+title+`","content":"body","properties":{"topic":"ops"}}`)
		if err != nil {
			t.Fatal(err)
		}
		var rich brain.RichObject
		if err := json.Unmarshal([]byte(out), &rich); err != nil {
			t.Fatal(err)
		}
		saved = append(saved, rich)
	}
	page := func(out string, err error) brain.Page {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var p brain.Page
		if err := json.Unmarshal([]byte(out), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}

	first := page(call(h, "search", `{"query":"knowledge","kind":"Fact"}`))
	reloaded := reloadHarness(t, h, opts)
	second := page(call(reloaded, "search", `{"more":true}`))
	updated, updateErr := call(h, "save", `{"kind":"Fact","id":"`+saved[0].ID.String()+`","title":"knowledge one, revised"}`)
	narrowed := page(call(h, "search", `{"query":"knowledge","namespace":[{"name":"workspace","value":"west"}]}`))
	openOut, openErr := call(h, "open", `{"ref":"`+doc.ID.String()+`","namespace":[{"name":"workspace","value":"west"}]}`)
	related := page(call(h, "related", `{"ref":"`+doc.ID.String()+`","namespace":[{"name":"workspace","value":"west"}]}`))
	_, noMore := call(h, "related", `{"more":true,"ref":"x"}`)
	_, badKind := call(h, "save", `{"kind":"Memory","title":"x"}`)
	_, noTitle := call(h, "save", `{"kind":"Fact","title":" "}`)
	_, badID := call(h, "save", `{"kind":"Fact","title":"x","id":"nope"}`)
	_, badFilter := call(h, "search", `{"query":"knowledge","kind":"Fact","filters":{"phase":"x"}}`)
	_, reserved := call(h, "search", `{"query":"knowledge","namespace":[{"name":"session","value":"x"}]}`)
	_, outside := call(h, "search", `{"query":"x","namespace":[{"name":"org","value":"other"}]}`)
	_, badRef := call(h, "open", `{"ref":"not a path"}`)
	_, noRef := call(h, "related", `{"ref":""}`)
	_, badTime := call(h, "search", `{"query":"knowledge","filters":{"created_after":"yesterday"}}`)
	_, missing := call(h, "open", `{"ref":"`+uuid.NewString()+`"}`)
	_, relatedOutside := call(h, "related", `{"ref":"`+doc.ID.String()+`","namespace":[{"name":"org","value":"other"}]}`)
	_, saveOutside := call(h, "save", `{"kind":"Fact","title":"x","namespace":[{"name":"org","value":"other"}]}`)
	_, undeclared := call(h, "save", `{"kind":"Fact","title":"x","properties":{"color":"red"}}`)
	h.session.Search.ClearNamespace()
	_, noNamespace := call(h, "open", `{"ref":"`+saved[0].ID.String()+`"}`)

	if len(slices.Collect(first.Results.Objects())) != 2 || !first.More || first.Facets.Kinds["Fact"] != 2 || first.Facets.Properties["topic"]["ops"] != 2 {
		t.Errorf("first search = %+v, want 2 facts, more, and facets naming the kind and topic", first)
	}
	if len(slices.Collect(second.Results.Objects())) != 1 || second.More || slices.Contains(idsOfPage(first), slices.Collect(second.Results.Objects())[0].ID) {
		t.Errorf("more after a checkpoint = %+v, want the third fact", second)
	}
	if updateErr != nil || !strings.Contains(updated, "revised") || !strings.Contains(updated, saved[0].ID.String()) {
		t.Errorf("save with an id = %s %v, want the record updated", updated, updateErr)
	}
	if !slices.Contains(titlesOfPage(narrowed), "Runbook") || slices.Contains(titlesOfPage(narrowed), "foreign knowledge") {
		t.Errorf("narrowed search = %v, want the west runbook and no other tenant's record", titlesOfPage(narrowed))
	}
	if openErr != nil || !strings.Contains(openOut, `"kind": "Document"`) || !strings.Contains(openOut, `"children"`) {
		t.Errorf("open = %s %v, want the record, its kind and its children", openOut, openErr)
	}
	if len(slices.Collect(related.Results.Objects())) != 1 || slices.Collect(related.Results.Objects())[0].Relation.Type != brain.RelationChild {
		t.Errorf("related = %+v, want the runbook's part as a child", related)
	}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"more with no earlier call":     {noMore, "no more results"},
		"an unwritable kind":            {badKind, `kind "Memory" cannot be saved; kinds: Fact`},
		"no title":                      {noTitle, "title is required"},
		"a bad id":                      {badID, "id must be a UUID"},
		"an unknown filter":             {badFilter, `unknown filter "phase" for Fact; filterable: topic`},
		"the reserved session attr":     {reserved, "reserved"},
		"another org":                   {outside, "outside host scope"},
		"a ref that is no path":         {badRef, "neither a UUID nor a path"},
		"an empty ref":                  {noRef, "ref is required"},
		"a filter time that is no time": {badTime, "created_after"},
		"a missing record":              {missing, "not found"},
		"related in another org":        {relatedOutside, "outside host scope"},
		"save in another org":           {saveOutside, "outside host scope"},
		"an undeclared property":        {undeclared, "color"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want an error naming %q", name, tc.err, tc.want)
		}
	}
	if !errors.Is(noNamespace, brain.ErrInvalid) {
		t.Errorf("open without a host namespace = %v, want it to fail closed", noNamespace)
	}
}

func idsOfPage(p brain.Page) []uuid.UUID {
	var out []uuid.UUID
	for _, o := range slices.Collect(p.Results.Objects()) {
		out = append(out, o.ID)
	}
	return out
}

func titlesOfPage(p brain.Page) []string {
	var out []string
	for _, o := range slices.Collect(p.Results.Objects()) {
		out = append(out, o.Title)
	}
	return out
}

// TestBrainTools_linkByIDOrIndexedPathAndFindItByItsNote: records link by
// id, an indexed file by its path (an unindexed one cannot be an end), the
// note makes the link searchable, and remove takes it away.
func TestBrainTools_linkByIDOrIndexedPathAndFindItByItsNote(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, append(vfsindex.MountIndexKinds(),
		brain.KindSpec{Kind: "Deal", IsParent: true}, brain.KindSpec{Kind: "Person", IsParent: true})...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	deal, err := eng.Save(ctx, scope, brain.Object{Kind: "Deal", Title: "Acme", Content: "Deal body."})
	if err != nil {
		t.Fatal(err)
	}
	person, err := eng.Save(ctx, scope, brain.Object{Kind: "Person", Title: "Sam", Content: "Buyer."})
	if err != nil {
		t.Fatal(err)
	}
	ms := mustMountTree(t, "link-graph", vfs.At("work", vfs.Local(t.TempDir())))
	h := mustNewTurnManager(t, AgentOptions{sessionID: "link-graph", mountSession: ms, Model: &scriptedModel{}, Brain: eng, SearchNamespace: ns})
	t.Cleanup(h.Close)
	activatePlan(t, h)
	call := func(name, args string) (string, error) {
		out, err := h.findTool(name, "").invoke(ctx, args, turnRuntime(h))
		return out.output, err
	}
	if err := ms.Route(ctx, "/workspace/work/doc.md").WriteFile(ctx, []byte("# Doc\n\nartifact\n")); err != nil {
		t.Fatal(err)
	}

	linked, linkErr := call("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":"about","note":"signs the renewal contract"}`)
	found, foundErr := call("search", `{"query":"signs the renewal contract"}`)
	related, relatedErr := call("related", `{"ref":"`+deal.ID.String()+`","relation":"about"}`)
	_, unindexed := call("link", `{"from":"/workspace/work/doc.md","to":"`+deal.ID.String()+`","relation":"about"}`)
	if _, err := runWriteTool(t, h, h.findTool("index_file", ""), `{"path":"/workspace/work/doc.md"}`); err != nil {
		t.Fatal(err)
	}
	_, byPath := call("link", `{"from":"/workspace/work/doc.md","to":"`+deal.ID.String()+`","relation":"about"}`)
	removed, removeErr := call("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":"about","remove":true}`)
	after, _ := call("related", `{"ref":"`+deal.ID.String()+`","relation":"about"}`)
	_, noTo := call("link", `{"from":"`+deal.ID.String()+`","to":"","relation":"about"}`)

	if linkErr != nil || !strings.Contains(linked, `"linked": true`) || !strings.Contains(linked, person.ID.String()) {
		t.Errorf("link = %s %v, want it linked", linked, linkErr)
	}
	if foundErr != nil || !strings.Contains(found, `"relation": "about"`) || !strings.Contains(found, "signs the renewal contract") {
		t.Errorf("search the note = %s %v, want the link", found, foundErr)
	}
	if relatedErr != nil || !strings.Contains(related, person.ID.String()) {
		t.Errorf("related by about = %s %v, want the person", related, relatedErr)
	}
	if unindexed == nil || !strings.Contains(unindexed.Error(), "not indexed") || byPath != nil {
		t.Errorf("link an unindexed file = %v, then an indexed one = %v, want not indexed, then linked", unindexed, byPath)
	}
	if removeErr != nil || !strings.Contains(removed, `"linked": false`) || strings.Contains(after, person.ID.String()) {
		t.Errorf("remove = %s %v, then related %s, want the person gone", removed, removeErr, after)
	}
	if noTo == nil || !strings.Contains(noTo.Error(), "to is required") {
		t.Errorf("link with no to = %v, want it named", noTo)
	}
}

func TestWorkerInheritsBrainAndNamespace(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", uuid.NewString())
	doc, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{Kind: "Document", Title: "Shared", Content: "worker-visible"})
	if err != nil {
		t.Fatal(err)
	}
	workerModel := &scriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	parentOpts := AgentOptions{
		MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: eng, SearchNamespace: ns,
		Specialists: []*Specialist{{Name: "researcher", Model: workerModel}},
	}
	parentH := mustNewTurnManager(t, parentOpts)
	t.Cleanup(parentH.Close)
	workerOpts := parentOpts.WithSpecialist(parentH.specialists["researcher"])
	workerOpts.SearchNamespace = ns
	workerOpts.sessionID = "w/researcher/spawn_tc1"
	worker := mustNewTurnManager(t, workerOpts)
	t.Cleanup(worker.Close)

	opened, openErr := worker.findTool("open", "").invoke(ctx, `{"ref":"`+doc.ID.String()+`"}`, turnRuntime(worker))
	found, searchErr := worker.findTool("search", "").invoke(ctx, `{"query":"worker-visible"}`, turnRuntime(worker))
	parentH.session.Search.ClearNamespace()
	gotNS, ok := worker.session.Search.Namespace()

	if openErr != nil || !strings.Contains(opened.output, "worker-visible") || searchErr != nil || !strings.Contains(found.output, doc.ID.String()) {
		t.Errorf("worker open = %s %v, search = %s %v, want the parent's record", opened.output, openErr, found.output, searchErr)
	}
	if !ok || !gotNS.Equal(ns) {
		t.Errorf("worker namespace after the parent clears its own = %v %v, want %v", gotNS, ok, ns)
	}
}

// putFixture writes obj straight to the store under its own namespace.
func putFixture(s brain.Store, obj brain.Object) error {
	_, err := s.Put(context.Background(), brain.Scope{Namespace: obj.Namespace}, brain.Indexed{Object: obj, Text: obj.IndexText()})
	return err
}
