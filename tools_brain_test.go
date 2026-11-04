package tacklr

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

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
	ns := storetest.Namespace(t, "org", uuid.NewString())
	west := append(ns.Clone(), brain.Attr{Name: "workspace", Value: "west"})
	doc, err := eng.PutTree(ctx, brain.Scope{Namespace: west}, brain.Object{Kind: "Document", Title: "Runbook"},
		brain.Parts(brain.Object{Kind: "Chunk", Content: "restart the knowledge service"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := storetest.Put(store, brain.Object{ID: uuid.New(), Kind: "Fact", Title: "foreign knowledge", Namespace: storetest.Namespace(t, "org", uuid.NewString())}); err != nil {
		t.Fatal(err)
	}
	opts := AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: eng, SearchNamespace: ns, BrainWriteKinds: []string{"Fact"}, sessionID: "brain-tools-1"}
	h := mustNewHarness(t, opts)
	// Each call is a round of its own: its writes commit before the next.
	call := func(h *Harness, name, args string) (string, error) {
		cc := turnRuntime(h)
		out, err := h.findTool(name, "").invoke(ctx, args, cc)
		cc.commit()
		return out.Output, err
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
	listed := page(call(h, "search", `{"kind":"Document","namespace":[{"name":"workspace","value":"west"}]}`))
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
	if titles := titlesOfPage(listed); !slices.Equal(titles, []string{"Runbook"}) {
		t.Errorf("search with a kind and no query = %v, want the kind's records listed", titles)
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
	ns := storetest.Namespace(t, "org", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	deal, err := eng.Save(ctx, scope, brain.Object{Kind: "Deal", Title: "Acme", Content: "Deal body."})
	if err != nil {
		t.Fatal(err)
	}
	person, err := eng.Save(ctx, scope, brain.Object{Kind: "Person", Title: "Sam", Content: "Buyer."})
	if err != nil {
		t.Fatal(err)
	}
	ms := mustMountTree(t, "link-graph", vfs.Request{}, vfs.At("work", vfs.Local(t.TempDir())))
	h := mustNewHarness(t, AgentOptions{sessionID: "link-graph", Model: &scriptedModel{}, Brain: eng, SearchNamespace: ns})
	activatePlan(t, h)
	call := func(name, args string) (string, error) {
		out, err := h.findTool(name, "").invoke(ctx, args, turnRuntime(h))
		return out.Output, err
	}
	if err := ms.Route(ctx, "/workspace/work/doc.md").WriteFile(ctx, []byte("# Doc\n\nartifact\n")); err != nil {
		t.Fatal(err)
	}

	linked, linkErr := call("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":"about","note":"signs the renewal contract"}`)
	found, foundErr := call("search", `{"query":"signs the renewal contract"}`)
	related, relatedErr := call("related", `{"ref":"`+deal.ID.String()+`","relation":"about"}`)
	_, unindexed := call("link", `{"from":"/workspace/work/doc.md","to":"`+deal.ID.String()+`","relation":"about"}`)
	indexer := &vfsindex.MountIndexer{VFS: ms, Brain: eng, Scope: scope}
	if _, err := indexer.IndexPath(ctx, "/workspace/work/doc.md"); err != nil {
		t.Fatal(err)
	}
	_, byPath := call("link", `{"from":"/workspace/work/doc.md","to":"`+deal.ID.String()+`","relation":"about"}`)
	relatedByPath, relatedByPathErr := call("related", `{"ref":"/workspace/work/doc.md","relation":"about"}`)
	removed, removeErr := call("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":"about","remove":true}`)
	after, _ := call("related", `{"ref":"`+deal.ID.String()+`","relation":"about"}`)
	_, noTo := call("link", `{"from":"`+deal.ID.String()+`","to":"","relation":"about"}`)
	_, noRelation := call("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":" "}`)
	reserved := `"namespace":[{"name":"session","value":"x"}]`
	_, linkReserved := call("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":"about",`+reserved+`}`)
	_, openReserved := call("open", `{"ref":"`+deal.ID.String()+`",`+reserved+`}`)

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
	if relatedByPathErr != nil || !strings.Contains(relatedByPath, deal.ID.String()) {
		t.Errorf("related by the file's path = %s %v, want the deal", relatedByPath, relatedByPathErr)
	}
	if removeErr != nil || !strings.Contains(removed, `"linked": false`) || strings.Contains(after, person.ID.String()) {
		t.Errorf("remove = %s %v, then related %s, want the person gone", removed, removeErr, after)
	}
	if noTo == nil || !strings.Contains(noTo.Error(), "to is required") {
		t.Errorf("link with no to = %v, want it named", noTo)
	}
	if noRelation == nil || !strings.Contains(noRelation.Error(), "relation are required") {
		t.Errorf("link with no relation = %v, want it rejected", noRelation)
	}
	for name, err := range map[string]error{"link": linkReserved, "open": openReserved} {
		if err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("%s in the reserved session namespace = %v, want it rejected", name, err)
		}
	}
}

func TestWorkerInheritsBrainAndNamespace(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", uuid.NewString())
	doc, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{Kind: "Document", Title: "Shared", Content: "worker-visible"})
	if err != nil {
		t.Fatal(err)
	}
	workerModel := &scriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*message.Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
		},
	}
	parentOpts := AgentOptions{
		MaxWindowSize: 1024, Model: &scriptedModel{}, Brain: eng, SearchNamespace: ns,
		Specialists: []*Specialist{{Name: "researcher", Model: workerModel}},
	}
	parentH := mustNewHarness(t, parentOpts)
	workerOpts := parentOpts.WithSpecialist(parentH.specialists["researcher"])
	workerOpts.SearchNamespace = ns
	workerOpts.sessionID = "w/researcher/spawn_tc1"
	worker := mustNewHarness(t, workerOpts)

	opened, openErr := worker.findTool("open", "").invoke(ctx, `{"ref":"`+doc.ID.String()+`"}`, turnRuntime(worker))
	found, searchErr := worker.findTool("search", "").invoke(ctx, `{"query":"worker-visible"}`, turnRuntime(worker))
	gotNS, ok := worker.session.Search.Namespace()

	if openErr != nil || !strings.Contains(opened.Output, "worker-visible") || searchErr != nil || !strings.Contains(found.Output, doc.ID.String()) {
		t.Errorf("worker open = %s %v, search = %s %v, want the parent's record", opened.Output, openErr, found.Output, searchErr)
	}
	if !ok || !gotNS.Equal(ns) {
		t.Errorf("worker namespace = %v %v, want %v", gotNS, ok, ns)
	}
}
