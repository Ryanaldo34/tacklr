package tacklr_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// committedResult runs tc as a round of its own on h and returns its result.
func committedResult(t *testing.T, h *tacklr.Harness, tc string, args string) string {
	t.Helper()
	r, err := runRound(t, h, call("c-"+tc, tc, args))
	if err != nil {
		t.Fatal(err)
	}
	return r.Calls[0].Body.Text()
}

func page(t *testing.T, out string) brain.Page {
	t.Helper()
	var p brain.Page
	if err := json.Unmarshal([]byte(out), &p); err != nil {
		t.Fatalf("page %q: %v", out, err)
	}
	return p
}

func titlesOfPage(p brain.Page) []string {
	var out []string
	for o := range p.Results.Objects() {
		out = append(out, o.Title)
	}
	return out
}

// TestBrainTools_saveSearchOpenAndRelatedServeOneSession: save writes a
// record of a writable kind; search pages its results with facets, and
// its next page survives a checkpoint; search narrows by namespace and
// lists a kind; open reads a record with its children and related its
// parts; a search's rest carries to the next step through Commit. Each bad
// request is the model's to correct.
func TestBrainTools_saveSearchOpenAndRelatedServeOneSession(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
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
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 100000, Brain: eng, SearchNamespace: ns, BrainWriteKinds: []string{"Fact"}}
	h := planned(t, "brain-tools-1", opts, tacklr.Todo{Title: "learn"})
	var saved []brain.RichObject
	for _, title := range []string{"knowledge one", "knowledge two", "knowledge three"} {
		var rich brain.RichObject
		if err := json.Unmarshal([]byte(committedResult(t, h, "save", `{"kind":"Fact","title":"`+title+`","content":"body","properties":{"topic":"ops"}}`)), &rich); err != nil {
			t.Fatal(err)
		}
		saved = append(saved, rich)
	}
	inWest := `"namespace":[{"name":"workspace","value":"west"}]`

	first := page(t, committedResult(t, h, "search", `{"query":"knowledge","kind":"Fact"}`))
	rows, _ := h.Checkpoint()
	reloaded := newHarness(t, "brain-tools-1", opts)
	restoreErr := reloaded.RestoreCheckpoint(rows)
	second := page(t, result(t, reloaded, call("s2", "search", `{"more":true}`)))
	updated := committedResult(t, h, "save", `{"kind":"Fact","id":"`+saved[0].ID.String()+`","title":"knowledge one, revised"}`)
	narrowed := page(t, committedResult(t, h, "search", `{"query":"knowledge",`+inWest+`}`))
	listed := page(t, committedResult(t, h, "search", `{"kind":"Document",`+inWest+`}`))
	opened := committedResult(t, h, "open", `{"ref":"`+doc.ID.String()+`",`+inWest+`}`)
	related := page(t, committedResult(t, h, "related", `{"ref":"`+doc.ID.String()+`",`+inWest+`}`))
	bad := map[string]string{
		`{"more":true,"ref":"x"}`: "no more results",
		`{"ref":""}`:              "ref is required",
		`{"ref":"` + doc.ID.String() + `","namespace":[{"name":"org","value":"other"}]}`: "outside host scope",
	}
	badSearch := map[string]string{
		`{"query":"knowledge","kind":"Fact","filters":{"phase":"x"}}`:        `unknown filter "phase" for Fact; filterable: topic`,
		`{"query":"knowledge","namespace":[{"name":"session","value":"x"}]}`: "reserved",
		`{"query":"x","namespace":[{"name":"org","value":"other"}]}`:         "outside host scope",
		`{"query":"knowledge","filters":{"created_after":"yesterday"}}`:      "created_after",
		`{"query":"knowledge","within":"not a path"}`:                        "neither a UUID nor a path",
	}
	badSave := map[string]string{
		`{"kind":"Memory","title":"x"}`:                                            `kind "Memory" cannot be saved; kinds: Fact`,
		`{"kind":"Fact","title":" "}`:                                              "title is required",
		`{"kind":"Fact","title":"x","id":"nope"}`:                                  "id must be a UUID",
		`{"kind":"Fact","title":"x","namespace":[{"name":"org","value":"other"}]}`: "outside host scope",
		`{"kind":"Fact","title":"x","properties":{"color":"red"}}`:                 "color",
	}
	badOpen := map[string]string{
		`{"ref":"not a path"}`:               "neither a UUID nor a path",
		`{"ref":"` + uuid.NewString() + `"}`: "not found",
	}

	if got := slices.Collect(first.Results.Objects()); len(got) != 2 || !first.More || first.Facets.Kinds["Fact"] != 2 || first.Facets.Properties["topic"]["ops"] != 2 {
		t.Errorf("first search = %+v, want 2 facts, more, and facets naming the kind and topic", first)
	}
	if got := slices.Collect(second.Results.Objects()); restoreErr != nil || len(got) != 1 || second.More || slices.Contains(titlesOfPage(first), got[0].Title) {
		t.Errorf("more after a checkpoint = %+v %v, want the third fact", second, restoreErr)
	}
	if !containsAll(updated, "revised", saved[0].ID.String()) {
		t.Errorf("save with an id = %s, want the record updated", updated)
	}
	if got := titlesOfPage(narrowed); !slices.Equal(got, []string{"Runbook"}) {
		t.Errorf("narrowed search = %v, want the west runbook only", got)
	}
	if got := titlesOfPage(listed); !slices.Equal(got, []string{"Runbook"}) {
		t.Errorf("search with a kind and no query = %v, want the kind's records listed", got)
	}
	if !containsAll(opened, `"kind": "Document"`, `"children"`) {
		t.Errorf("open = %s, want the record, its kind and its children", opened)
	}
	if got := slices.Collect(related.Results.Objects()); len(got) != 1 || got[0].Relation.Type != brain.RelationChild {
		t.Errorf("related = %+v, want the runbook's part as a child", related)
	}
	for tool, cases := range map[string]map[string]string{"related": bad, "search": badSearch, "save": badSave, "open": badOpen} {
		for args, want := range cases {
			if got := result(t, h, call("bad", tool, args)); !strings.Contains(got, want) {
				t.Errorf("%s %s = %q, want %q", tool, args, got, want)
			}
		}
	}
}

// TestRunCall_aSearchCursorCarriesToTheNextStepThroughCommit: a search
// leaves the rest of its results as the session's cursor; the Tool step
// saves nothing, the round's Commit keeps the cursor, and the next step's
// search with more continues it. A host interceptor that asks for each
// page in one call continues the call's own cursor.
func TestRunCall_aSearchCursorCarriesToTheNextStepThroughCommit(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithConfig(brain.EngineConfig{DefaultLimit: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "cursor")
	for _, title := range []string{"cursor one", "cursor two"} {
		if _, err := eng.Save(t.Context(), brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: title, Content: "cursor"}); err != nil {
			t.Fatal(err)
		}
	}
	// pageAll gives a search every page of its results in one result.
	pageAll := func(ctx context.Context, inv tacklr.ToolInvocation, next tacklr.ToolCallFunc) (tacklr.ToolOutcome, error) {
		out, err := next(ctx, inv)
		all := out.Output
		for err == nil && strings.Contains(out.Output, `"more": true`) {
			inv.ArgsJSON = `{"more":true}`
			out, err = next(ctx, inv)
			all += out.Output
		}
		out.Output = all
		return out, err
	}
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 100000, Brain: eng, SearchNamespace: ns}
	h := newHarness(t, "cursor", opts)
	prompt(t, h, "go")
	opts.ToolInterceptors = []tacklr.ToolInterceptor{pageAll}
	paged := result(t, newHarness(t, "cursor-pages", opts), call("s4", "search", `{"query":"cursor"}`))
	out := newStream(t).out
	r := tacklr.NewRound(reply(call("s1", "search", `{"query":"cursor"}`)))

	r.Calls[0], err = h.RunCall(t.Context(), r.Calls[0], out)
	before := result(t, h, call("s2", "search", `{"more":true}`))
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)
	more := result(t, h, call("s3", "search", `{"more":true}`))

	if err != nil || commitErr != nil || !strings.Contains(more, "cursor") || strings.Contains(more, "no more results") {
		t.Fatalf("search %v, commit %v, more %q; want the next step to continue the committed cursor", err, commitErr, more)
	}
	if !strings.Contains(before, "no more results") {
		t.Fatalf("more before the commit = %q, want no cursor yet: the Tool step saved nothing", before)
	}
	if !containsAll(paged, "cursor one", "cursor two") {
		t.Fatalf("every page in one call = %q, want both facts", paged)
	}
}

// TestBrainTools_linkByIDOrIndexedPathAndFindItByItsNote: records link by
// id, an indexed file by its path (an unindexed one cannot be an end), the
// note makes the link searchable, and remove takes it away. related pages
// past 50 links, also after a checkpoint, counts them, and narrows by kind
// and link status; search near a record finds only its linked records.
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
	// 55 contacts, the first 5 on active links, and one that is not linked.
	var contacts []uuid.UUID
	for i := range 55 {
		contact, err := eng.Save(ctx, scope, brain.Object{Kind: "Person", Title: "contact", Content: "account contact"})
		if err != nil {
			t.Fatal(err)
		}
		status := ""
		if i < 5 {
			status = "active"
		}
		if err := eng.Link(ctx, scope, brain.LinkRequest{From: deal.ID, To: contact.ID, Relation: "has_contact", Meta: brain.EdgeMeta{Status: status}}); err != nil {
			t.Fatal(err)
		}
		contacts = append(contacts, contact.ID)
	}
	outsider, err := eng.Save(ctx, scope, brain.Object{Kind: "Person", Title: "contact", Content: "account contact"})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir()))).WithCodecs(adapters.Codecs()...)(ctx, "link-graph", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/work/doc.md").WriteFile(ctx, []byte("# Doc\n\nartifact\n")); err != nil {
		t.Fatal(err)
	}
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 100000, Brain: eng, SearchNamespace: ns}
	h := planned(t, "link-graph", opts, tacklr.Todo{Title: "link"})
	run := func(name, args string) string { return result(t, h, call("l", name, args)) }
	link := func(from, to, extra string) string {
		return run("link", `{"from":"`+from+`","to":"`+to+`","relation":"about"`+extra+`}`)
	}
	const docPath = "/workspace/work/doc.md"

	linked := link(deal.ID.String(), person.ID.String(), `,"note":"signs the renewal contract"`)
	found := run("search", `{"query":"signs the renewal contract"}`)
	related := run("related", `{"ref":"`+deal.ID.String()+`","relations":["about"]}`)
	dealRef := `"ref":"` + deal.ID.String() + `"`
	firstContacts := page(t, committedResult(t, h, "related", `{`+dealRef+`,"relations":["has_contact"],"direction":"out","limit":50}`))
	rows, _ := h.Checkpoint()
	reloaded := newHarness(t, "link-graph", opts)
	restoreErr := reloaded.RestoreCheckpoint(rows)
	moreContacts := page(t, result(t, reloaded, call("r2", "related", `{"more":true}`)))
	active := page(t, run("related", `{`+dealRef+`,"kind":"Person","status":"active"}`))
	near := page(t, run("search", `{"query":"account contact","limit":50,"near":{`+dealRef+`,"relations":["has_contact"]}}`))
	nearBadRef := run("search", `{"query":"contact","near":{"ref":"not a path"}}`)
	unindexed := link(docPath, deal.ID.String(), "")
	indexer := &vfsindex.MountIndexer{VFS: ms, Brain: eng, Scope: scope}
	if _, err := indexer.IndexPath(ctx, docPath); err != nil {
		t.Fatal(err)
	}
	byPath := link(docPath, deal.ID.String(), "")
	relatedByPath := run("related", `{"ref":"`+docPath+`","relations":["about"]}`)
	removed := link(deal.ID.String(), person.ID.String(), `,"remove":true`)
	after := run("related", `{"ref":"`+deal.ID.String()+`","relations":["about"]}`)
	noTo := link(deal.ID.String(), "", "")
	noRelation := run("link", `{"from":"`+deal.ID.String()+`","to":"`+person.ID.String()+`","relation":" "}`)
	reserved := `,"namespace":[{"name":"session","value":"x"}]`
	linkReserved := link(deal.ID.String(), person.ID.String(), reserved)
	openReserved := run("open", `{"ref":"`+deal.ID.String()+`"`+reserved+`}`)

	if !containsAll(linked, `"linked": true`, person.ID.String()) || !containsAll(found, `"relation": "about"`, "signs the renewal contract") || !strings.Contains(related, person.ID.String()) {
		t.Errorf("link = %s, search the note = %s, related = %s; want the link, found by its note", linked, found, related)
	}
	if !strings.Contains(unindexed, "not indexed") || !containsAll(byPath, `"linked": true`) || !strings.Contains(relatedByPath, deal.ID.String()) {
		t.Errorf("link an unindexed file = %s, then an indexed one = %s, related by path = %s; want not indexed, then linked", unindexed, byPath, relatedByPath)
	}
	if !strings.Contains(removed, `"linked": false`) || strings.Contains(after, person.ID.String()) {
		t.Errorf("remove = %s, then related %s, want the person gone", removed, after)
	}
	allContacts := append(slices.Collect(firstContacts.Results.Objects()), slices.Collect(moreContacts.Results.Objects())...)
	if c := firstContacts.Counts; c == nil || c.Total != 55 || c.ByRelation["has_contact"] != 55 || len(slices.Collect(firstContacts.Results.Objects())) != 50 || !firstContacts.More {
		t.Errorf("related contacts = %d results, more %v, counts %+v, want 50 of 55 and more", len(slices.Collect(firstContacts.Results.Objects())), firstContacts.More, c)
	}
	if got := idsOfRich(allContacts); restoreErr != nil || moreContacts.More || !slices.Equal(slices.Sorted(slices.Values(uuidStrings(got))), slices.Sorted(slices.Values(uuidStrings(contacts)))) {
		t.Errorf("related contacts over two pages and a checkpoint = %d (more %v, %v), want each of the 55 once", len(got), moreContacts.More, restoreErr)
	}
	if c := active.Counts; c == nil || c.Total != 5 || len(active.Results) == 0 {
		t.Errorf("related active contacts = %+v, want 5 counted", c)
	}
	if got := idsOfRich(slices.Collect(near.Results.Objects())); len(got) == 0 || slices.Contains(got, outsider.ID) || !slices.Contains(contacts, got[0]) {
		t.Errorf("search near the deal = %v, want its contacts only", got)
	}
	for got, want := range map[string]string{noTo: "to is required", noRelation: "relation are required", linkReserved: "reserved", openReserved: "reserved", nearBadRef: "neither a UUID nor a path"} {
		if !strings.Contains(got, want) {
			t.Errorf("result = %q, want %q", got, want)
		}
	}
}

// TestWithSpecialist_aWorkerSharesTheBrainAndSkipsThePlanLock: a worker
// built for a nested specialist reads and searches the parent's Brain in
// its namespace, and writes without a plan.
func TestWithSpecialist_aWorkerSharesTheBrainAndSkipsThePlanLock(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", uuid.NewString())
	doc, err := eng.Save(t.Context(), brain.Scope{Namespace: ns}, brain.Object{Kind: "Document", Title: "Shared", Content: "worker-visible"})
	if err != nil {
		t.Fatal(err)
	}
	model, _ := scripted(t)
	parent := tacklr.AgentOptions{
		MaxWindowSize: 100000, Model: model, Brain: eng, SearchNamespace: ns, BrainWriteKinds: []string{"Note"},
		Specialists: []*tacklr.Specialist{{Name: "lead", Model: model, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}},
	}
	spec := tacklr.FindSpecialist(parent.Specialists, " researcher ")
	worker := newHarness(t, tacklr.ChildID("parent", "researcher", "spawn_tc1", true), parent.WithSpecialist(spec))

	opened := result(t, worker, call("o", "open", `{"ref":"`+doc.ID.String()+`"}`))
	found := result(t, worker, call("s", "search", `{"query":"worker-visible"}`))
	saved := result(t, worker, call("w", "save", `{"kind":"Note","title":"from the worker"}`))

	if spec == nil || !strings.Contains(opened, "worker-visible") || !strings.Contains(found, doc.ID.String()) || !strings.Contains(saved, "from the worker") {
		t.Errorf("worker open = %s, search = %s, save = %s; want the parent's record and a write without a plan", opened, found, saved)
	}
}

func idsOfRich(objs []brain.RichObject) []uuid.UUID {
	out := make([]uuid.UUID, len(objs))
	for i, o := range objs {
		out[i] = o.ID
	}
	return out
}

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}
