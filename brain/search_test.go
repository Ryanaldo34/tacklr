package brain_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// docWithParts saves a Document titled title with one part per body.
func docWithParts(t *testing.T, eng *brain.Engine, scope brain.Scope, title string, props map[string]any, bodies ...string) brain.Object {
	t.Helper()
	parts := make([]brain.Object, len(bodies))
	for i, b := range bodies {
		parts[i] = brain.Object{Kind: "Chunk", Title: title + " part", Content: b}
	}
	doc, err := eng.PutTree(context.Background(), scope, brain.Object{Kind: "Document", Title: title, Properties: props}, brain.Parts(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestSearch_ranksRecordsAndTheirPartsInTheCallersTenant(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	scope, other := brain.Scope{Namespace: tenant(t)}, brain.Scope{Namespace: tenant(t)}
	guide := docWithParts(t, eng, scope, "OAuth guide", nil, "Implement OAuth PKCE for mobile clients with authorization code flow")
	docWithParts(t, eng, other, "Other tenant", nil, "OAuth PKCE elsewhere")
	policy := save(t, eng, scope, brain.Object{Kind: "Fact", Title: "Refund policy", Content: "refunds within 30 days"})
	open := docWithParts(t, eng, scope, "Deal A", map[string]any{"stage": "negotiation"}, "pipeline revenue forecast")
	docWithParts(t, eng, scope, "Deal B", map[string]any{"stage": "closed"}, "pipeline revenue forecast")
	search := func(req brain.SearchRequest) []brain.RichObject {
		page, err := eng.Search(ctx, scope, req)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Collect(page.Results.Objects())
	}

	byPart := search(brain.SearchRequest{Query: "oauth pkce implementation"})
	byRecord := search(brain.SearchRequest{Query: "refunds"})
	filtered := search(brain.SearchRequest{Query: "pipeline revenue", Filters: mustFilter(t, map[string]any{"stage": "negotiation"})})
	byKind := search(brain.SearchRequest{Query: "pipeline revenue", Kind: "Fact"})
	byID := search(brain.SearchRequest{Query: policy.ID.String()})
	exact := search(brain.SearchRequest{Query: `"Refund policy"`})
	_, emptyErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "  "})
	_, filterErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "q", Filters: brain.Filter{Props: map[string]brain.PropFilter{"": {Eq: "x"}}}})
	_, noScope := eng.Search(ctx, brain.Scope{}, brain.SearchRequest{Query: "q"})

	if len(byPart) != 1 || byPart[0].ID != guide.ID || len(byPart[0].Evidence) == 0 || byPart[0].Content != "" {
		t.Errorf("search by a part's words = %+v, want the guide alone, with evidence and without content", byPart)
	}
	if !slices.Equal(idsOf(byRecord), []uuid.UUID{policy.ID}) {
		t.Errorf("search a record without parts = %v, want the policy", titlesOf(byRecord))
	}
	if !slices.Equal(idsOf(filtered), []uuid.UUID{open.ID}) {
		t.Errorf("filter on the record of a matching part = %v, want Deal A", titlesOf(filtered))
	}
	if len(byKind) != 0 {
		t.Errorf("search a kind with no match = %v, want none", titlesOf(byKind))
	}
	if !slices.Equal(idsOf(byID), []uuid.UUID{policy.ID}) || len(exact) == 0 || exact[0].ID != policy.ID {
		t.Errorf("search by id = %v, exact = %v, want the policy", titlesOf(byID), titlesOf(exact))
	}
	if !errors.Is(emptyErr, brain.ErrInvalid) || !errors.Is(filterErr, brain.ErrInvalid) || !errors.Is(noScope, brain.ErrInvalid) {
		t.Errorf("empty query %v, bad filter %v, no scope %v, want ErrInvalid", emptyErr, filterErr, noScope)
	}
}

func TestSearch_facetsCountTheKindsAndFieldValuesOfAPage(t *testing.T) {
	ctx := context.Background()
	open, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	catalogStore := brain.NewMemoryStore()
	catalog, err := brain.NewEngine(catalogStore, brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}}}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	for _, stage := range []string{"open", "open", "won"} {
		save(t, open, scope, brain.Object{Kind: "Deal", Title: "renewal deal", Properties: map[string]any{"stage": stage, "ref": uuid.NewString(), "owner": "ann"}})
		save(t, catalog, scope, brain.Object{Kind: "Deal", Title: "renewal deal", Properties: map[string]any{"stage": stage}})
	}
	for i := range 7 {
		save(t, open, scope, brain.Object{Kind: "Note", Title: fmt.Sprint("renewal note ", i), Properties: map[string]any{"ref": uuid.NewString(), "tags": []any{"a"}}})
	}
	// A row written before the kind dropped a field keeps it.
	if err := putFixture(catalogStore, brain.Object{ID: uuid.New(), Kind: "Deal", Title: "renewal deal", Namespace: scope.Namespace, Properties: map[string]any{"stage": "lost", "color": "red"}}); err != nil {
		t.Fatal(err)
	}

	openPage, openErr := open.Search(ctx, scope, brain.SearchRequest{Query: "renewal", Limit: 20})
	catalogPage, catalogErr := catalog.Search(ctx, scope, brain.SearchRequest{Query: "renewal", Kind: "Deal"})

	if openErr != nil || openPage.Facets.Kinds["Deal"] != 3 || openPage.Facets.Kinds["Note"] != 7 {
		t.Errorf("open-mode kinds = %v %v, want 3 deals and 7 notes", openPage.Facets.Kinds, openErr)
	}
	if got := openPage.Facets.Properties; got["stage"]["open"] != 2 || got["stage"]["won"] != 1 || got["owner"]["ann"] != 3 || got["ref"] != nil || got["tags"] != nil {
		t.Errorf("open-mode properties = %v, want stage and owner, and not the many-valued ref or the list", got)
	}
	if got := catalogPage.Facets.Properties; catalogErr != nil || len(got) != 1 || got["stage"]["open"] != 2 || got["stage"]["won"] != 1 || got["stage"]["lost"] != 1 {
		t.Errorf("catalog properties = %v %v, want only the kind's field", got, catalogErr)
	}
}

func TestSearch_teachingErrorsNameTheValidChoices(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}, {Name: "amount", Type: brain.FieldTypeNumber}}},
		brain.KindSpec{Kind: "Fact", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	search := func(kind string, filters map[string]any) error {
		_, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "q", Kind: kind, Filters: mustFilter(t, filters)})
		return err
	}

	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"an unknown kind":       {search("Phase", nil), "unknown kind \"Phase\"; kinds: Deal, Fact"},
		"an unknown field":      {search("Deal", map[string]any{"phase": "x"}), "unknown filter \"phase\" for Deal; filterable: amount, stage"},
		"a field with no kind":  {search("", map[string]any{"stage": "x"}), "filter \"stage\" needs a kind; kinds: Deal, Fact"},
		"a kind with no fields": {search("Fact", map[string]any{"stage": "x"}), "filterable: none"},
	} {
		if !errors.Is(tc.err, brain.ErrInvalid) || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want ErrInvalid naming %q", name, tc.err, tc.want)
		}
	}
}

func TestSearch_morePagesThroughTheRestEvenAfterACheckpoint(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	for i := range 5 {
		save(t, eng, scope, brain.Object{Kind: "Note", Title: fmt.Sprintf("shared retrieval note %d", i)})
	}
	sc := brain.NewSearchContext()

	first, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "shared retrieval", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	sc.SetRest("search", first.Rest)
	checkpoint, err := sc.Export()
	if err != nil {
		t.Fatal(err)
	}
	restored := brain.NewSearchContext()
	if err := restored.Restore(checkpoint); err != nil {
		t.Fatal(err)
	}
	second, secondErr := eng.Next(ctx, scope, restored.Rest("search"), 2)
	last, lastErr := eng.Next(ctx, scope, second.Rest, 10)
	empty, _ := brain.NewSearchContext().Export()
	badRestore := brain.NewSearchContext().Restore([]byte(`{`))
	badRest := brain.NewSearchContext().Restore([]byte(`{"rests":{"search":{"refs":"x"}}}`))
	staleFilter := brain.NewSearchContext()
	if err := staleFilter.Restore([]byte(`{"rests":{"search":{"refs":[{"id":"` + uuid.NewString() + `"}],"filter":{"CreatedAfter":"yesterday"}}}}`)); err != nil {
		t.Fatal(err)
	}
	_, staleErr := eng.Next(ctx, scope, staleFilter.Rest("search"), 1)

	seen := map[uuid.UUID]bool{}
	for _, o := range slices.Concat(slices.Collect(first.Results.Objects()), slices.Collect(second.Results.Objects()), slices.Collect(last.Results.Objects())) {
		seen[o.ID] = true
	}
	if !first.More || secondErr != nil || !second.More || lastErr != nil || last.More || len(slices.Collect(last.Results.Objects())) != 1 || len(seen) != 5 {
		t.Errorf("pages of 2, 2 and the rest = %v %v %v (more %v %v %v), want all 5 once", titlesOf(slices.Collect(first.Results.Objects())), titlesOf(slices.Collect(second.Results.Objects())), titlesOf(slices.Collect(last.Results.Objects())), first.More, second.More, last.More)
	}
	if empty != nil || badRestore == nil || badRest == nil || !last.Rest.Empty() {
		t.Errorf("empty export = %s, bad restore = %v, bad rest = %v, want nothing and errors", empty, badRestore, badRest)
	}
	if !errors.Is(staleErr, brain.ErrInvalid) {
		t.Errorf("more from a checkpoint whose filter no longer validates = %v, want ErrInvalid", staleErr)
	}
}

func TestSearch_aRerankerReordersAndDropsTheTopResults(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	plain, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	var asked string
	// The reranker reverses the objects and drops the last.
	reranked, err := brain.NewEngine(store, brain.WithReranker(func(_ context.Context, query string, objs []brain.RichObject) ([]brain.RichObject, error) {
		asked = query
		out := slices.Clone(objs)
		slices.Reverse(out)
		return out[:len(out)-1], nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	failing, err := brain.NewEngine(store, brain.WithReranker(func(context.Context, string, []brain.RichObject) ([]brain.RichObject, error) {
		return nil, errString("rerank down")
	}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	save(t, plain, scope, brain.Object{Kind: "Note", Title: "alpha alpha alpha"})
	save(t, plain, scope, brain.Object{Kind: "Note", Title: "alpha beta"})
	save(t, plain, scope, brain.Object{Kind: "Note", Title: "alpha beta gamma delta"})
	query := brain.SearchRequest{Query: "alpha"}

	base, _ := plain.Search(ctx, scope, query)
	got, gotErr := reranked.Search(ctx, scope, query)
	_, failErr := failing.Search(ctx, scope, query)

	want := titlesOf(slices.Collect(base.Results.Objects()))
	slices.Reverse(want)
	if gotErr != nil || !slices.Equal(titlesOf(slices.Collect(got.Results.Objects())), want[:2]) || asked != "alpha" {
		t.Errorf("reranked for %q = %v %v, want %v reversed without the last, for alpha", asked, titlesOf(slices.Collect(got.Results.Objects())), gotErr, titlesOf(slices.Collect(base.Results.Objects())))
	}
	if !errors.Is(failErr, errString("rerank down")) {
		t.Errorf("failing reranker = %v, want its error", failErr)
	}
}

func TestSearch_listsAKindNewestFirstAndSearchesWithinOneRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	contract := docWithParts(t, eng, scope, "contract", nil, "the penalty clause applies", "payment terms", "late penalty fees")
	docWithParts(t, eng, scope, "other", nil, "penalty for another deal")
	first := save(t, eng, scope, brain.Object{Kind: "Note", Title: "first"})
	second := save(t, eng, scope, brain.Object{Kind: "Note", Title: "second penalty"})
	partsTree, err := store.GetTree(ctx, scope, contract.ID, 0)
	parts := partsTree.Parts
	if err != nil {
		t.Fatal(err)
	}
	within := func(ref uuid.UUID) ([]string, error) {
		page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "penalty", Within: ref})
		var got []string
		for _, o := range slices.Collect(page.Results.Objects()) {
			got = append(got, o.Evidence[0].Snippet)
		}
		slices.Sort(got)
		return got, err
	}

	listed, listErr := eng.Search(ctx, scope, brain.SearchRequest{Kind: "Note", Limit: 1})
	rest, restErr := eng.Next(ctx, scope, listed.Rest, 1)
	byRecord, recordErr := within(contract.ID)
	byPart, partErr := within(parts[1].ID)
	_, nothing := eng.Search(ctx, scope, brain.SearchRequest{})
	_, withinNothing := eng.Search(ctx, scope, brain.SearchRequest{Kind: "Note", Within: contract.ID})

	newest := func(p brain.Page) string {
		for o := range p.Results.Objects() {
			return o.Title
		}
		return ""
	}
	if listErr != nil || restErr != nil || !listed.More || newest(listed) != second.Title || newest(rest) != first.Title {
		t.Errorf("list Notes one per page = %v then %v (%v, %v), want second then first", titlesOf(slices.Collect(listed.Results.Objects())), titlesOf(slices.Collect(rest.Results.Objects())), listErr, restErr)
	}
	want := []string{"late penalty fees", "the penalty clause applies"}
	if recordErr != nil || partErr != nil || !slices.Equal(byRecord, want) || !slices.Equal(byPart, want) {
		t.Errorf("search within the contract = %v %v, within one of its parts = %v %v, want its two penalty parts", byRecord, recordErr, byPart, partErr)
	}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{"no query, kind or filters": {nothing, "kind or filters"}, "within with no query": {withinNothing, "related"}} {
		if !errors.Is(tc.err, brain.ErrInvalid) || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want ErrInvalid naming %q", name, tc.err, tc.want)
		}
	}
}
