package brain_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestRelated_listsARecordsChildrenOrAPartsParentAndNearbyParts: Related lists
// a record's parts in order, counted and in pages, or a part's record and the
// parts next to it, which a filter narrows; a graph relation with no graph
// lists nothing; an unknown id is not found, no id, an empty relation, an
// unknown direction or a bad filter is invalid, and a cancelled call stops.
func TestRelated_listsARecordsChildrenOrAPartsParentAndNearbyParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{SiblingRadius: 1}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	doc := docWithParts(t, eng, scope, "Doc", nil, "p1", "p2", "p3", "p4", "p5", "p6", "p7")
	partsTree, err := store.GetTree(ctx, scope, doc.ID, 0)
	parts := partsTree.Parts
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	children, childrenErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Count: true})
	first, firstErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Relations: []string{"contains"}, Limit: 3})
	rest, restErr := eng.Next(ctx, scope, first.Rest, 10)
	around, aroundErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: parts[3].ID})
	parentOnly, parentErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: parts[3].ID, Filters: brain.Filter{Kind: brain.StringMatch{Eq: "Document"}}})
	byRelation, relationErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Relations: []string{"references"}})
	_, missing := eng.Related(ctx, scope, brain.RelatedRequest{ID: uuid.New()})
	_, noID := eng.Related(ctx, scope, brain.RelatedRequest{})
	_, emptyRelation := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Relations: []string{" "}})
	_, sideways := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Direction: "sideways"})
	_, badFilter := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Filters: brain.Filter{UpdatedAfter: "yesterday"}})
	_, stopped := eng.Related(cancelled, scope, brain.RelatedRequest{ID: doc.ID})

	got := slices.Collect(children.Results.Objects())
	if childrenErr != nil || !slices.Equal(idsOf(got), idsOf(objectsOfParts(parts))) || got[0].Relation.Type != brain.RelationChild {
		t.Errorf("children = %v %v, want the 7 parts in order, tagged child", titlesOf(got), childrenErr)
	}
	if c := children.Counts; c == nil || c.Total != 7 || !maps.Equal(c.ByRelation, map[string]int{brain.RelationChild: 7}) {
		t.Errorf("children counted = %+v, want 7 children", c)
	}
	if got := idsOf(slices.Collect(parentOnly.Results.Objects())); parentErr != nil || !slices.Equal(got, []uuid.UUID{doc.ID}) {
		t.Errorf("around a part, only Documents = %v %v, want its record", got, parentErr)
	}
	if firstErr != nil || restErr != nil || !first.More || len(slices.Collect(first.Results.Objects())) != 3 || len(slices.Collect(rest.Results.Objects())) != 4 || rest.More {
		t.Errorf("children in pages of 3 = %d then %d (more %v %v), want 3 then 4", len(slices.Collect(first.Results.Objects())), len(slices.Collect(rest.Results.Objects())), first.More, rest.More)
	}
	var tags []string
	for _, o := range slices.Collect(around.Results.Objects()) {
		tags = append(tags, o.Relation.Type)
	}
	if aroundErr != nil || !slices.Equal(idsOf(slices.Collect(around.Results.Objects())), []uuid.UUID{doc.ID, parts[2].ID, parts[4].ID}) || !slices.Equal(tags, []string{brain.RelationParent, brain.RelationSibling, brain.RelationSibling}) {
		t.Errorf("around a part = %v %v %v, want its parent and the parts next to it", titlesOf(slices.Collect(around.Results.Objects())), tags, aroundErr)
	}
	if relationErr != nil || len(byRelation.Results) != 0 {
		t.Errorf("a graph relation without a graph = %+v %v, want nothing", byRelation.Results, relationErr)
	}
	if !errors.Is(missing, brain.ErrNotFound) || !errors.Is(stopped, context.Canceled) {
		t.Errorf("missing %v, cancelled %v", missing, stopped)
	}
	for name, err := range map[string]error{"no id": noID, "an empty relation": emptyRelation, "an unknown direction": sideways, "a bad filter": badFilter} {
		if !errors.Is(err, brain.ErrInvalid) {
			t.Errorf("related with %s = %v, want ErrInvalid", name, err)
		}
	}
}

func objectsOfParts(parts []brain.Object) []brain.RichObject {
	out := make([]brain.RichObject, len(parts))
	for i, p := range parts {
		out[i] = p.Rich(false)
	}
	return out
}

// TestSearch_theStoreDecidesGraphHitsLinkOnlyPagesAndRelationlessGraphs:
// a graph hit whose stored row no longer passes the filter is dropped, a
// reranked page of links alone keeps its links, and a graph that indexes no
// relation still serves objects.
func TestSearch_theStoreDecidesGraphHitsLinkOnlyPagesAndRelationlessGraphs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(helixtest.Graph(t)), brain.WithReranker(func(_ context.Context, _ string, objs []brain.RichObject) ([]brain.RichObject, error) {
		return objs, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	bare, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Open(t, func(o *helixgraph.Options) { o.EdgeRelations = nil })))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	deal := save(t, eng, scope, brain.Object{Kind: "Deal", Title: "acme renewal", Properties: map[string]any{"stage": "open"}})
	person := save(t, eng, scope, brain.Object{Kind: "Person", Title: "Sam"})
	link(t, eng, scope, deal.ID, person.ID, "about", "countersigned quarterly")
	moved := deal
	moved.Properties = map[string]any{"stage": "closed"}
	if err := storetest.Put(store, moved); err != nil {
		t.Fatal(err)
	}
	save(t, bare, scope, brain.Object{Kind: "Deal", Title: "acme renewal"})

	stale, staleErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "acme renewal", Filters: storetest.Filter(t, map[string]any{"stage": "open"})})
	closed, closedErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "acme renewal", Filters: storetest.Filter(t, map[string]any{"stage": "closed"})})
	links, linksErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "countersigned quarterly"})
	plain, plainErr := bare.Search(ctx, scope, brain.SearchRequest{Query: "acme renewal"})

	if staleErr != nil || closedErr != nil || len(stale.Results) != 0 || !slices.Equal(idsOf(slices.Collect(closed.Results.Objects())), []uuid.UUID{deal.ID}) {
		t.Errorf("stage open = %v %v, closed = %v %v, want only the stored stage to match", titlesOf(slices.Collect(stale.Results.Objects())), staleErr, titlesOf(slices.Collect(closed.Results.Objects())), closedErr)
	}
	var firstLink brain.LinkResult
	for l := range links.Results.Links() {
		firstLink = l
		break
	}
	if linksErr != nil || len(links.Results) != 1 || firstLink.To.ID != person.ID {
		t.Errorf("search a note = %+v %v, want the link", links.Results, linksErr)
	}
	if plainErr != nil || len(slices.Collect(plain.Results.Objects())) != 1 {
		t.Errorf("search with a relationless graph = %v %v, want the deal", titlesOf(slices.Collect(plain.Results.Objects())), plainErr)
	}
}
