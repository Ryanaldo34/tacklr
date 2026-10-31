package brain_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestSearch_parentWithoutPartsIsFoundByFindObjects: search looks at chunks;
// find_objects looks at parent records. An Engram with no parts is missing from
// search and present in find_objects.
func TestSearch_parentWithoutPartsIsFoundByFindObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := helixtest.Graph(t)
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
		}},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	sc := brain.NewSearchContext()

	deal, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Acme renewal", Summary: "enterprise opportunity",
		Content:    "pricing memo",
		Properties: map[string]any{"stage": "negotiation"},
	})
	if err != nil {
		t.Fatal(err)
	}

	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "Acme renewal"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 0 {
		t.Fatalf("search must miss parent-only object: %+v", page.Objects)
	}

	found, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "Acme renewal"}, sc)
	if err != nil || len(found.Objects) != 1 || found.Objects[0].ID != deal.ID {
		t.Fatalf("find_objects title: %+v err=%v", found.Objects, err)
	}
	byStage, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query:   "Acme",
		Filters: mustFilter(t, map[string]any{"kind": "Deal", "stage": "negotiation"}),
	}, sc)
	if err != nil || len(byStage.Objects) != 1 || byStage.Objects[0].ID != deal.ID {
		t.Fatalf("find_objects stage filter: %+v err=%v", byStage.Objects, err)
	}

	pos := 1
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "thread", Content: "pkce flow on the renewal",
		ParentID: &deal.ID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}

	corpus, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "pkce"}, sc)
	if err != nil || len(corpus.Objects) != 1 || corpus.Objects[0].ID != deal.ID {
		t.Fatalf("search after chunk: %+v err=%v", corpus.Objects, err)
	}
	if len(corpus.Objects[0].Evidence) == 0 {
		t.Fatal("search must attach part evidence")
	}
}
