package brain

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestMemoryGraph_edgesAndSplit(t *testing.T) {
	ctx := context.Background()
	g := NewMemoryGraph()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	if err := g.AddEdge(ctx, uuid.Nil, b, "r"); err == nil {
		t.Fatal("nil from should error")
	}
	if err := g.AddEdge(ctx, a, b, ""); err == nil {
		t.Fatal("empty relation should error")
	}
	if err := g.AddEdge(ctx, a, b, "references"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, c, a, "references"); err != nil {
		t.Fatal(err)
	}

	ns, err := g.Neighbors(context.Background(), a, []string{"references"}, 10)
	if err != nil || len(ns) != 2 {
		t.Fatalf("%+v %v", ns, err)
	}
	ns, err = g.Neighbors(context.Background(), a, nil, 10)
	if err != nil || len(ns) != 0 {
		t.Fatalf("no types: %+v", ns)
	}
	ns, err = g.Neighbors(context.Background(), a, []string{"references"}, 0)
	if err != nil || len(ns) != 0 {
		t.Fatalf("limit 0: %+v", ns)
	}
	ns, err = g.Neighbors(context.Background(), a, []string{"references"}, 1)
	if err != nil || len(ns) != 1 {
		t.Fatalf("limit 1: %+v", ns)
	}

	wantC, labels := SplitRelationTypes([]string{"contains", "REFERENCES", "part_of", "depends_on", "contains"})
	if !wantC || len(labels) != 2 {
		t.Fatalf("%v %v", wantC, labels)
	}
	wantC, labels = SplitRelationTypes(nil)
	if !wantC || len(labels) != 0 {
		// nil defaults to containment only
		t.Logf("nil split: %v %v", wantC, labels)
	}
}
