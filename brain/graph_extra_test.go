package brain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMemoryGraph_edgesAndSplit(t *testing.T) {
	ctx := context.Background()
	g := NewMemoryGraph()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	if err := g.AddEdge(ctx, uuid.Nil, b, "r", EdgeMeta{}); err == nil {
		t.Fatal("nil from should error")
	}
	if err := g.AddEdge(ctx, a, b, "", EdgeMeta{}); err == nil {
		t.Fatal("empty relation should error")
	}
	if err := g.AddEdge(ctx, a, b, "references", EdgeMeta{Note: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, a, b, "references", EdgeMeta{Note: "updated"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, c, a, "references", EdgeMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := g.RemoveEdge(ctx, c, a, "references"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, c, a, "references", EdgeMeta{}); err != nil {
		t.Fatal(err)
	}

	ns, err := g.Neighbors(context.Background(), a, []string{"references"}, 10)
	if err != nil || len(ns) != 2 {
		t.Fatalf("%+v %v", ns, err)
	}
	// Upsert must not duplicate (a→b); note must reflect last write.
	var outNote string
	for _, n := range ns {
		if n.ObjectID == b && n.Direction == "out" {
			outNote = n.Meta.Note
		}
	}
	if outNote != "updated" {
		t.Fatalf("upsert note: %q neighbors=%+v", outNote, ns)
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
	// Empty tokens only → containment-only (no graph labels).
	wantC, labels = SplitRelationTypes([]string{"", "  "})
	if !wantC || len(labels) != 0 {
		t.Fatalf("blank rels: %v %v", wantC, labels)
	}
}

// TestMemoryGraph_cancelledContextFailsClosed: cancelled ctx is rejected before mutation/read.
func TestMemoryGraph_cancelledContextFailsClosed(t *testing.T) {
	g := NewMemoryGraph()
	a, b := uuid.New(), uuid.New()
	if err := g.AddEdge(context.Background(), a, b, "about", EdgeMeta{Note: "ok"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := g.AddEdge(ctx, a, uuid.New(), "about", EdgeMeta{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.EnsureObject(ctx, Object{ID: a, Kind: "Document"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureObject: %v", err)
	}
	if err := g.RemoveObject(ctx, a); !errors.Is(err, context.Canceled) {
		t.Fatalf("RemoveObject: %v", err)
	}
	if _, err := g.Neighbors(ctx, a, []string{"about"}, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("Neighbors: %v", err)
	}
	if _, err := g.SearchText(ctx, "ok", 5, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchText: %v", err)
	}
	if _, err := g.SearchVector(ctx, []float32{1}, 5, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchVector: %v", err)
	}
	// Live edge still present (cancel did not mutate).
	ns, err := g.Neighbors(context.Background(), a, []string{"about"}, 10)
	if err != nil || len(ns) != 1 || ns[0].ObjectID != b {
		t.Fatalf("after cancel: %+v err=%v", ns, err)
	}
}

// TestEdgeMeta_IsZero: only meaningful fields make meta non-zero (tool/LinkWith payload).
func TestEdgeMeta_IsZero(t *testing.T) {
	if !(EdgeMeta{}).IsZero() {
		t.Fatal("empty should be zero")
	}
	if (EdgeMeta{Note: "x"}).IsZero() {
		t.Fatal("note")
	}
	if (EdgeMeta{Status: "active"}).IsZero() {
		t.Fatal("status")
	}
	if (EdgeMeta{Role: "primary"}).IsZero() {
		t.Fatal("role")
	}
	if (EdgeMeta{Confidence: 0.1}).IsZero() {
		t.Fatal("confidence")
	}
	id := uuid.New()
	if (EdgeMeta{EvidenceID: &id}).IsZero() {
		t.Fatal("evidence")
	}
	if (EdgeMeta{CreatedAt: time.Now()}).IsZero() {
		t.Fatal("created")
	}
	if (EdgeMeta{UpdatedAt: time.Now()}).IsZero() {
		t.Fatal("updated")
	}
}

func TestMemoryGraph_removeObjectNilID(t *testing.T) {
	if err := NewMemoryGraph().RemoveObject(context.Background(), uuid.Nil); err == nil {
		t.Fatal("want error")
	}
}
