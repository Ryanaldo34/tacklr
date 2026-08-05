package helixgraph_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
)

// TestGraph_ensureObjectAndAddEdgeRequestShape covers write RPC construction under -short.
func TestGraph_ensureObjectAndAddEdgeRequestShape(t *testing.T) {
	ctx := context.Background()
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(b))
		_ = json.NewEncoder(w).Encode(map[string]any{})
	}))
	t.Cleanup(srv.Close)

	g, err := helixgraph.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	pid := uuid.New()
	ns := uuid.New()
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	obj := brain.Object{
		ID:          uuid.New(),
		Kind:        "Document",
		Title:       "memo",
		Summary:     "sum",
		Content:     "body text",
		NamespaceID: ns,
		ParentID:    &pid,
		CreatedAt:   now,
		UpdatedAt:   now,
		Embedding:   []float32{0.1, 0.2},
	}
	if err := g.EnsureObject(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("bodies: %d", len(bodies))
	}
	ensure := bodies[0]
	for _, want := range []string{
		obj.ID.String(), "Document", "memo", "sum", "body text",
		ns.String(), pid.String(), "search_text", "embedding",
		"created_at", "updated_at", "AddN", "Drop",
	} {
		if !strings.Contains(ensure, want) {
			t.Fatalf("ensure body missing %q:\n%s", want, ensure)
		}
	}

	// Index ensure RPC.
	if err := g.EnsureObjectIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !strings.Contains(bodies[1], "object_id") {
		t.Fatalf("index body: %v", bodies)
	}

	// Edge RPC.
	from, to := uuid.New(), uuid.New()
	if err := g.AddEdge(ctx, from, to, "references"); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 3 {
		t.Fatalf("bodies after edge: %d", len(bodies))
	}
	edge := bodies[2]
	for _, want := range []string{from.String(), to.String(), "references", "AddE"} {
		if !strings.Contains(edge, want) {
			t.Fatalf("edge body missing %q:\n%s", want, edge)
		}
	}
}

// TestGraph_writeValidation covers EnsureObject / AddEdge argument failures without RPC.
func TestGraph_writeValidation(t *testing.T) {
	ctx := context.Background()
	g, err := helixgraph.New("http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObject(ctx, brain.Object{}); err == nil {
		t.Fatal("nil object id")
	}
	if err := g.AddEdge(ctx, uuid.New(), uuid.Nil, "r"); err == nil {
		t.Fatal("nil to")
	}
}

// TestGraph_searchTextAndVectorRequestShape covers native Helix search RPC construction.
func TestGraph_searchTextAndVectorRequestShape(t *testing.T) {
	ctx := context.Background()
	var bodies []string
	oid := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(b))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hits": map[string]any{
				"properties": []map[string]any{
					{"object_id": oid.String(), "$distance": 0.1},
				},
			},
		})
	}))
	t.Cleanup(srv.Close)

	g, err := helixgraph.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := g.SearchText(ctx, "commercial risk", 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != oid {
		t.Fatalf("text hits: %+v", hits)
	}
	if !strings.Contains(bodies[0], "TextSearchNodes") && !strings.Contains(bodies[0], "search_text") {
		t.Fatalf("text body: %s", bodies[0])
	}

	hits, err = g.SearchVector(ctx, []float32{1, 0}, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != oid {
		t.Fatalf("vector hits: %+v", hits)
	}
	if !strings.Contains(bodies[1], "VectorSearchNodes") && !strings.Contains(bodies[1], "embedding") {
		t.Fatalf("vector body: %s", bodies[1])
	}

	if err := g.EnsureSearchIndexes(ctx, true); err != nil {
		t.Fatal(err)
	}
	if len(bodies) < 3 {
		t.Fatalf("index ensure RPCs: %d", len(bodies))
	}
}
