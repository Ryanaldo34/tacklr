package helixgraph_test

import (
	"context"
	"encoding/json"
	"errors"
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
	var nodeExists bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		body := string(b)
		bodies = append(bodies, body)
		// Exists probe: Count without AddN/SetProperty/AddE.
		if strings.Contains(body, "brain_object_exists") ||
			(strings.Contains(body, "Count") && !strings.Contains(body, "AddN") &&
				!strings.Contains(body, "SetProperty") && !strings.Contains(body, "AddE") &&
				!strings.Contains(body, "DropEdge") && !strings.Contains(body, "CreateIndex")) {
			n := int64(0)
			if nodeExists {
				n = 1
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"n": n})
			return
		}
		if strings.Contains(body, "AddN") {
			nodeExists = true
		}
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
	// First EnsureObject: exists-check then insert (AddN, no node Drop).
	// parent_id is not dual-written (parts stay in Postgres only).
	if len(bodies) < 2 {
		t.Fatalf("want exists+insert RPCs, got %d", len(bodies))
	}
	joinedEnsure := strings.Join(bodies, "\n")
	for _, want := range []string{
		obj.ID.String(), "Document", "memo", "sum", "body text",
		ns.String(), "search_text", "embedding",
		"created_at", "updated_at", "AddN",
	} {
		if !strings.Contains(joinedEnsure, want) {
			t.Fatalf("ensure body missing %q:\n%s", want, joinedEnsure)
		}
	}
	if strings.Contains(joinedEnsure, pid.String()) {
		t.Fatalf("parent_id must not be dual-written to Helix:\n%s", joinedEnsure)
	}
	insertBody := bodies[len(bodies)-1]
	if strings.Contains(insertBody, "AddN") && strings.Contains(insertBody, `"Drop"`) {
		t.Fatalf("insert must not Drop node:\n%s", insertBody)
	}

	// Second EnsureObject updates in place (SetProperty), no AddN / node Drop.
	nBeforeUpdate := len(bodies)
	obj.Title = "memo-v2"
	if err := g.EnsureObject(ctx, obj); err != nil {
		t.Fatal(err)
	}
	updateBodies := bodies[nBeforeUpdate:]
	updateJoined := strings.Join(updateBodies, "\n")
	if !strings.Contains(updateJoined, "memo-v2") {
		t.Fatalf("update path missing memo-v2:\n%s", updateJoined)
	}
	if !strings.Contains(updateJoined, "SetProperty") {
		t.Fatalf("update path missing SetProperty:\n%s", updateJoined)
	}
	for _, b := range updateBodies {
		if strings.Contains(b, "AddN") {
			t.Fatalf("re-ensure must not AddN:\n%s", b)
		}
	}

	// Index ensure RPC.
	nBeforeIdx := len(bodies)
	if err := g.EnsureObjectIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != nBeforeIdx+1 || !strings.Contains(bodies[len(bodies)-1], "object_id") {
		t.Fatalf("index body: %v", bodies[nBeforeIdx:])
	}

	// Edge RPC with relationship metadata.
	from, to := uuid.New(), uuid.New()
	evid := uuid.New()
	meta := brain.EdgeMeta{
		Note: "pricing risk", Status: "active", Role: "primary",
		Confidence: 0.9, EvidenceID: &evid,
	}
	nBeforeEdge := len(bodies)
	if err := g.AddEdge(ctx, from, to, "references", meta); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != nBeforeEdge+1 {
		t.Fatalf("bodies after edge: %d", len(bodies))
	}
	edge := bodies[len(bodies)-1]
	for _, want := range []string{
		from.String(), to.String(), "references", "AddE", "DropEdgeLabeled",
		"pricing risk", "active", "primary", "0.9", evid.String(),
	} {
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
	if err := g.AddEdge(ctx, uuid.New(), uuid.Nil, "r", brain.EdgeMeta{}); err == nil {
		t.Fatal("nil to")
	}
	if err := g.RemoveObject(ctx, uuid.Nil); err == nil {
		t.Fatal("nil remove id")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := g.AddEdge(cctx, uuid.New(), uuid.New(), "r", brain.EdgeMeta{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled AddEdge: %v", err)
	}
	if err := g.RemoveObject(cctx, uuid.New()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled RemoveObject: %v", err)
	}
	if err := g.EnsureObject(cctx, brain.Object{ID: uuid.New()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled EnsureObject: %v", err)
	}
	if err := g.Bootstrap(cctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Bootstrap: %v", err)
	}
	if g.ObjectSearchReady() {
		t.Fatal("Bootstrap cancel must leave search not ready")
	}
}

// TestGraph_removeObjectAndBootstrapRequestShape covers RemoveObject + Bootstrap under -short.
func TestGraph_removeObjectAndBootstrapRequestShape(t *testing.T) {
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
	if g.ObjectSearchReady() {
		t.Fatal("ready before bootstrap")
	}
	if err := g.Bootstrap(ctx, false); err != nil {
		t.Fatal(err)
	}
	if !g.ObjectSearchReady() {
		t.Fatal("Bootstrap should mark ready")
	}
	// Bootstrap issues object_id index + text/vector indexes (via EnsureSearchIndexes).
	if len(bodies) < 2 {
		t.Fatalf("bootstrap RPCs: %d", len(bodies))
	}
	joined := strings.Join(bodies, "\n")
	for _, want := range []string{"object_id", "search_text", "embedding", "CreateIndex", "if_not_exists"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("bootstrap missing %q:\n%s", want, joined)
		}
	}

	id := uuid.New()
	nBefore := len(bodies)
	if err := g.RemoveObject(ctx, id); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != nBefore+1 {
		t.Fatalf("remove bodies: %d", len(bodies))
	}
	rm := bodies[len(bodies)-1]
	if !strings.Contains(rm, id.String()) || !strings.Contains(rm, "Drop") {
		t.Fatalf("remove body: %s", rm)
	}

	// Bootstrap failure path must not mark ready.
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(fail.Close)
	gFail, err := helixgraph.New(fail.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := gFail.Bootstrap(ctx, true); err == nil {
		t.Fatal("want bootstrap error")
	}
	if gFail.ObjectSearchReady() {
		t.Fatal("failed Bootstrap must not mark ready")
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
	if !g.TenantEnabled() {
		t.Fatal("tenant indexes should be enabled")
	}
	// With tenant enabled, SearchText must pass namespace into Helix.
	ns := uuid.New()
	nBefore := len(bodies)
	if _, err := g.SearchText(ctx, "risk", 3, &ns); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != nBefore+1 || !strings.Contains(bodies[len(bodies)-1], ns.String()) {
		t.Fatalf("tenant search body: %v", bodies[nBefore:])
	}
	if err := g.EnsureEdgeTextIndex(ctx, "about"); err != nil {
		t.Fatal(err)
	}
	edgeIdx := bodies[len(bodies)-1]
	if !strings.Contains(edgeIdx, "about") || !strings.Contains(edgeIdx, "note") {
		t.Fatalf("edge text index: %s", edgeIdx)
	}
}
