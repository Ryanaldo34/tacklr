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

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
)

func TestGraph_neighborsRequestAST(t *testing.T) {
	from := uuid.New()
	to := uuid.New()
	fromPeer := uuid.New()
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/v1/query") {
			t.Fatalf("path %s", r.URL.Path)
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(b))
		// OutE then InE: return one peer each (with edge meta on outbound).
		props := []map[string]any{}
		if strings.Contains(string(b), "OutE") {
			conf := 0.75
			props = append(props, map[string]any{
				"object_id":   to.String(),
				"note":        "cites memo",
				"status":      "active",
				"role":        "primary",
				"confidence":  conf,
				"evidence_id": from.String(),
			})
		} else {
			props = append(props, map[string]any{"object_id": fromPeer.String(), "evidence_id": "not-a-uuid"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"neighbors": map[string]any{"properties": props},
		})
	}))
	t.Cleanup(server.Close)

	g, err := helixgraph.New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := g.Neighbors(context.Background(), from, []string{"references"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 2 {
		t.Fatalf("want out+in: %+v", ns)
	}
	if ns[0].ObjectID != to || ns[0].Direction != "out" || ns[0].Meta.Note != "cites memo" {
		t.Fatalf("out hop: %+v", ns[0])
	}
	if ns[0].Meta.Role != "primary" || ns[0].Meta.Confidence != 0.75 {
		t.Fatalf("out meta: %+v", ns[0].Meta)
	}
	if ns[0].Meta.EvidenceID == nil || *ns[0].Meta.EvidenceID != from {
		t.Fatalf("evidence: %+v", ns[0].Meta.EvidenceID)
	}
	if ns[1].ObjectID != fromPeer || ns[1].Direction != "in" {
		t.Fatalf("in hop: %+v", ns[1])
	}
	if ns[1].Meta.EvidenceID != nil {
		t.Fatalf("invalid evidence_id must be skipped: %+v", ns[1].Meta)
	}
	if len(bodies) != 2 {
		t.Fatalf("want OutE + InE RPCs: %d", len(bodies))
	}
	joined := strings.Join(bodies, "\n")
	for _, want := range []string{from.String(), "references", "OutE", "InE", "object_id", "note"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("requests missing %q:\n%s", want, joined)
		}
	}
}

func TestNewFromClient_requiresClient(t *testing.T) {
	if _, err := helixgraph.NewFromClient(nil); err == nil {
		t.Fatal("want error")
	}
}

// TestGraph_neighborsCancelsBetweenRPCs: cancel after OutE aborts before InE.
func TestGraph_neighborsCancelsBetweenRPCs(t *testing.T) {
	from, to := uuid.New(), uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			cancel() // after first direction returns, next dir must see ctx.Canceled
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"neighbors": map[string]any{
				"properties": []map[string]any{{"object_id": to.String()}},
			},
		})
	}))
	t.Cleanup(srv.Close)
	g, err := helixgraph.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Neighbors(ctx, from, []string{"references"}, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled mid-walk: %v calls=%d", err, calls)
	}
	if calls != 1 {
		t.Fatalf("want single OutE RPC before cancel: %d", calls)
	}
}

func TestGraph_validationAndClient(t *testing.T) {
	ctx := context.Background()
	g, err := helixgraph.New("http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if g.Client() == nil {
		t.Fatal("Client()")
	}
	if err := g.EnsureObject(ctx, brain.Object{}); err == nil {
		t.Fatal("nil object id")
	}
	if err := g.AddEdge(ctx, uuid.Nil, uuid.New(), "r", brain.EdgeMeta{}); err == nil {
		t.Fatal("nil from")
	}
	if err := g.AddEdge(ctx, uuid.New(), uuid.New(), "  ", brain.EdgeMeta{}); err == nil {
		t.Fatal("empty rel")
	}
	// Empty relation list → empty neighbors (no RPC).
	ns, err := g.Neighbors(ctx, uuid.New(), nil, 10)
	if err != nil || len(ns) != 0 {
		t.Fatalf("%+v %v", ns, err)
	}
	ns, err = g.Neighbors(ctx, uuid.Nil, []string{"r"}, 10)
	if err != nil || len(ns) != 0 {
		t.Fatalf("nil id: %+v %v", ns, err)
	}
	// Malformed neighbor payload.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"neighbors":123}`))
	}))
	t.Cleanup(srv.Close)
	g2, err := helixgraph.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.Neighbors(ctx, uuid.New(), []string{"r"}, 5); err == nil {
		t.Fatal("want decode error")
	}
	// Invalid object_id strings are skipped; OutE+InE share one valid peer id (deduped).
	validPeer := uuid.New()
	srv3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"neighbors": map[string]any{
				"properties": []map[string]any{
					{"object_id": ""},
					{"object_id": "not-a-uuid"},
					{"object_id": validPeer.String()},
				},
			},
		})
	}))
	t.Cleanup(srv3.Close)
	g3, err := helixgraph.New(srv3.URL)
	if err != nil {
		t.Fatal(err)
	}
	from := uuid.New()
	ns, err = g3.Neighbors(ctx, from, []string{"r", "r", "  "}, 0) // limit<=0 → default
	if err != nil || len(ns) != 1 || ns[0].ObjectID != validPeer {
		t.Fatalf("skip bad ids: %+v err=%v", ns, err)
	}
}
