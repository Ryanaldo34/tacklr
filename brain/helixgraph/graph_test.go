package helixgraph_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain/helixgraph"
)

func TestGraph_neighborsRequestAST(t *testing.T) {
	from := uuid.New()
	to := uuid.New()
	var gotBody []byte
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
		gotBody = b
		// Real Helix ValueMap shape.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"neighbors": map[string]any{
				"properties": []map[string]any{
					{"object_id": to.String()},
				},
			},
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
	if len(ns) != 1 || ns[0].ObjectID != to || ns[0].RelationType != "references" {
		t.Fatalf("%+v", ns)
	}

	body := string(gotBody)
	if !strings.Contains(body, "object_id") {
		t.Fatalf("request missing object_id: %s", body)
	}
	if !strings.Contains(body, from.String()) {
		t.Fatalf("request missing source id: %s", body)
	}
	if !strings.Contains(body, "references") {
		t.Fatalf("request missing references label: %s", body)
	}
	if !strings.Contains(body, "Both") {
		t.Fatalf("request missing Both traversal: %s", body)
	}
}

func TestNewFromClient_requiresClient(t *testing.T) {
	if _, err := helixgraph.NewFromClient(nil); err == nil {
		t.Fatal("want error")
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
	if err := g.PutObject(ctx, uuid.Nil); err == nil {
		t.Fatal("nil object id")
	}
	if err := g.AddEdge(ctx, uuid.Nil, uuid.New(), "r"); err == nil {
		t.Fatal("nil from")
	}
	if err := g.AddEdge(ctx, uuid.New(), uuid.New(), "  "); err == nil {
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
	// Invalid object_id strings are skipped.
	srv3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"neighbors": map[string]any{
				"properties": []map[string]any{
					{"object_id": ""},
					{"object_id": "not-a-uuid"},
					{"object_id": uuid.New().String()},
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
	if err != nil || len(ns) != 1 {
		t.Fatalf("skip bad ids: %+v err=%v", ns, err)
	}
}
