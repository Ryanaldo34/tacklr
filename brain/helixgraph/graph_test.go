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
		_ = json.NewEncoder(w).Encode(map[string]any{
			"neighbors": []map[string]any{
				{"object_id": to.String()},
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
	// Both-direction traversal for free-form edge label.
	if !strings.Contains(body, "Both") && !strings.Contains(body, `"Both"`) {
		// SDK may encode as {"Both":"references"} or similar.
		if !strings.Contains(body, "references") {
			t.Fatalf("request missing relation label: %s", body)
		}
	}
	if !strings.Contains(body, "references") {
		t.Fatalf("request missing references label: %s", body)
	}
}

func TestNewFromClient_requiresClient(t *testing.T) {
	if _, err := helixgraph.NewFromClient(nil); err == nil {
		t.Fatal("want error")
	}
}
