package agentbench

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIEmbedder_requestAndDecode(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Fatalf("path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("auth: %s", r.Header.Get("Authorization"))
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"embedding": []float64{0.1, 0.2, 0.3}, "index": 0},
			},
		})
	}))
	t.Cleanup(srv.Close)

	e := &OpenAIEmbedder{
		BaseURL:    srv.URL + "/v1",
		APIKey:     "test-key",
		Model:      "text-embedding-3-small",
		HTTPClient: srv.Client(),
	}
	vec, err := e.Embed(context.Background(), "hello hybrid")
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) != 3 || vec[0] != 0.1 {
		t.Fatalf("vec: %v", vec)
	}
	if gotBody["model"] != "text-embedding-3-small" {
		t.Fatalf("body model: %v", gotBody)
	}
	if gotBody["input"] != "hello hybrid" {
		t.Fatalf("body input: %v", gotBody)
	}
}

func TestOpenAIEmbedder_emptyText(t *testing.T) {
	e := &OpenAIEmbedder{BaseURL: "http://example.invalid", Model: "m"}
	vec, err := e.Embed(context.Background(), "  ")
	if err != nil || vec != nil {
		t.Fatalf("empty: %v %v", vec, err)
	}
}

func TestOpenAIEmbedder_httpError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad model"}`))
	}))
	t.Cleanup(srv.Close)
	e := &OpenAIEmbedder{BaseURL: srv.URL, Model: "x", HTTPClient: srv.Client()}
	_, err := e.Embed(context.Background(), "hi")
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("want HTTP error: %v", err)
	}
}

func TestConfig_embedModelLabel(t *testing.T) {
	c := Config{EmbedModel: "nomic", LexicalOnly: true}.withDefaults()
	if c.embedModelLabel() != "lexical_only" {
		t.Fatal(c.embedModelLabel())
	}
	c2 := Config{EmbedModel: "nomic"}.withDefaults()
	if c2.embedModelLabel() != "nomic" {
		t.Fatal(c2.embedModelLabel())
	}
}
