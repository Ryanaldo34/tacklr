package brain_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

type nopRerank struct{}

func (nopRerank) Rerank(context.Context, []brain.RichObject) ([]brain.RichObject, error) {
	return nil, nil
}

func TestNewEngine_embedderOrLexicalOnly(t *testing.T) {
	store := brain.NewMemoryStore()
	_, err := brain.NewEngine(store)
	if err == nil || !errors.Is(err, brain.ErrInvalid) || !strings.Contains(err.Error(), "WithEmbedder") || !strings.Contains(err.Error(), "WithLexicalOnly") {
		t.Fatalf("neither option: %v", err)
	}
	_, err = brain.NewEngine(store, brain.WithEmbedder(nopEmbed{}), brain.WithLexicalOnly())
	if err == nil || !errors.Is(err, brain.ErrInvalid) || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("both options: %v", err)
	}
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	got, err := eng.Put(context.Background(), brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Note", Title: "hello", Content: "body",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Embedding) != 0 {
		t.Fatalf("lexical-only put must not embed: %+v", got.Embedding)
	}
}

type nopEmbed struct{}

func (nopEmbed) Embed(context.Context, string) ([]float32, error) { return []float32{1}, nil }

// TestNewEngine_options: WithReranker/ExpandRecipes/Kinds construct path.
func TestNewEngine_options(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	if _, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithExpandRecipes(brain.ExpandRecipe{Name: ""})); err == nil || !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("empty expand recipe name must fail construct: %v", err)
	}
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(),
		brain.WithReranker(nopRerank{}),
		brain.WithExpandRecipes(
			brain.ExpandRecipe{Name: "kids", MaxHops: 1, WantContainment: true},
		),
		brain.WithKinds(brain.KindSpec{
			Kind: "Note",
			Fields: []brain.FieldSpec{
				{Name: "tag", Type: brain.FieldTypeString},
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	// ExpandByRecipe missing name
	if _, err := eng.ExpandByRecipe(ctx, brain.Scope{}, uuid.Nil, "nope", brain.NewSearchContext()); err == nil {
		t.Fatal("missing recipe")
	}
	// Register + expand containment kids
	parent := uuid.New()
	child := uuid.New()
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	pos := 1
	_ = store.Put(ctx, brain.Object{ID: parent, Kind: "Document", Title: "p", Namespace: ns, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: child, Kind: "Chunk", Title: "c", ParentID: &parent, Position: &pos, Namespace: ns, UpdatedAt: now})
	scope := brain.Scope{Namespace: ns}
	res, err := eng.ExpandByRecipe(ctx, scope, parent, "kids", brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Objects) < 1 {
		t.Fatalf("kids expand: %+v", res.Objects)
	}
	if err := eng.RegisterExpandRecipe(brain.ExpandRecipe{Name: ""}); err == nil {
		t.Fatal("empty recipe name")
	}
	if err := eng.RegisterExpandRecipe(brain.ExpandRecipe{Name: " extra ", WantContainment: true}); err != nil {
		t.Fatal(err)
	}
	if eng.HasEdgeSearch() {
		t.Fatal("memory store has no edge search")
	}
	if _, err := eng.FindLinks(ctx, brain.Scope{}, brain.FindLinksRequest{
		RelationType: "about", Query: "x",
	}); !errors.Is(err, brain.ErrUnsupported) {
		t.Fatalf("FindLinks without graph: %v", err)
	}
	if _, err := eng.FindObjects(ctx, brain.Scope{}, brain.FindObjectsRequest{Query: "x"}, brain.NewSearchContext()); !errors.Is(err, brain.ErrUnsupported) {
		t.Fatalf("FindObjects without graph: %v", err)
	}
	if eng.HasObjectSearch() {
		// optional — only when graph object searcher is set
	}
}
