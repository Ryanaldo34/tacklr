package brain

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMemoryStore_searchChannelsAndClone(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	_ = s.Put(context.Background(), Object{ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now})
	p1, p2 := uuid.New(), uuid.New()
	pos1, pos2 := 1, 2
	_ = s.Put(context.Background(), Object{
		ID: p1, Kind: "Chunk", Title: "alpha", Content: "oauth pkce flow",
		ParentID: &parent, Position: &pos1, NamespaceID: ns, UpdatedAt: now,
		Embedding: []float32{1, 0, 0},
	})
	_ = s.Put(context.Background(), Object{
		ID: p2, Kind: "Chunk", Title: "beta", Content: "pasta recipes",
		ParentID: &parent, Position: &pos2, NamespaceID: ns, UpdatedAt: now,
		Embedding: []float32{0, 1, 0},
	})
	_ = s.PutKind(context.Background(), ObjectKind{Kind: "Chunk", IsPart: true})
	_ = s.PutKind(context.Background(), ObjectKind{Kind: "Document", IsParent: true})

	lex, err := s.SearchLexical(ctx, Scope{Namespace: &ns}, "oauth", nil, 5)
	if err != nil || len(lex) == 0 || lex[0].ID != p1 {
		t.Fatalf("lex: %+v %v", lex, err)
	}
	vec, err := s.SearchVector(ctx, Scope{Namespace: &ns}, []float32{0.9, 0.1, 0}, nil, 5)
	if err != nil || len(vec) == 0 || vec[0].ID != p1 {
		t.Fatalf("vec: %+v %v", vec, err)
	}
	tri, err := s.SearchTrigram(ctx, Scope{Namespace: &ns}, "oauth pkce", nil, 5)
	if err != nil || len(tri) == 0 {
		t.Fatalf("tri: %+v %v", tri, err)
	}
	// Empty channels
	if got, _ := s.SearchVector(ctx, Scope{}, nil, nil, 5); got != nil {
		t.Fatal(got)
	}
	if got, _ := s.SearchTrigram(ctx, Scope{}, "", nil, 5); got != nil {
		t.Fatal(got)
	}
	if got, _ := s.SearchLexical(ctx, Scope{}, "x", nil, 0); got != nil {
		t.Fatal(got)
	}

	kids, err := s.ListChildren(ctx, Scope{Namespace: &ns}, parent)
	if err != nil || len(kids) != 2 || kids[0].ID != p1 {
		t.Fatalf("kids: %+v %v", kids, err)
	}
	kinds, err := s.ListKinds(ctx)
	if err != nil || len(kinds) != 2 {
		t.Fatalf("kinds: %+v", kinds)
	}
}
