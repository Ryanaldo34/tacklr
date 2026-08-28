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
	ns := MustNamespace("id", uuid.NewString())
	now := time.Now().UTC()
	parent := uuid.New()
	_ = s.Put(context.Background(), Object{ID: parent, Kind: "Document", Title: "Doc", Namespace: ns, UpdatedAt: now})
	p1, p2 := uuid.New(), uuid.New()
	pos1, pos2 := 1, 2
	_ = s.Put(context.Background(), Object{
		ID: p1, Kind: "Chunk", Title: "alpha", Content: "oauth pkce flow",
		ParentID: &parent, Position: &pos1, Namespace: ns, UpdatedAt: now,
		Embedding: []float32{1, 0, 0},
	})
	_ = s.Put(context.Background(), Object{
		ID: p2, Kind: "Chunk", Title: "beta", Content: "pasta recipes",
		ParentID: &parent, Position: &pos2, Namespace: ns, UpdatedAt: now,
		Embedding: []float32{0, 1, 0},
	})
	_ = s.PutKind(context.Background(), ObjectKind{Kind: "Chunk", IsPart: true})
	_ = s.PutKind(context.Background(), ObjectKind{Kind: "Document", IsParent: true})

	lex, err := s.SearchLexical(ctx, Scope{Namespace: ns}, "oauth", Filter{}, 5)
	if err != nil || len(lex) == 0 || lex[0].ID != p1 {
		t.Fatalf("lex: %+v %v", lex, err)
	}
	vec, err := s.SearchVector(ctx, Scope{Namespace: ns}, []float32{0.9, 0.1, 0}, Filter{}, 5)
	if err != nil || len(vec) == 0 || vec[0].ID != p1 {
		t.Fatalf("vec: %+v %v", vec, err)
	}
	tri, err := s.SearchTrigram(ctx, Scope{Namespace: ns}, "oauth pkce", Filter{}, 5)
	if err != nil || len(tri) == 0 {
		t.Fatalf("tri: %+v %v", tri, err)
	}
	// Empty channels
	if got, _ := s.SearchVector(ctx, Scope{}, nil, Filter{}, 5); got != nil {
		t.Fatal(got)
	}
	if got, _ := s.SearchTrigram(ctx, Scope{}, "", Filter{}, 5); got != nil {
		t.Fatal(got)
	}
	if got, _ := s.SearchLexical(ctx, Scope{}, "x", Filter{}, 0); got != nil {
		t.Fatal(got)
	}

	kids, err := s.ListChildren(ctx, Scope{Namespace: ns}, parent)
	if err != nil || len(kids) != 2 || kids[0].ID != p1 {
		t.Fatalf("kids: %+v %v", kids, err)
	}
	// Position nil sorts as 0 without panicking.
	p0 := uuid.New()
	_ = s.Put(context.Background(), Object{
		ID: p0, Kind: "Chunk", Title: "nil-pos", Content: "z",
		ParentID: &parent, Namespace: ns, UpdatedAt: now,
	})
	kids, err = s.ListChildren(ctx, Scope{Namespace: ns}, parent)
	if err != nil || len(kids) != 3 {
		t.Fatalf("kids with nil pos: %+v %v", kids, err)
	}
	kinds, err := s.ListKinds(ctx)
	if err != nil || len(kinds) != 2 {
		t.Fatalf("kinds: %+v", kinds)
	}

	// Invalid filters fail closed from search channels (compile once).
	if _, err := s.SearchLexical(ctx, Scope{Namespace: ns}, "oauth", Filter{UpdatedAfter: "nope"}, 5); err == nil {
		t.Fatal("want filter compile error")
	}
	if _, err := s.SearchVector(ctx, Scope{Namespace: ns}, []float32{1, 0, 0}, Filter{Props: map[string]PropFilter{"": {Eq: "x"}}}, 5); err == nil {
		t.Fatal("want empty key error")
	}
	if _, err := s.SearchTrigram(ctx, Scope{Namespace: ns}, "oauth", Filter{Props: map[string]PropFilter{"stage": {In: []any{}}}}, 5); err == nil {
		t.Fatal("want empty list error")
	}
}
