package postgres

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestSessionMessageGuards(t *testing.T) {
	s := &Store{}
	ctx := context.Background()
	scope := brain.Scope{Namespace: brain.Namespace{{Name: "org", Value: "acme"}}, SessionID: "s"}
	if rows, err := s.sessionLexical(ctx, scope, "q", 0); err != nil || rows != nil {
		t.Fatalf("lexical guard %v %v", rows, err)
	}
	if rows, err := s.sessionVector(ctx, scope, []float32{1}, 0); err != nil || rows != nil {
		t.Fatalf("vector guard %v %v", rows, err)
	}
	if rows, err := s.sessionTrigram(ctx, scope, "q", 0); err != nil || rows != nil {
		t.Fatalf("trigram guard %v %v", rows, err)
	}
	if _, err := s.readSessionMessage(ctx, "s", uuid.Nil); err == nil {
		t.Fatal("nil id")
	}
	if rows, err := s.readSessionMessages(ctx, "", []uuid.UUID{uuid.New()}); err != nil || rows != nil {
		t.Fatalf("empty session %v %v", rows, err)
	}
	if rows, err := s.readSessionMessages(ctx, "s", nil); err != nil || rows != nil {
		t.Fatalf("empty ids %v %v", rows, err)
	}
	hits, err := mergeRanked(nil, []brain.ScoredID{{Score: 1}}, nil)
	if err != nil || len(hits) != 1 {
		t.Fatalf("merge = %+v %v", hits, err)
	}
}
