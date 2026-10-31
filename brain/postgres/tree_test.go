package postgres_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestStore_aTreeCommitThatCannotFinishLeavesThePreviousTree(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, pool := testDB(t)
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "v1"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "kept"}))
	if err != nil {
		t.Fatal(err)
	}
	// Another transaction holds the document's row until the commit gives up.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT 1 FROM objects WHERE id = $1 FOR UPDATE`, doc.ID); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	_, blockedErr := eng.PutTree(short, scope, doc, brain.Parts(brain.Object{Kind: "Chunk", Content: "replacement"}))
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	_, unencodableErr := eng.PutTree(ctx, scope, doc, brain.Parts(brain.Object{Kind: "Chunk", Content: "x", Properties: map[string]any{"bad": make(chan int)}}))
	kidsTree, _ := store.GetTree(ctx, scope, doc.ID, 0)
	kids := kidsTree.Parts

	if !errors.Is(blockedErr, context.DeadlineExceeded) {
		t.Errorf("commit blocked past its deadline = %v, want DeadlineExceeded", blockedErr)
	}
	if unencodableErr == nil {
		t.Error("a part whose properties JSON cannot hold was written")
	}
	if got := contents(kids); !slices.Equal(got, []string{"kept"}) {
		t.Errorf("parts after the failed writes = %v, want the previous tree", got)
	}
}

func contents(objs []brain.Object) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Content
	}
	return out
}
