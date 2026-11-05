package brain_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// tenant is a namespace no other test uses.
func tenant(t testing.TB) brain.Namespace { return storetest.Namespace(t, "org", uuid.NewString()) }

// save saves obj through eng and returns it.
func save(t testing.TB, eng *brain.Engine, scope brain.Scope, obj brain.Object) brain.Object {
	t.Helper()
	saved, err := eng.Save(context.Background(), scope, obj)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

// link links from→to through eng.
func link(t testing.TB, eng *brain.Engine, scope brain.Scope, from, to uuid.UUID, rel, note string) {
	t.Helper()
	if err := eng.Link(context.Background(), scope, brain.LinkRequest{From: from, To: to, Relation: rel, Meta: brain.EdgeMeta{Note: note}}); err != nil {
		t.Fatal(err)
	}
}

func titlesOf(objs []brain.RichObject) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Title
	}
	return out
}

func idsOf(objs []brain.RichObject) []uuid.UUID {
	out := make([]uuid.UUID, len(objs))
	for i, o := range objs {
		out[i] = o.ID
	}
	return out
}

// embedFunc is the host's embedding provider, the lowest-level client of a
// store. Tacklr ships no embedder, so a test gives fn the provider's answer.
type embedFunc func(ctx context.Context, texts []string) ([][]float32, error)

func (f embedFunc) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return f(ctx, texts)
}

func (embedFunc) Model() string { return "test-model" }

// repeatVector returns n copies of v, one per embedded text.
func repeatVector(v []float32, n int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = v
	}
	return out
}
