package brain_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

// tenant is a namespace no other test uses.
func tenant(t testing.TB) brain.Namespace { return mustNS(t, "org", uuid.NewString()) }

func mustFilter(t testing.TB, m map[string]any) brain.Filter {
	t.Helper()
	f, err := brain.DecodeFilter(m)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// putFixture writes obj straight to the store under its own namespace.
func putFixture(s brain.Store, obj brain.Object) error {
	_, err := s.Put(context.Background(), brain.Scope{Namespace: obj.Namespace}, brain.Indexed{Object: obj, Text: obj.IndexText()})
	return err
}

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

type errString string

func (e errString) Error() string { return string(e) }

// stubEmbedder embeds every text as v.
type stubEmbedder struct{ v []float32 }

func (s stubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = s.v
	}
	return out, nil
}

func (stubEmbedder) Model() string { return "stub-model" }

// failEmbedder fails every embed.
type failEmbedder struct{}

func (failEmbedder) Embed(context.Context, []string) ([][]float32, error) {
	return nil, errString("embed down")
}

func (failEmbedder) Model() string { return "fail-model" }
