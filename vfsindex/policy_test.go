package vfsindex

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestBridge_policyAndTrack: Start warms prefix members, composes AfterPersist,
// and Track makes a selective path searchable after write.
func TestBridge_policyAndTrack(t *testing.T) {
	ctx := context.Background()
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("auto", vfs.Local(t.TempDir())).Indexed("prefix"),
		vfs.At("off", vfs.Local(t.TempDir())).Indexed("none"),
		vfs.At("odd", vfs.Local(t.TempDir())).Indexed("unknown-policy"),
		vfs.At("memory", vfs.Memory()),
	)(ctx, "br", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	var composed []string
	ms.SetAfterPersist(func(_ context.Context, path string) error {
		composed = append(composed, path)
		return nil
	})
	if err := ms.WriteFile(ctx, "/workspace/auto/seed.txt", []byte("warmup-phrase-xyz\n")); err != nil {
		t.Fatal(err)
	}

	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	br, err := Start(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = br.Close() })

	if _, err := ms.Stat(ctx, MemoryPoint); err != nil {
		t.Fatalf("/workspace/memory: %v", err)
	}
	if br.PolicyAt("/workspace/work/a.txt") != PolicySelective {
		t.Fatalf("default policy: %s", br.PolicyAt("/workspace/work/a.txt"))
	}
	if br.PolicyAt("/workspace/auto/seed.txt") != PolicyPrefix {
		t.Fatalf("prefix: %s", br.PolicyAt("/workspace/auto/seed.txt"))
	}
	if br.PolicyAt("/workspace/off/x.txt") != PolicyNone {
		t.Fatalf("none: %s", br.PolicyAt("/workspace/off/x.txt"))
	}
	if br.PolicyAt("/workspace/odd/x.txt") != PolicySelective {
		t.Fatalf("unknown normalizes selective: %s", br.PolicyAt("/workspace/odd/x.txt"))
	}
	if !br.ShouldIndex("/workspace/auto/seed.txt") {
		t.Fatal("prefix ShouldIndex")
	}
	if br.ShouldIndex("/workspace/off/x.txt") {
		t.Fatal("none ShouldIndex")
	}
	if br.ShouldIndex("/workspace/work/a.txt") {
		t.Fatal("selective without track")
	}
	br.Track("/workspace/work/a.txt")
	if !br.ShouldIndex("/workspace/work/a.txt") {
		t.Fatal("tracked path")
	}
	if err := ms.WriteFile(ctx, "/workspace/memory/strict.txt", []byte("strict-memory-phrase\n")); err != nil {
		t.Fatal(err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "strict-memory-phrase"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("strict memory index: page=%+v err=%v", page, err)
	}

	if err := ms.WriteFile(ctx, "/workspace/auto/live.txt", []byte("live-auto-phrase\n")); err != nil {
		t.Fatal(err)
	}
	if len(composed) == 0 {
		t.Fatal("expected composed AfterPersist")
	}
	waitIndexed(t, eng, scope, "live-auto-phrase")
	waitIndexed(t, eng, scope, "warmup-phrase-xyz")

	if err := ms.WriteFile(ctx, "/workspace/off/secret.txt", []byte("off-secret-token\n")); err != nil {
		t.Fatal(err)
	}
	res, err := br.Indexer.IndexPathResult(ctx, "/workspace/off/secret.txt")
	if err != nil || res != PathSkipped {
		t.Fatalf("none IndexPath: res=%q err=%v", res, err)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/a.txt", []byte("tracked-selective-phrase\n")); err != nil {
		t.Fatal(err)
	}
	waitIndexed(t, eng, scope, "tracked-selective-phrase")

	br.Untrack("/workspace/work/a.txt")
	if br.ShouldIndex("/workspace/work/a.txt") {
		t.Fatal("after untrack")
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitIndexed(t *testing.T, eng *brain.Engine, scope brain.Scope, query string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: query}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Objects) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("search %q: no hit", query)
}
