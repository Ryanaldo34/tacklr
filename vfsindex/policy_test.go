package vfsindex

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestBridge_policyAndTrack: Start attaches /memory, warms prefix mounts,
// composes AfterPersist, and Track makes a selective path searchable after write.
func TestBridge_policyAndTrack(t *testing.T) {
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("br", reg)
	var composed []string
	ms.SetAfterPersist(func(_ context.Context, path string) error {
		composed = append(composed, path)
		return nil
	})
	for _, spec := range []vfs.MountSpec{
		{Point: "/work", Profile: "scratch"},
		{Point: "/auto", Profile: "scratch", IndexPolicy: "  PREFIX  ", Params: map[string]string{"subpath": "auto"}},
		{Point: "/off", Profile: "scratch", IndexPolicy: "none", Params: map[string]string{"subpath": "off"}},
		{Point: "/odd", Profile: "scratch", IndexPolicy: "unknown-policy", Params: map[string]string{"subpath": "odd"}},
	} {
		if err := ms.Mount(ctx, spec); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.WriteFile(ctx, "/auto/seed.txt", []byte("warmup-phrase-xyz\n")); err != nil {
		t.Fatal(err)
	}

	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	br, err := Start(ms, eng, scope, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = br.Close() })

	if _, err := ms.Stat(ctx, MemoryPoint); err != nil {
		t.Fatalf("/memory: %v", err)
	}
	// Second attach is a no-op when /memory is already mounted.
	attachMemoryMount(ms)
	if br.PolicyAt("/work/a.txt") != PolicySelective {
		t.Fatalf("default policy: %s", br.PolicyAt("/work/a.txt"))
	}
	if br.PolicyAt("/auto/seed.txt") != PolicyPrefix {
		t.Fatalf("prefix: %s", br.PolicyAt("/auto/seed.txt"))
	}
	if br.PolicyAt("/off/x.txt") != PolicyNone {
		t.Fatalf("none: %s", br.PolicyAt("/off/x.txt"))
	}
	if br.PolicyAt("/odd/x.txt") != PolicySelective {
		t.Fatalf("unknown normalizes selective: %s", br.PolicyAt("/odd/x.txt"))
	}
	if !br.ShouldIndex("/auto/seed.txt") {
		t.Fatal("prefix ShouldIndex")
	}
	if br.ShouldIndex("/off/x.txt") {
		t.Fatal("none ShouldIndex")
	}
	if br.ShouldIndex("/work/a.txt") {
		t.Fatal("selective without track")
	}
	br.Track("/work/a.txt")
	if !br.ShouldIndex("/work/a.txt") {
		t.Fatal("tracked path")
	}

	if err := ms.WriteFile(ctx, "/auto/live.txt", []byte("live-auto-phrase\n")); err != nil {
		t.Fatal(err)
	}
	if len(composed) == 0 {
		t.Fatal("expected composed AfterPersist")
	}
	waitIndexed(t, eng, scope, "live-auto-phrase")
	waitIndexed(t, eng, scope, "warmup-phrase-xyz")

	if err := ms.WriteFile(ctx, "/off/secret.txt", []byte("off-secret-token\n")); err != nil {
		t.Fatal(err)
	}
	res, err := br.Indexer.IndexPathResult(ctx, "/off/secret.txt")
	if err != nil || res != PathSkipped {
		t.Fatalf("none IndexPath: res=%q err=%v", res, err)
	}

	if err := ms.WriteFile(ctx, "/work/a.txt", []byte("tracked-selective-phrase\n")); err != nil {
		t.Fatal(err)
	}
	waitIndexed(t, eng, scope, "tracked-selective-phrase")

	br.Untrack("/work/a.txt")
	if br.ShouldIndex("/work/a.txt") {
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
