package vfsindex

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestBridge_policyAndTrack: policy strings normalize, a none mount skips
// IndexPath, and Track is only a selective-path record. Writes are not indexed.
func TestBridge_policyAndTrack(t *testing.T) {
	ctx := context.Background()
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(t.TempDir())),
		vfs.At("auto", vfs.Local(t.TempDir())).Indexed("prefix"),
		vfs.At("off", vfs.Local(t.TempDir())).Indexed("none"),
		vfs.At("odd", vfs.Local(t.TempDir())).Indexed("unknown-policy"),
		vfs.At("scratch", vfs.Local(t.TempDir())).Indexed("watch"),
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

	if err := ms.Route(ctx, "/workspace/auto/seed.txt").
		WriteFile(ctx, []byte("warmup-phrase-xyz\n")); err != nil {
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

	if br.PolicyAt("/workspace/scratch/a.txt") != PolicyWatch {
		t.Fatalf("watch: %s", br.PolicyAt("/workspace/scratch/a.txt"))
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

	if err := ms.Route(ctx, "/workspace/auto/live.txt").
		WriteFile(ctx, []byte("live-auto-phrase\n")); err != nil {
		t.Fatal(err)
	}
	if len(composed) == 0 {
		t.Fatal("host AfterPersist did not run")
	}

	if err := ms.Route(ctx, "/workspace/off/secret.txt").
		WriteFile(ctx, []byte("off-secret-token\n")); err != nil {
		t.Fatal(err)
	}

	res, err := br.Indexer.IndexPathResult(ctx, "/workspace/off/secret.txt")
	if err != nil || res != PathSkipped {
		t.Fatalf("none IndexPath: res=%q err=%v", res, err)
	}

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

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func TestAsyncScheduler_reportsQueueAndClosedOutcomes(t *testing.T) {
	// Arrange
	var events []SchedulerEvent
	scheduler := &AsyncScheduler{
		QueueCap: 1,
		pending:  map[string]IndexReason{"/queued": ReasonSync},
		wake:     make(chan struct{}, 1),
	}
	scheduler.SetObserver(func(event SchedulerEvent) {
		events = append(events, event)
	})

	// Act
	queueErr := scheduler.Notify(t.Context(), "/dropped", ReasonSync)
	scheduler.mu.Lock()
	scheduler.closed = true
	scheduler.mu.Unlock()
	closedErr := scheduler.Notify(t.Context(), "/closed", ReasonExplicit)

	// Assert
	if !errors.Is(queueErr, ErrQueueFull) || !errors.Is(closedErr, ErrSchedulerClosed) {
		t.Fatalf("queue error = %v closed error = %v", queueErr, closedErr)
	}
	if len(events) != 2 || !errors.Is(events[0].Err, ErrQueueFull) || !errors.Is(events[1].Err, ErrSchedulerClosed) {
		t.Fatalf("events = %#v", events)
	}
}
