package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

func fuseMountCount(t *testing.T, g prometheus.Gatherer, outcome string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "tacklr_fuse_mount_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var got string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "outcome" {
					got = lp.GetValue()
				}
			}
			if got == outcome && m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func drainTurn(t *testing.T, s *EventStream) {
	t.Helper()
	if s == nil {
		return
	}
	for range s.Events {
	}
	s.Close()
}

// vfsRegistryOpts uses DirectProjection when the kernel cannot mount so VFS
// tools still attach. Kernel tests that require FUSE should not use this.
func vfsRegistryOpts(opts ...RegistryOption) []RegistryOption {
	if !vfs.FuseAvailable() {
		return append([]RegistryOption{WithVFSProjection(DirectProjection{})}, opts...)
	}
	return opts
}

func vfsSpec(t *testing.T, model tacklr.InferenceStrategy, point string) AgentSpec {
	t.Helper()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	return AgentSpec{
		Config:      tacklr.Config{MaxWindowSize: 8192},
		Model:       model,
		FSRegistry:  reg,
		FSBootstrap: []vfs.MountSpec{{Point: point, Profile: "local"}},
	}
}

func okModel() *mockInferenceStrategy {
	return &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
}

func TestRunTurn_twoTurnsKeepHostDirOrList(t *testing.T) {
	ctx := context.Background()
	promReg := prometheus.NewRegistry()
	mp, err := telemetry.MeterProviderFromPrometheusRegisterer(promReg, "fuse-two-turn", "v0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	r := NewRegistry(testStore(t), "default", vfsRegistryOpts(WithMeterProvider(mp))...)
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	thread := "sess/two/" + strings.ReplaceAll(t.Name(), "/", "_")
	t.Cleanup(func() { r.DropLiveHarness(thread) })

	s1, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "one"})
	if err != nil {
		t.Fatal(err)
	}
	h1 := s1.harness
	drainTurn(t, s1)

	s2, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "two"})
	if err != nil {
		t.Fatal(err)
	}
	h2 := s2.harness
	if h2 == h1 {
		t.Fatal("want a new harness each turn")
	}
	ms := h2.VFS()
	if ms == nil {
		t.Fatal("want VFS")
	}
	if err := ms.WriteFile(ctx, "/work/note.md", []byte("hello\n")); err != nil {
		t.Fatal(err)
	}

	if vfs.FuseAvailable() {
		dir := ms.HostDir()
		if dir == "" {
			t.Fatal("HostDir empty on turn two")
		}
		ents, err := os.ReadDir(filepath.Join(dir, "work"))
		if err != nil {
			t.Fatal(err)
		}
		sess, err := ms.ReadDir(ctx, "/work")
		if err != nil {
			t.Fatal(err)
		}
		hostNames := map[string]bool{}
		for _, e := range ents {
			hostNames[e.Name()] = e.IsDir()
		}
		for _, e := range sess {
			isDir, ok := hostNames[e.Name]
			if !ok || isDir != e.IsDir {
				t.Fatalf("host ls %s IsDir=%v match=%v host=%v sess=%+v", e.Name, e.IsDir, ok, hostNames, sess)
			}
		}
		got, err := os.ReadFile(filepath.Join(dir, "work", "note.md"))
		if err != nil || string(got) != "hello\n" {
			t.Fatalf("host read: %q err=%v", got, err)
		}
	} else {
		ents, err := ms.ReadDir(ctx, "/work")
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, e := range ents {
			if e.Name == "note.md" {
				found = true
			}
		}
		if !found {
			t.Fatalf("list /work missing note.md: %+v", ents)
		}
	}
	drainTurn(t, s2)

	if n := fuseMountCount(t, promReg, telemetry.FuseMountOutcomeOK); n < 1 {
		t.Fatalf("tacklr_fuse_mount_total{outcome=ok} = %v, want >= 1", n)
	}
}

type unavailableProjection struct{}

func (unavailableProjection) Available() bool                        { return false }
func (unavailableProjection) Attach(*vfs.MountSession, string) error { return nil }

// TestRunTurn_unavailableProjectionStillCompletes: host cannot publish a tree
// → no VFS tools, but the turn still answers the prompt.
func TestRunTurn_unavailableProjectionStillCompletes(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry(testStore(t), "default", WithVFSProjection(unavailableProjection{}))
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	s, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: "sess-noproj", Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.DropLiveHarness("sess-noproj") })
	if s.harness.VFS() != nil {
		t.Fatal("want no VFS when projection is unavailable")
	}
	var saw bool
	for ev := range s.Events {
		if ev.Type == streaming.StreamEventMessage && ev.Content == "ok" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("turn must complete without VFS")
	}
}

func TestRunTurn_multiSegmentBootstrapFailsLoad(t *testing.T) {
	r := NewRegistry(testStore(t), "default", vfsRegistryOpts()...)
	r.Register("default", vfsSpec(t, okModel(), "/tmp/tacklr"))
	thread := "sess-multi-" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "hi"})
	if err == nil || !strings.Contains(err.Error(), "/tmp/tacklr") {
		t.Fatalf("want multi-segment load error, got %v", err)
	}
}

func TestRunTurn_failedResumeThenFreshPrompt(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry(testStore(t), "default", vfsRegistryOpts()...)
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	thread := "sess-warm-fail-" + strings.ReplaceAll(t.Name(), "/", "_")
	t.Cleanup(func() { r.DropLiveHarness(thread) })

	s1, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "one"})
	if err != nil {
		t.Fatal(err)
	}
	h1 := s1.harness
	drainTurn(t, s1)

	_, err = r.RunTurn(ctx, TurnRequest{
		AgentID:   "default",
		ThreadID:  thread,
		Load:      true,
		Responses: map[string]json.RawMessage{"missing": []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("want runHarness resume failure")
	}

	s3, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if s3.harness == h1 {
		t.Fatal("want a new harness after dump")
	}
	if vfs.FuseAvailable() && s3.harness.VFS().HostDir() == "" {
		t.Fatal("HostDir empty on third prompt")
	}
	if !vfs.FuseAvailable() {
		if _, err := s3.harness.VFS().ReadDir(ctx, "/work"); err != nil {
			t.Fatal(err)
		}
	}
	drainTurn(t, s3)
}

func TestRunTurn_coldRunHarnessFailureThenFreshPrompt(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry(testStore(t), "default", vfsRegistryOpts()...)
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	thread := "sess-cold-fail-" + strings.ReplaceAll(t.Name(), "/", "_")
	t.Cleanup(func() { r.DropLiveHarness(thread) })

	_, err := r.RunTurn(ctx, TurnRequest{
		AgentID:   "default",
		ThreadID:  thread,
		Responses: map[string]json.RawMessage{"missing": []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("want cold runHarness failure")
	}

	s3, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if s3.harness == nil {
		t.Fatal("want constructed harness")
	}
	if vfs.FuseAvailable() {
		if s3.harness.VFS() == nil || s3.harness.VFS().HostDir() == "" {
			t.Fatal("want live HostDir on reconstructed harness")
		}
	} else if _, err := s3.harness.VFS().ReadDir(ctx, "/work"); err != nil {
		t.Fatal(err)
	}
	drainTurn(t, s3)
}

func TestRunTurn_fuseMountFailHard(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ctx := context.Background()
	promReg := prometheus.NewRegistry()
	mp, err := telemetry.MeterProviderFromPrometheusRegisterer(promReg, "fuse-fail", "v0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	thread := "sess-fuse-fail"
	if err := os.MkdirAll(filepath.Join(os.TempDir(), "tacklr-fuse"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, suf := range []string{"", "-1"} {
		p := filepath.Join(os.TempDir(), "tacklr-fuse", thread+suf)
		_ = os.RemoveAll(p)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(p) })
	}
	r := NewRegistry(testStore(t), "default", WithMeterProvider(mp))
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	_, err = r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "hi"})
	if err == nil {
		t.Fatal("want FuseMount fail-hard")
	}
	if n := fuseMountCount(t, promReg, telemetry.FuseMountOutcomeError); n < 1 {
		t.Fatalf("tacklr_fuse_mount_total{outcome=error} = %v, want >= 1", n)
	}

	// Unblock and construct a live session.
	for _, suf := range []string{"", "-1"} {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), "tacklr-fuse", thread+suf))
	}
	s, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.DropLiveHarness(thread) })
	if s.harness.VFS().HostDir() == "" {
		t.Fatal("want HostDir after unblocked remount")
	}
	drainTurn(t, s)
}
