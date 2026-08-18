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
	"github.com/ryanaldo34/tacklr/brain"
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
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192},
			Model:  model,
		},
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

func TestRegistry_usesCanonicalAgentOptions(t *testing.T) {
	// Arrange
	engine, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	sawBrainTools := false
	model := &mockInferenceStrategy{
		invokeFn: func(_ context.Context, _ []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			sawBrainTools = len(tools) > 5
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	registry := NewRegistry(testStore(t), "default")
	registry.Register("default", AgentSpec{
		Options: tacklr.AgentOptions{
			Model:         model,
			Brain:         engine,
			ContextPolicy: tacklr.ContextPolicy{PressureRatio: 0.5, CompressFraction: 0.2},
		},
	})

	// Act
	stream, err := registry.RunTurn(t.Context(), TurnRequest{
		AgentID:  "default",
		ThreadID: "canonical-options",
		Prompt:   "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Events {
	}
	t.Cleanup(stream.Close)

	// Assert
	if !sawBrainTools {
		t.Fatal("registry did not preserve canonical brain options")
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
	if s1.SessionID() != thread {
		t.Fatalf("SessionID = %q, want %q", s1.SessionID(), thread)
	}
	if s1.VFS() == nil {
		t.Fatal("want VFS on first turn")
	}
	h1 := s1.harness
	drainTurn(t, s1)
	if s1.SessionID() != "" || s1.VFS() != nil {
		t.Fatal("Close must release SessionID and VFS")
	}

	s2, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "two"})
	if err != nil {
		t.Fatal(err)
	}
	h2 := s2.harness
	if h2 == h1 {
		t.Fatal("want a new harness each turn")
	}
	if s2.SessionID() != thread {
		t.Fatalf("SessionID = %q, want %q", s2.SessionID(), thread)
	}
	ms := s2.VFS()
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
	if s.VFS() != nil {
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
	if s3.SessionID() != thread {
		t.Fatalf("SessionID = %q, want %q", s3.SessionID(), thread)
	}
	if vfs.FuseAvailable() && s3.VFS().HostDir() == "" {
		t.Fatal("HostDir empty on third prompt")
	}
	if !vfs.FuseAvailable() {
		if _, err := s3.VFS().ReadDir(ctx, "/work"); err != nil {
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
	if s3.SessionID() != thread {
		t.Fatalf("SessionID = %q, want %q", s3.SessionID(), thread)
	}
	if vfs.FuseAvailable() {
		if s3.VFS() == nil || s3.VFS().HostDir() == "" {
			t.Fatal("want live HostDir on reconstructed harness")
		}
	} else if _, err := s3.VFS().ReadDir(ctx, "/work"); err != nil {
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
	if s.SessionID() != thread {
		t.Fatalf("SessionID = %q, want %q", s.SessionID(), thread)
	}
	if s.VFS().HostDir() == "" {
		t.Fatal("want HostDir after unblocked remount")
	}
	drainTurn(t, s)
}

type flipProjection struct{ calls int }

func (p *flipProjection) Available() bool {
	p.calls++
	return p.calls == 1
}
func (p *flipProjection) Attach(*vfs.MountSession, string) error { return nil }

// TestRunTurn_vfsProjection_inProcessAndFlip covers the two host projection
// outcomes that are not already hit by the FUSE two-turn test: DirectProjection
// write/read across turns, and a projection that refuses Attach after construct.
func TestRunTurn_vfsProjection_inProcessAndFlip(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry(testStore(t), "default",
		WithVFSProjection(DirectProjection{}),
		WithTracer(telemetry.Tracer()),
	)
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	thread := "sess-direct"
	s, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.DropLiveHarness(thread) })
	if err := s.VFS().WriteFile(ctx, "/work/note.md", []byte("direct\n")); err != nil {
		t.Fatal(err)
	}
	drainTurn(t, s)
	s2, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: thread, Prompt: "again"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.VFS().ReadFile(ctx, "/work/note.md")
	if err != nil || string(got) != "direct\n" {
		t.Fatalf("second turn VFS = %q err=%v", got, err)
	}
	drainTurn(t, s2)

	flip := NewRegistry(testStore(t), "default", WithVFSProjection(&flipProjection{}))
	flip.Register("default", vfsSpec(t, okModel(), "/work"))
	sf, err := flip.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: "sess-flip", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { flip.DropLiveHarness("sess-flip") })
	if err := sf.VFS().WriteFile(ctx, "/work/ok.md", []byte("ok\n")); err != nil {
		t.Fatal(err)
	}
	drainTurn(t, sf)
}

// TestRunTurn_constructFailures is fail-closed construct: blank thread id,
// unknown VFS profile, missing skill dir, and FuseProjection session-id sanitize.
func TestRunTurn_constructFailures(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}))
	r.Register("default", vfsSpec(t, okModel(), "/work"))
	_, err := r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: "   ", Prompt: "hi"})
	if err == nil || !strings.Contains(err.Error(), "session id") {
		t.Fatalf("want session id construct error, got %v", err)
	}

	bad := vfsSpec(t, okModel(), "/work")
	bad.FSBootstrap = []vfs.MountSpec{{Point: "/work", Profile: "nope"}}
	r.Register("default", bad)
	_, err = r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: "sess-bad-profile", Prompt: "hi"})
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("want unknown profile error, got %v", err)
	}

	r.Register("default", AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{
				MaxWindowSize:    8192,
				SkillDirectories: []string{filepath.Join(t.TempDir(), "does-not-exist")},
			},
			Model: okModel(),
		},
	})
	_, err = r.RunTurn(ctx, TurnRequest{AgentID: "default", ThreadID: "sess-skills", Prompt: "hi"})
	if err == nil || !strings.Contains(err.Error(), "initialize skills") {
		t.Fatalf("want skills construct error, got %v", err)
	}

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("dot-sess", reg)
	if err := ms.Materialize(ctx, []vfs.MountSpec{{Point: "/work", Profile: "local"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	err = FuseProjection{}.Attach(ms, ".")
	if vfs.FuseAvailable() {
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(ms.HostDir(), "session") {
			t.Fatalf("HostDir = %q, want sanitized session path", ms.HostDir())
		}
		return
	}
	if err == nil {
		t.Fatal("want Attach error when FUSE is unavailable")
	}
}
