package tacklr

import (
	"encoding/json"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

func drainRuntime(sm *sessionManager) sessionRuntime {
	ch := make(chan StreamEvent, 8)
	go func() {
		for range ch {
		}
	}()
	return newSessionRuntime(ch, sm)
}

// TestSessionModules_surviveCheckpoint is the durable-module outcome: permission
// memory, search namespace, and a host VFS pointer survive Capture → JSON wire
// → Apply on a fresh manager.
func TestSessionModules_surviveCheckpoint(t *testing.T) {
	sm := newSessionManager()
	if err := sm.StateSet("keep", "user"); err != nil {
		t.Fatal(err)
	}
	if v, ok := sm.StateGet("keep"); !ok || v != "user" {
		t.Fatalf("user key: %v ok=%v", v, ok)
	}

	rt := drainRuntime(sm).WithToolCallID("spawn_1")
	if rt.CurrentToolCallID() != "spawn_1" {
		t.Fatalf("CurrentToolCallID=%q", rt.CurrentToolCallID())
	}

	sm.Permissions.Remember("allow_tool", permAllowAlways)
	sm.Permissions.Remember("deny_tool", permDenyAlways)
	sm.OnCall.Record("w1", "tool_permission", onCallLayer{Args: `{"path":"/a"}`})
	if sm.Permissions.Decision("allow_tool") != permAllowAlways {
		t.Fatal("allow-always not stored")
	}
	if sm.Permissions.Decision("deny_tool") != permDenyAlways {
		t.Fatal("deny-always not stored")
	}

	ns := mustNS(t, "org", "acme")
	sm.Search.SetNamespace(ns)

	ms, err := vfs.NewMountSession("sess-modules")
	if err != nil {
		t.Fatal(err)
	}
	sm.VFS = ms
	if sm.VFS != ms {
		t.Fatal("VFS field must hold the host mount table")
	}

	cp, err := captureCheckpoint(
		[]*Message{{Role: RoleUser, Content: "go"}},
		sm, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.Modules()["search"]) == 0 {
		t.Fatal("checkpoint must include search context")
	}
	if cp.Version() != CheckpointVersion {
		t.Fatalf("checkpoint schema version = %d", cp.Version())
	}

	// In-process Apply: typed park/permission maps + SearchContext blob.
	smTyped := newSessionManager()
	if _, err := applyCheckpoint(*cp, smTyped); err != nil {
		t.Fatal(err)
	}
	assertModules(t, smTyped, ns)

	// JSON wire preserves typed module sections.
	rawState, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	var wire SessionCheckpoint
	if err := json.Unmarshal(rawState, &wire); err != nil {
		t.Fatal(err)
	}

	sm2 := newSessionManager()
	if _, err := applyCheckpoint(wire, sm2); err != nil {
		t.Fatal(err)
	}
	assertModules(t, sm2, ns)

	fresh := mustNS(t, "org", "other")
	sm2.Search = brain.NewSearchContext()
	sm2.Search.SetNamespace(fresh)
	gotFresh, ok := sm2.Search.Namespace()
	if !ok || !gotFresh.Equal(fresh) {
		t.Fatalf("replacement Search must be usable, got %v ok=%v", gotFresh, ok)
	}
}

func TestTypedCheckpoint_rejectsNamedModuleWithoutPartialApply(t *testing.T) {
	// Arrange
	source := newSessionManager()
	source.Plan.SetDocument("source")
	checkpoint, err := captureCheckpoint(
		[]*Message{{Role: RoleUser, Content: "go"}},
		source,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	cp := checkpoint.WithModule("permissions", json.RawMessage(`{"allow":`))
	target := newSessionManager()
	target.Plan.SetDocument("target")

	// Act
	_, err = applyCheckpoint(cp, target)

	// Assert
	if err == nil || target.Plan.Document() != "target" {
		t.Fatalf("Apply error = %v plan = %q", err, target.Plan.Document())
	}
}

func TestTypedCheckpoint_rejectsCorruptModuleSections(t *testing.T) {
	// Arrange
	source := newSessionManager()
	checkpoint, err := captureCheckpoint(
		[]*Message{{Role: RoleUser, Content: "go"}},
		source,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]json.RawMessage{
		"plan":        json.RawMessage(`{"todos":`),
		"permissions": json.RawMessage(`{"allow":`),
		"onCall":      json.RawMessage(`{"stages":`),
		"search":      json.RawMessage(`{`),
	}
	for module, raw := range cases {
		t.Run(module, func(t *testing.T) {
			wire := checkpoint.WithModule(module, raw)
			target := newSessionManager()
			target.Plan.SetDocument("target")
			if _, err := applyCheckpoint(wire, target); err == nil {
				t.Fatalf("module %q was accepted", module)
			}
			if target.Plan.Document() != "target" {
				t.Fatalf("partial apply mutated plan for module %q", module)
			}
		})
	}
}

func TestTypedCheckpoint_rejectsCorruptUserState(t *testing.T) {
	// Arrange
	source := newSessionManager()
	if err := source.StateSet("keep", "user"); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := captureCheckpoint(
		[]*Message{{Role: RoleUser, Content: "go"}},
		source,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	cp := checkpoint.WithUserStateKey("bad", json.RawMessage(`{`))
	target := newSessionManager()

	// Act
	_, err = applyCheckpoint(cp, target)

	// Assert
	if err == nil {
		t.Fatal("corrupt user state was accepted")
	}
}

func assertModules(t *testing.T, sm *sessionManager, ns brain.Namespace) {
	t.Helper()
	if sm.Permissions.Decision("allow_tool") != permAllowAlways ||
		sm.Permissions.Decision("deny_tool") != permDenyAlways {
		t.Fatal("permission memory must reload")
	}
	layer, ok := sm.OnCall.Get("w1", "tool_permission")
	if !ok || layer.Denied || layer.Args != `{"path":"/a"}` {
		t.Fatalf("on-call stage reload args=%q denied=%v ok=%v", layer.Args, layer.Denied, ok)
	}
	gotNS, ok := sm.Search.Namespace()
	if !ok || !gotNS.Equal(ns) {
		t.Fatalf("search namespace %v ok=%v want %v", gotNS, ok, ns)
	}
}
