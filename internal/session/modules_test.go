package session_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

func drainRuntime(sm *session.SessionManager) session.Runtime {
	ch := make(chan streaming.StreamEvent, 8)
	go func() {
		for range ch {
		}
	}()
	return session.NewRuntime(ch, sm)
}

// TestSessionModules_surviveCheckpoint is the durable-module outcome: permission
// memory, parked workers, search namespace, and a host VFS pointer survive
// Capture → JSON wire → Apply on a fresh manager.
func TestSessionModules_surviveCheckpoint(t *testing.T) {
	sm := session.NewSessionManager()
	sm.LoadUserAndPlanState(map[string]any{
		"_parked_workers":          "not-a-map",
		"_permission_always_allow": 42,
		"_permission_always_deny":  []any{"x"},
		"_on_call_stages":          make(chan int),
		"_search_namespace":        "not-a-uuid",
		"keep":                     "user",
	})
	if v, ok := sm.StateGet("keep"); !ok || v != "user" {
		t.Fatalf("user key after malformed bags: %v ok=%v", v, ok)
	}

	rt := drainRuntime(sm).WithToolCallID("spawn_1")
	if rt.CurrentToolCallID() != "spawn_1" {
		t.Fatalf("CurrentToolCallID=%q", rt.CurrentToolCallID())
	}

	if sm.Permissions.Decision("allow_tool") != session.PermissionNone {
		t.Fatal("malformed bags must not grant memory")
	}
	sm.Permissions.Remember("allow_tool", session.PermissionAllowAlways)
	sm.Permissions.Remember("deny_tool", session.PermissionDenyAlways)
	if _, ok := sm.OnCall.Get("w1", "tool_permission"); ok {
		t.Fatal("malformed stages must not decode")
	}
	sm.OnCall.Record("w1", "tool_permission", session.OnCallLayer{Args: `{"path":"/a"}`})
	if sm.Permissions.Decision("allow_tool") != session.PermissionAllowAlways {
		t.Fatal("allow-always not stored")
	}
	if sm.Permissions.Decision("deny_tool") != session.PermissionDenyAlways {
		t.Fatal("deny-always not stored")
	}

	sm.SetParkedWorker("spawn_1", session.ParkedWorkerMeta{
		WorkerName:        "researcher",
		WorkerSessionID:   "sess/w/researcher/spawn_1",
		Task:              "summarize the deal",
		ChildInterruptIDs: []string{"child-1"},
	})
	meta, ok := sm.ParkedWorker("spawn_1")
	if !ok || meta.WorkerName != "researcher" || meta.Task != "summarize the deal" {
		t.Fatalf("parked = %+v ok=%v", meta, ok)
	}

	ns := uuid.New()
	sm.Search.SetNamespace(ns)

	reg := vfs.NewBackendRegistry()
	ms, err := vfs.NewMountSession("sess-modules", reg)
	if err != nil {
		t.Fatal(err)
	}
	sm.VFS = ms
	if sm.VFS != ms {
		t.Fatal("VFS field must hold the host mount table")
	}

	cp, err := session.NewCheckpointer().Capture(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "go"}},
		sm, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.State.Modules["search"]) == 0 {
		t.Fatal("checkpoint must include search context")
	}
	if cp.State.Version != stores.CheckpointVersion || cp.State.RuntimeState != nil {
		t.Fatalf("checkpoint schema = version %d legacy=%#v", cp.State.Version, cp.State.RuntimeState)
	}

	// In-process Apply: typed park/permission maps + SearchContext blob.
	smTyped := session.NewSessionManager()
	if _, err := session.NewCheckpointer().Apply(*cp, smTyped); err != nil {
		t.Fatal(err)
	}
	assertModules(t, smTyped, ns, "spawn_1", "researcher")

	// JSON wire preserves typed module sections.
	rawState, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	var wire stores.SessionCheckpoint
	if err := json.Unmarshal(rawState, &wire); err != nil {
		t.Fatal(err)
	}

	sm2 := session.NewSessionManager()
	if _, err := session.NewCheckpointer().Apply(wire, sm2); err != nil {
		t.Fatal(err)
	}
	assertModules(t, sm2, ns, "spawn_1", "researcher")

	sm2.DeleteParkedWorker("spawn_1")
	sm2.SetParkedWorker("spawn_2", session.ParkedWorkerMeta{WorkerName: "writer", Task: "draft"})
	cp2, err := session.NewCheckpointer().Capture(nil, sm2, nil)
	if err != nil {
		t.Fatal(err)
	}
	sm3 := session.NewSessionManager()
	if _, err := session.NewCheckpointer().Apply(*cp2, sm3); err != nil {
		t.Fatal(err)
	}
	if _, ok := sm3.ParkedWorker("spawn_1"); ok {
		t.Fatal("deleted park must not reload")
	}
	got2, ok := sm3.ParkedWorker("spawn_2")
	if !ok || got2.WorkerName != "writer" {
		t.Fatalf("replacement park = %+v ok=%v", got2, ok)
	}

	fresh := uuid.New()
	sm2.Search = brain.NewSearchContext()
	sm2.Search.SetNamespace(fresh)
	gotFresh, ok := sm2.Search.Namespace()
	if !ok || gotFresh != fresh {
		t.Fatalf("replacement Search must be usable, got %v ok=%v", gotFresh, ok)
	}
}

func TestTypedCheckpoint_rejectsNamedModuleWithoutPartialApply(t *testing.T) {
	// Arrange
	source := session.NewSessionManager()
	source.Plan.SetDocument("source")
	checkpoint, err := session.NewCheckpointer().Capture(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "go"}},
		source,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.State.Modules["permissions"] = json.RawMessage(`{"allow":`)
	target := session.NewSessionManager()
	target.Plan.SetDocument("target")

	// Act
	_, err = session.NewCheckpointer().Apply(*checkpoint, target)

	// Assert
	if err == nil || target.Plan.Document() != "target" {
		t.Fatalf("Apply error = %v plan = %q", err, target.Plan.Document())
	}
}

func TestLegacyCheckpoint_migratesToTypedModules(t *testing.T) {
	// Arrange
	legacy, err := stores.NewCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "go"}},
		nil,
		map[string]any{
			"_plan":          []any{map[string]any{"title": "legacy", "status": "pending"}},
			"_plan_document": "legacy document",
			"host":           "value",
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	manager := session.NewSessionManager()

	// Act
	if _, err := session.NewCheckpointer().Apply(*legacy, manager); err != nil {
		t.Fatal(err)
	}
	migrated, err := session.NewCheckpointer().Capture(legacy.ContextWindow, manager, nil)

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if migrated.State.Version != stores.CheckpointVersion || len(migrated.State.Modules["plan"]) == 0 {
		t.Fatalf("migrated checkpoint = %#v", migrated.State)
	}
	if got := manager.Plan.Get(); len(got) != 1 || got[0].Title != "legacy" {
		t.Fatalf("migrated plan = %#v", got)
	}
}

func assertModules(t *testing.T, sm *session.SessionManager, ns uuid.UUID, parkID, worker string) {
	t.Helper()
	if sm.Permissions.Decision("allow_tool") != session.PermissionAllowAlways ||
		sm.Permissions.Decision("deny_tool") != session.PermissionDenyAlways {
		t.Fatal("permission memory must reload")
	}
	layer, ok := sm.OnCall.Get("w1", "tool_permission")
	if !ok || layer.Denied || layer.Args != `{"path":"/a"}` {
		t.Fatalf("on-call stage reload args=%q denied=%v ok=%v", layer.Args, layer.Denied, ok)
	}
	got, ok := sm.ParkedWorker(parkID)
	if !ok || got.WorkerName != worker || got.WorkerSessionID != "sess/w/researcher/spawn_1" {
		t.Fatalf("park reload = %+v ok=%v", got, ok)
	}
	gotNS, ok := sm.Search.Namespace()
	if !ok || gotNS != ns {
		t.Fatalf("search namespace %v ok=%v want %v", gotNS, ok, ns)
	}
}
