package inprocess

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

// failSnap is a host SnapshotStore that can take load/save down and come back.
type failSnap struct {
	durable.SnapshotStore
	loadErr    error
	failSaveOn int
	saves      int
}

func (s *failSnap) Save(ctx context.Context, id durable.SessionID, snap durable.Snapshot, expected durable.Revision) (durable.Revision, error) {
	s.saves++
	if s.failSaveOn > 0 && s.saves >= s.failSaveOn {
		return "", errors.New("snapshot save down")
	}
	return s.SnapshotStore.Save(ctx, id, snap, expected)
}

func (s *failSnap) Load(ctx context.Context, id durable.SessionID) (durable.Snapshot, durable.Revision, error) {
	if s.loadErr != nil {
		return durable.Snapshot{}, "", s.loadErr
	}
	return s.SnapshotStore.Load(ctx, id)
}

func TestRuntime_snapshotStoreDownThenRecovers(t *testing.T) {
	ctx := t.Context()
	store := &failSnap{SnapshotStore: NewMemorySnapshot(), loadErr: errors.New("snapshot load down")}
	rt := New(Config{Catalog: newCatalog(t, scriptedComplete("recovered"), durable.AgentSpec{}), Snapshots: store, Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	waitTurnPersisted(t, rt, id)
	st, err := rt.Status(ctx, id)
	if err != nil || st.State != durable.SessionFailed {
		t.Fatalf("load down: %+v %v", st, err)
	}

	store.loadErr = nil
	store.failSaveOn = 1
	store.saves = 0
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	waitTurnPersisted(t, rt, id)
	st, err = rt.Status(ctx, id)
	if err != nil || st.State != durable.SessionFailed {
		t.Fatalf("save down: %+v %v", st, err)
	}

	store.failSaveOn = 0
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	waitTurnPersisted(t, rt, id)
	st, err = rt.Status(ctx, id)
	if err != nil || st.State != durable.SessionComplete || st.Result != "recovered" {
		t.Fatalf("recovered: %+v %v", st, err)
	}
}

func TestRuntime_hostCloseChildParentStillRuns(t *testing.T) {
	ctx := t.Context()
	started := make(chan struct{}, 1)
	stopped := make(chan struct{})
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{spawnAsync("researcher", "sp1")}, IsComplete: true,
			}
		},
	}
	rt := New(Config{
		Catalog:    specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: hangUntilCancel(started, stopped)}),
		Snapshots:  NewMemorySnapshot(),
		Projection: vfs.DirectProjection{},
	})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not start")
	}
	if err := rt.Close(ctx, durable.ChildSessionID(id, "researcher", "sp1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("closed child turn was not cancelled")
	}
	_ = waitEvents(t, rt, id, sub, 8*time.Second)
	st, err := rt.Status(ctx, id)
	if err != nil || st.State == durable.SessionUnknown {
		t.Fatalf("parent after child close: %+v %v", st, err)
	}
}
