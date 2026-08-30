package inprocess

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

// persistGate is a host SnapshotStore that blocks Save until release. A host
// that sees complete while the checkpoint is still unsaved is the Status race.
type persistGate struct {
	inner   durable.SnapshotStore
	began   chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPersistGate(inner durable.SnapshotStore) *persistGate {
	return &persistGate{
		inner:   inner,
		began:   make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *persistGate) Save(ctx context.Context, id durable.SessionID, snap durable.Snapshot, rev durable.Revision) (durable.Revision, error) {
	g.once.Do(func() { close(g.began) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return g.inner.Save(ctx, id, snap, rev)
}

func (g *persistGate) Load(ctx context.Context, id durable.SessionID) (durable.Snapshot, durable.Revision, error) {
	return g.inner.Load(ctx, id)
}

func (g *persistGate) Delete(ctx context.Context, id durable.SessionID) error {
	return g.inner.Delete(ctx, id)
}

func (g *persistGate) open() { close(g.release) }

func subscribe(t *testing.T, rt *Runtime, id durable.SessionID) durable.Subscription {
	t.Helper()
	sub, err := rt.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func TestPrompt_completeMeansStatusIsCompleteAndPersisted(t *testing.T) {
	ctx := t.Context()
	gate := newPersistGate(NewMemorySnapshot())
	rt := New(Config{
		Catalog:    newCatalog(t, scriptedComplete("hello from agent"), durable.AgentSpec{}),
		Snapshots:  gate,
		Projection: vfs.DirectProjection{},
	})

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}

	wait, stopWait := context.WithTimeout(ctx, 5*time.Second)
	defer stopWait()
	select {
	case <-gate.began:
	case <-wait.Done():
		t.Fatal("checkpoint save never started")
	}

	sub := subscribe(t, rt, id)
	waitParentEvent(t, rt, id, sub, 5*time.Second, func(ev tacklr.StreamEvent) bool {
		if ev.Type == tacklr.StreamEventComplete {
			t.Fatal("Subscribe delivered complete before the checkpoint was saved")
		}
		return ev.Type == tacklr.StreamEventMessage && ev.Content == "hello from agent"
	})
	select {
	case ev := <-sub.Events():
		if ev.Type == tacklr.StreamEventComplete {
			t.Fatal("Subscribe delivered complete before the checkpoint was saved")
		}
	default:
	}
	st, err := rt.Status(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != durable.SessionRunning {
		t.Fatalf("Status while persist is in flight: %+v", st)
	}

	gate.open()
	waitParentEvent(t, rt, id, sub, 5*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventComplete
	})
	st, err = rt.Status(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Result != "hello from agent" {
		t.Fatalf("Status.Result=%q", st.Result)
	}
	if _, _, err := gate.Load(ctx, id); err != nil {
		t.Fatalf("complete must mean the checkpoint is loadable: %v", err)
	}
}

func TestPrompt_nextTurnIsRunningUntilItsComplete(t *testing.T) {
	ctx := t.Context()
	var n atomic.Int32
	block := make(chan struct{})
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			i := n.Add(1)
			if i == 2 {
				select {
				case <-block:
				case <-ctx.Done():
					return
				}
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventMessage, Content: "turn-" + strconv.Itoa(int(i)), IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)

	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	waitParentEvent(t, rt, id, sub, 5*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventComplete
	})
	st, err := rt.Status(ctx, id)
	if err != nil || st.State != durable.SessionComplete || st.Result != "turn-1" {
		t.Fatalf("first turn: %+v %v", st, err)
	}

	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "two"}); err != nil {
		t.Fatal(err)
	}
	st, err = rt.Status(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != durable.SessionRunning {
		t.Fatalf("second Prompt returned but Status is still %+v", st)
	}

	close(block)
	waitParentEvent(t, rt, id, sub, 5*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventComplete
	})
	st, err = rt.Status(ctx, id)
	if err != nil || st.State != durable.SessionComplete || st.Result != "turn-2" {
		t.Fatalf("second turn: %+v %v", st, err)
	}
}

func TestPrompt_yieldKeepsStatusRunningAndWaiting(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "chose", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "ask"}); err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)

	waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		if ev.Type == tacklr.StreamEventComplete {
			t.Fatal("complete while parked for user input")
		}
		return ev.Type == tacklr.StreamEventInterrupt
	})

	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := rt.Resume(ctx, id, durable.Resume{Responses: map[string][]byte{"ask1": payload}}); err != nil {
		t.Fatal(err)
	}
	waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventComplete
	})
	st, err := rt.Status(ctx, id)
	if err != nil || st.State != durable.SessionComplete {
		t.Fatalf("after resume complete: %+v %v", st, err)
	}
}

func TestPrompt_errorMeansStatusIsFailed(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("provider exploded")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Content: boom.Error(), Error: boom}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)
	waitParentEvent(t, rt, id, sub, 5*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventError
	})
}
