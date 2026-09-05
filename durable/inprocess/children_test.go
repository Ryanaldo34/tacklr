package inprocess

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/durtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

func interruptChildModel(content string) *testkit.ScriptedModel {
	var invokes atomic.Int32
	return &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if invokes.Add(1) == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "ask1", CallID: "ask1", Name: "ask_user", Arguments: `{}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: content, IsComplete: true}
		},
	}
}

func askUserTool() *tacklr.Tool {
	options := `[{"title":"A","description":"a","isRecommended":true},{"title":"B","description":"b","isRecommended":false}]`
	return tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			intr, err := runtime.Park("user_selection_choice", []byte(options))
			if err != nil {
				return "", err
			}
			return "selected: " + intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})
}

func waitStatus(t *testing.T, rt *Runtime, id durable.SessionID, pred func(durable.SessionStatus) bool) durable.SessionStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	var last durable.SessionStatus
	for {
		st, err := rt.Status(t.Context(), id)
		if err == nil {
			last = st
			if pred(st) {
				return st
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("status %s not reached: %+v", id, last)
		case <-tick.C:
		}
	}
}

func waitGone(t *testing.T, rt *Runtime, id durable.SessionID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := rt.Status(t.Context(), id); errors.Is(err, durable.ErrSessionNotFound) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("session %s still present", id)
		case <-tick.C:
		}
	}
}

func waitParentEvent(t *testing.T, rt *Runtime, id durable.SessionID, sub durable.Subscription, timeout time.Duration, pred func(tacklr.StreamEvent) bool) tacklr.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var seen []tacklr.StreamEvent
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed, seen=%v", summarize(seen))
			}
			seen = append(seen, ev)
			if pred(ev) {
				durtest.AssertStatusMatchesEvent(t, rt, id, ev)
				return ev
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for parent event, seen=%v", summarize(seen))
		}
	}
}

func specialistCatalog(t *testing.T, parent tacklr.InferenceStrategy, kids ...tacklr.Specialist) *durable.MemoryCatalog {
	t.Helper()
	specs := make([]*tacklr.Specialist, len(kids))
	for i := range kids {
		cp := kids[i]
		specs[i] = &cp
	}
	return newCatalog(t, parent, durable.AgentSpec{
		Options: tacklr.AgentOptions{Specialists: specs},
	})
}

func begin(t *testing.T, rt *Runtime, id *durable.SessionID) durable.Subscription {
	t.Helper()
	return beginWithPrompt(t.Context(), t, rt, id)
}

func beginWithPrompt(ctx context.Context, t *testing.T, rt *Runtime, id *durable.SessionID) durable.Subscription {
	t.Helper()
	sid, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	*id = sid
	if err := rt.Prompt(ctx, sid, durable.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(t.Context(), sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func spawnAsync(specialist, callID string) tacklr.ToolCall {
	return tacklr.ToolCall{
		ID: callID, CallID: callID, Name: "spawn_specialist",
		Arguments: `{"specialist":"` + specialist + `","task_description_and_context":"x","block":false}`,
	}
}

func hangUntilCancel(started, stopped chan struct{}) *testkit.ScriptedModel {
	return &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			close(stopped)
		},
	}
}

func TestChildren_asyncSpawnThenCollect(t *testing.T) {
	child := scriptedComplete("from-research")
	var step atomic.Int32
	var sawJob, sawToolJob atomic.Bool
	var rt *Runtime
	var parentID durable.SessionID
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := step.Add(1)
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{spawnAsync("researcher", "sp1")}, IsComplete: true,
				}
				return
			}
			id := durable.ChildSessionID(parentID, "researcher", "sp1")
			waitStatus(t, rt, id, func(st durable.SessionStatus) bool {
				return st.State == durable.SessionComplete || st.State == durable.SessionFailed
			})
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser && strings.Contains(m.Content, "completed:") && strings.Contains(m.Content, "from-research") {
					sawJob.Store(true)
				}
				if m != nil && m.Role == tacklr.RoleTool && strings.Contains(m.Content, "from-research") {
					sawToolJob.Store(true)
				}
			}
			if sawJob.Load() {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		},
	}
	rt = New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: child}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	sub := begin(t, rt, &parentID)
	childID := durable.ChildSessionID(parentID, "researcher", "sp1")
	got := waitEvents(t, rt, parentID, sub, 8*time.Second)
	var sawDone bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
			sawDone = true
		}
	}
	if sawToolJob.Load() {
		t.Fatal("job result must not be a second RoleTool for the spawn call_id")
	}
	if !sawJob.Load() || !sawDone {
		t.Fatalf("job=%v done=%v events=%v", sawJob.Load(), sawDone, summarize(got))
	}
	kids, err := rt.Children(t.Context(), parentID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range kids {
		if id == childID {
			t.Fatalf("auto-collected child still on parent list: %v", kids)
		}
	}
	if st, err := rt.Status(t.Context(), childID); err != nil || st.State != durable.SessionComplete {
		t.Fatalf("collected child should remain status-able: %+v %v", st, err)
	}
}

func TestChildren_mixedBlockingPairsBeforeNextRound(t *testing.T) {
	release := make(chan struct{})
	var blockingStarted, released atomic.Bool
	blocker := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			blockingStarted.Store(true)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "block-result", IsComplete: true}
		},
	}
	async := scriptedComplete("async-result")
	var step atomic.Int32
	var tooEarly atomic.Bool
	var sawBlock, sawAsync atomic.Bool
	var parentID durable.SessionID
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := step.Add(1)
			switch n {
			case 1:
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{
						{ID: "b1", CallID: "b1", Name: "spawn_specialist", Arguments: `{"specialist":"blocker","task_description_and_context":"wait","block":true}`},
						spawnAsync("async", "a1"),
					},
					IsComplete: true,
				}
			case 2:
				if !released.Load() {
					tooEarly.Store(true)
				}
				for _, m := range msgs {
					if m == nil || m.Role != tacklr.RoleTool {
						continue
					}
					if m.Content == "block-result" {
						sawBlock.Store(true)
					}
					if strings.Contains(m.Content, "Child ") && strings.Contains(m.Content, "scheduled") {
						sawAsync.Store(true)
					}
				}
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
			default:
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
			}
		},
	}
	rt := New(Config{Catalog: specialistCatalog(t, parent,
		tacklr.Specialist{Name: "blocker", Model: blocker},
		tacklr.Specialist{Name: "async", Model: async},
	), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	sub := begin(t, rt, &parentID)
	go func() {
		for !blockingStarted.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		released.Store(true)
		close(release)
	}()
	got := waitEvents(t, rt, parentID, sub, 8*time.Second)
	if tooEarly.Load() {
		t.Fatal("next model round started before blocking child finished")
	}
	if !sawBlock.Load() || !sawAsync.Load() {
		t.Fatalf("second round missing pairs block=%v async=%v events=%v", sawBlock.Load(), sawAsync.Load(), summarize(got))
	}
	var blockOut, asyncOut, second bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && ev.Content == "block-result" {
			blockOut = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "scheduled") {
			asyncOut = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "second-round" {
			second = true
		}
	}
	if !blockOut || !asyncOut || !second {
		t.Fatalf("stream pairing block=%v async=%v second=%v events=%v", blockOut, asyncOut, second, summarize(got))
	}
}

func TestChildren_waitingStaysRunningUntilHITLResolved(t *testing.T) {
	child := interruptChildModel("chose-A")
	var step atomic.Int32
	var rt *Runtime
	var parentID durable.SessionID
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := step.Add(1)
			id := durable.ChildSessionID(parentID, "researcher", "sp1")
			switch n {
			case 1:
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{spawnAsync("researcher", "sp1")}, IsComplete: true,
				}
			case 2:
				st := waitStatus(t, rt, id, func(st durable.SessionStatus) bool { return st.Waiting })
				if st.State != durable.SessionRunning {
					t.Fatalf("HITL must stay running: %+v", st)
				}
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{
						{ID: "lc1", CallID: "lc1", Name: "list_children", Arguments: `{}`},
						{ID: "gc1", CallID: "gc1", Name: "get_child", Arguments: `{"child_id":"` + string(id) + `"}`},
					},
					IsComplete: true,
				}
			case 3:
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "gc2", CallID: "gc2", Name: "get_child",
						Arguments: `{"child_id":"` + string(id) + `","block":true}`,
					}},
					IsComplete: true,
				}
			default:
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
			}
		},
	}
	rt = New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: child, Tools: []*tacklr.Tool{askUserTool()}}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	sub := begin(t, rt, &parentID)

	var listOut, poll string
	wait, stopWait := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopWait()
	for listOut == "" || poll == "" {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("subscription closed")
			}
			if ev.Type != tacklr.StreamEventToolResult {
				continue
			}
			if strings.Contains(ev.Content, "Child sessions:") {
				listOut = ev.Content
			}
			if strings.Contains(ev.Content, "Still running") {
				poll = ev.Content
			}
		case <-wait.Done():
			t.Fatalf("list=%q poll=%q", listOut, poll)
		}
	}
	if strings.Contains(listOut, "interrupted") || !strings.Contains(listOut, "status=running") {
		t.Fatalf("list_children HITL: %q", listOut)
	}
	if !strings.Contains(poll, "status=running") {
		t.Fatalf("non-blocking get while HITL: %q", poll)
	}

	waitParentEvent(t, rt, parentID, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventInterrupt && ev.MessageID == "gc2"
	})
	childID := durable.ChildSessionID(parentID, "researcher", "sp1")
	st, err := rt.Status(t.Context(), childID)
	if err != nil || st.State != durable.SessionRunning || !st.Waiting {
		t.Fatalf("child after parent park: %+v %v", st, err)
	}

	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := rt.Resume(t.Context(), parentID, durable.Resume{Responses: map[string][]byte{"gc2": payload}}); err != nil {
		t.Fatal(err)
	}
	wait, stopWait = context.WithTimeout(t.Context(), 8*time.Second)
	defer stopWait()
	var collected, done bool
	for !collected || !done {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("closed")
			}
			if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "chose-A") {
				collected = true
			}
			if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
				done = true
			}
			if ev.Type == tacklr.StreamEventError {
				t.Fatalf("error: %v %s", ev.Error, ev.Content)
			}
		case <-wait.Done():
			t.Fatalf("resume collect=%v done=%v", collected, done)
		}
	}
}

func TestChildren_getChildParkAndSiblingCancel(t *testing.T) {
	keep := interruptChildModel("kept-result")
	drop := interruptChildModel("dropped")
	var step atomic.Int32
	var rt *Runtime
	var parentID durable.SessionID
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := step.Add(1)
			keepID := durable.ChildSessionID(parentID, "keeper", "sp_keep")
			dropID := durable.ChildSessionID(parentID, "dropper", "sp_drop")
			switch n {
			case 1:
				ch <- tacklr.LLMResponseChunk{
					Type:       tacklr.StreamEventFunctionCall,
					ToolCalls:  []tacklr.ToolCall{spawnAsync("keeper", "sp_keep"), spawnAsync("dropper", "sp_drop")},
					IsComplete: true,
				}
			case 2:
				waitStatus(t, rt, keepID, func(st durable.SessionStatus) bool { return st.Waiting })
				waitStatus(t, rt, dropID, func(st durable.SessionStatus) bool { return st.Waiting })
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{
						{ID: "gc1", CallID: "gc1", Name: "get_child", Arguments: `{"child_id":"` + string(keepID) + `","block":true}`},
						{ID: "cc1", CallID: "cc1", Name: "cancel_child", Arguments: `{"child_id":"` + string(dropID) + `"}`},
					},
					IsComplete: true,
				}
			default:
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
			}
		},
	}
	rt = New(Config{Catalog: specialistCatalog(t, parent,
		tacklr.Specialist{Name: "keeper", Model: keep, Tools: []*tacklr.Tool{askUserTool()}},
		tacklr.Specialist{Name: "dropper", Model: drop, Tools: []*tacklr.Tool{askUserTool()}},
	), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	sub := begin(t, rt, &parentID)

	var cancelOut string
	var parked bool
	wait, stopWait := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopWait()
	for !parked || cancelOut == "" {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("closed")
			}
			if ev.Type == tacklr.StreamEventInterrupt && ev.MessageID == "gc1" {
				parked = true
			}
			if ev.Type == tacklr.StreamEventToolResult {
				for _, tc := range ev.ToolCalls {
					if tc.Name == "get_child" {
						t.Fatalf("blocking get completed before resume: %q", ev.Content)
					}
					if tc.Name == "cancel_child" {
						cancelOut = ev.Content
					}
				}
			}
		case <-wait.Done():
			t.Fatalf("park=%v cancel=%q", parked, cancelOut)
		}
	}
	if !strings.Contains(cancelOut, "cancelled") {
		t.Fatalf("cancel leftover: %q", cancelOut)
	}
	waitGone(t, rt, durable.ChildSessionID(parentID, "dropper", "sp_drop"))

	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := rt.Resume(t.Context(), parentID, durable.Resume{Responses: map[string][]byte{"gc1": payload}}); err != nil {
		t.Fatal(err)
	}
	waitParentEvent(t, rt, parentID, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "kept-result")
	})
}

func TestChildren_stopKillsRunningChild(t *testing.T) {
	tests := []struct {
		name  string
		abort func(rt *Runtime, id durable.SessionID, cancelPrompt context.CancelFunc)
	}{
		{"Cancel", func(rt *Runtime, id durable.SessionID, _ context.CancelFunc) {
			if err := rt.Cancel(t.Context(), id); err != nil {
				t.Fatal(err)
			}
		}},
		{"PromptContext", func(_ *Runtime, _ durable.SessionID, cancelPrompt context.CancelFunc) {
			cancelPrompt()
		}},
		{"Close", func(rt *Runtime, id durable.SessionID, _ context.CancelFunc) {
			if err := rt.Close(t.Context(), id); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			stopped := make(chan struct{})
			parent := &testkit.ScriptedModel{
				InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
					ch <- tacklr.LLMResponseChunk{
						Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{spawnAsync("researcher", "sp1")}, IsComplete: true,
					}
				},
			}
			rt := New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{
				Name: "researcher", Model: hangUntilCancel(started, stopped),
			}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
			var id durable.SessionID
			promptCtx, cancelPrompt := context.WithCancel(t.Context())
			t.Cleanup(cancelPrompt)
			sub := beginWithPrompt(promptCtx, t, rt, &id)
			waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
				return ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "/w/researcher/sp1")
			})
			waitStart, stopStart := context.WithTimeout(t.Context(), 5*time.Second)
			defer stopStart()
			select {
			case <-started:
			case <-waitStart.Done():
				t.Fatal("child did not start")
			}
			tc.abort(rt, id, cancelPrompt)
			waitGone(t, rt, durable.ChildSessionID(id, "researcher", "sp1"))
			waitStop, stopStop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stopStop()
			select {
			case <-stopped:
			case <-waitStop.Done():
				t.Fatal("child turn was not cancelled")
			}
		})
	}
}

func TestChildren_unknownSpecialistAndChild(t *testing.T) {
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{
					{ID: "sp1", CallID: "sp1", Name: "spawn_specialist", Arguments: `{"specialist":"ghost","task_description_and_context":"x"}`},
					{ID: "gc1", CallID: "gc1", Name: "get_child", Arguments: `{"child_id":"missing"}`},
					{ID: "gc0", CallID: "gc0", Name: "get_child", Arguments: `{}`},
					{ID: "cc1", CallID: "cc1", Name: "cancel_child", Arguments: `{"child_id":"missing"}`},
					{ID: "cc0", CallID: "cc0", Name: "cancel_child", Arguments: `{}`},
					{ID: "bad", CallID: "bad", Name: "spawn_specialist", Arguments: `{}`},
					{ID: "ns1", CallID: "ns1", Name: "spawn_specialist", Arguments: `{"task_description_and_context":"x"}`},
				},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: scriptedComplete("x")}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var texts []string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult {
			texts = append(texts, ev.Content)
		}
	}
	blob := strings.Join(texts, "\n")
	for _, want := range []string{
		"that specialist is not registered",
		"that child_id is unknown",
		"child_id is required",
		"task_description_and_context is required",
		"specialist is required",
	} {
		if !strings.Contains(blob, want) {
			t.Fatalf("want %q in invalid-tool results: %v", want, texts)
		}
	}
}

func TestChildren_nudgeUntilCollected(t *testing.T) {
	release := make(chan struct{})
	child := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "nudge-result", IsComplete: true}
		},
	}
	var step atomic.Int32
	var sawNudge atomic.Bool
	var parentID durable.SessionID
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := step.Add(1)
			id := durable.ChildSessionID(parentID, "researcher", "sp1")
			switch n {
			case 1:
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{spawnAsync("researcher", "sp1")}, IsComplete: true,
				}
			case 2:
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "too-soon", IsComplete: true}
			case 3:
				for _, m := range msgs {
					if m != nil && m.Role == tacklr.RoleUser && strings.Contains(m.Content, "Automated harness nudge") {
						sawNudge.Store(true)
					}
				}
				close(release)
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "gc1", CallID: "gc1", Name: "get_child",
						Arguments: `{"child_id":"` + string(id) + `","block":true}`,
					}},
					IsComplete: true,
				}
			default:
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
			}
		},
	}
	rt := New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: child}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	sub := begin(t, rt, &parentID)
	got := waitEvents(t, rt, parentID, sub, 8*time.Second)
	var sawCollect, sawDone bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "nudge-result") {
			sawCollect = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
			sawDone = true
		}
	}
	if !sawNudge.Load() || !sawCollect || !sawDone {
		t.Fatalf("nudge=%v collect=%v done=%v events=%v", sawNudge.Load(), sawCollect, sawDone, summarize(got))
	}
}

func TestChildren_blockingSpawnParksOnChildHITL(t *testing.T) {
	child := interruptChildModel("blocked-ok")
	var step atomic.Int32
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if step.Add(1) == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
						Arguments: `{"specialist":"researcher","task_description_and_context":"ask"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
		},
	}
	rt := New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: child, Tools: []*tacklr.Tool{askUserTool()}}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventInterrupt && ev.MessageID == "sp1"
	})
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 1})
	if err := rt.Resume(t.Context(), id, durable.Resume{Responses: map[string][]byte{"sp1": payload}}); err != nil {
		t.Fatal(err)
	}
	wait, stopWait := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopWait()
	var spawnOut, done bool
	for !spawnOut || !done {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("closed")
			}
			if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "blocked-ok") {
				spawnOut = true
			}
			if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
				done = true
			}
			if ev.Type == tacklr.StreamEventError {
				t.Fatalf("error: %v %s", ev.Error, ev.Content)
			}
		case <-wait.Done():
			t.Fatalf("spawnOut=%v done=%v", spawnOut, done)
		}
	}
}

func TestChildren_parentHITLLeavesAsyncChildRunning(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	child := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "bg", IsComplete: true}
		},
	}
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-hitl", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{
					spawnAsync("researcher", "sp1"),
					{ID: "ask1", CallID: "ask1", Name: "ask_user_choice", Arguments: `{"question":"Go?","choices":[{"title":"Yes"},{"title":"No"}]}`},
				},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: child}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	var spawned, parked bool
	wait, stopWait := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopWait()
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("closed")
			}
			if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "scheduled") {
				spawned = true
			}
			if ev.Type == tacklr.StreamEventInterrupt && ev.MessageID == "ask1" {
				parked = true
			}
			if spawned && parked {
				st, err := rt.Status(t.Context(), durable.ChildSessionID(id, "researcher", "sp1"))
				if err != nil || st.State != durable.SessionRunning {
					t.Fatalf("child during parent HITL: %+v %v", st, err)
				}
				close(release)
				return
			}
		case <-wait.Done():
			t.Fatalf("parent did not park with child running spawned=%v parked=%v", spawned, parked)
		}
	}
}

func TestChildren_failedChildIsCollectable(t *testing.T) {
	failing := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Content: "boom-child", IsComplete: true}
		},
	}
	var step atomic.Int32
	var sawJob atomic.Bool
	var rt *Runtime
	var parentID durable.SessionID
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := step.Add(1)
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{spawnAsync("researcher", "sp1")}, IsComplete: true,
				}
				return
			}
			id := durable.ChildSessionID(parentID, "researcher", "sp1")
			waitStatus(t, rt, id, func(st durable.SessionStatus) bool {
				return st.State == durable.SessionFailed || st.State == durable.SessionComplete
			})
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser && strings.Contains(m.Content, "failed:") {
					sawJob.Store(true)
				}
			}
			if sawJob.Load() {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		},
	}
	rt = New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{Name: "researcher", Model: failing}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	sub := begin(t, rt, &parentID)
	got := waitEvents(t, rt, parentID, sub, 8*time.Second)
	var sawDone bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
			sawDone = true
		}
	}
	if !sawJob.Load() || !sawDone {
		t.Fatalf("job=%v done=%v events=%v", sawJob.Load(), sawDone, summarize(got))
	}
}

func TestChildren_nestedSpecialistCollectsGrandchild(t *testing.T) {
	researcher := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "dg1", CallID: "dg1", Name: "spawn_specialist",
					Arguments: `{"specialist":"digger","task_description_and_context":"deeper"}`,
				}},
				IsComplete: true,
			}
		},
	}
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"go"}`,
				}},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: specialistCatalog(t, parent, tacklr.Specialist{
		Name:  "researcher",
		Model: researcher,
		Specialists: []*tacklr.Specialist{{
			Name:  "digger",
			Model: scriptedComplete("from-digger"),
		}},
	}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var sawNested, sawDone bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "from-digger") {
			sawNested = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
			sawDone = true
		}
	}
	if !sawNested || !sawDone {
		t.Fatalf("nested=%v done=%v events=%v", sawNested, sawDone, summarize(got))
	}
}

func TestChildren_customToolSpawnsChild(t *testing.T) {
	delegate := tacklr.NewTool(tacklr.ToolConfig{
		Name: "delegate",
		Handler: func(ctx context.Context, args struct {
			Specialist string `json:"specialist"`
			Task       string `json:"task"`
		}, runtime tacklr.HarnessRuntime) (string, error) {
			id, err := runtime.SpawnChild(ctx, args.Specialist, args.Task)
			if err != nil {
				return "", err
			}
			child, err := runtime.AwaitChild(ctx, id)
			if err != nil {
				return "", err
			}
			return child.Result, nil
		},
	})
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "c1", CallID: "c1", Name: "delegate",
					Arguments: `{"specialist":"researcher","task":"dig"}`,
				}},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, parent, durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Tools: []*tacklr.Tool{delegate},
			Specialists: []*tacklr.Specialist{{
				Name:  "researcher",
				Model: scriptedComplete("research result"),
			}},
		},
	}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var saw bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "research result") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want custom tool child result, got %v", summarize(got))
	}
}
