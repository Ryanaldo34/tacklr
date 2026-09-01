package inprocess

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/durtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func scriptedComplete(content string) *testkit.ScriptedModel {
	return &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventMessage,
				Content:    content,
				IsComplete: true,
			}
		},
	}
}

func newCatalog(t *testing.T, model tacklr.InferenceStrategy, extra durable.AgentSpec) *durable.MemoryCatalog {
	t.Helper()
	cat := durable.NewCatalog("default")
	spec := extra
	if spec.Options.Model == nil {
		spec.Options.Model = model
	}
	if spec.Options.Config.MaxWindowSize == 0 {
		spec.Options.Config.MaxWindowSize = 8192
	}
	cat.Register("default", spec)
	return cat
}

func waitTurnPersisted(t *testing.T, rt *Runtime, id durable.SessionID) {
	t.Helper()
	p, err := rt.get(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.waitPriorTurn(t.Context(), p, false); err != nil {
		t.Fatal(err)
	}
}

func waitEvents(t *testing.T, rt *Runtime, id durable.SessionID, sub durable.Subscription, timeout time.Duration) []tacklr.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var got []tacklr.StreamEvent
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
			if ev.Type == tacklr.StreamEventComplete || ev.Type == tacklr.StreamEventError || ev.Type == tacklr.StreamEventInterrupt {
				durtest.AssertStatusMatchesEvent(t, rt, id, ev)
				return got
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for events, got %d", len(got))
		}
	}
}

func TestCreateSessionPromptCompletedMessage(t *testing.T) {
	ctx := t.Context()
	model := scriptedComplete("hello from agent")
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 5*time.Second)
	var saw bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "hello from agent") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want completed message, got %+v", got)
	}
}

func TestBindThenPromptReadsWorkspace(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{
		InvokeFn: workspaceReadModel,
	}
	cat := newCatalog(t, model, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir)))})
	rt := New(Config{Catalog: cat, Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{
		Text: "read it",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Params:   map[string]string{vfs.ParamName: "docs"},
			Auth:     vfs.Credential{Token: "x"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace file content, got %q events=%+v", body, summarize(got))
	}
}

func TestPrompt_readSkillFromOpenSkills(t *testing.T) {
	ctx := t.Context()
	pack := t.TempDir()
	d := filepath.Join(pack, "research")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte("---\nname: research\ndescription: Research carefully\n---\n\nAlways verify claims.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
				return
			}
			id := "skill-" + itoa(len(msgs))
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: id, CallID: id, Name: "read_skill",
					Arguments: `{"name":"research"}`,
				}},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{
		OpenVFS:    vfs.Tree(vfs.At("work", builtins.Local(t.TempDir()))),
		OpenSkills: vfs.Tree(vfs.At("skills", builtins.Local(pack))),
	}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "use research"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "Always verify claims") {
		t.Fatalf("want skill instructions, got %q events=%+v", body, summarize(got))
	}
}

func TestUnbindThenPromptWorkspaceGone(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{
		InvokeFn: workspaceReadModel,
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{OpenVFS: bindLocalDocs(dir)}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	bind := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "x"},
	}}}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "first", Auth: bind}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitEvents(t, rt, id, sub, 8*time.Second)
	_ = sub.Close()

	head, _ := rt.events.Head(ctx, id)
	if err := rt.Prompt(ctx, id, durable.Prompt{
		Text: "second",
		Auth: durable.AuthContext{Drop: []string{"docs"}},
	}); err != nil {
		t.Fatal(err)
	}
	sub2, err := rt.Subscribe(ctx, id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	got := waitEvents(t, rt, id, sub2, 8*time.Second)
	var sawMissing bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && (strings.Contains(ev.Content, "not found") ||
			strings.Contains(ev.Content, "not a registered tool") || strings.Contains(ev.Content, "does not exist")) {
			sawMissing = true
		}
		if ev.Error != nil && (strings.Contains(ev.Error.Error(), "not found") ||
			strings.Contains(ev.Error.Error(), "not a registered tool") || strings.Contains(ev.Error.Error(), "does not exist")) {
			sawMissing = true
		}
	}
	if !sawMissing {
		t.Fatalf("want workspace gone after unbind, got %+v", summarize(got))
	}
}

func TestAskUserChoiceYieldsThenResumeCompletes(t *testing.T) {
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
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "ask"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == tacklr.StreamEventToolResult {
			t.Fatalf("park yielded a tool_result: %+v", summarize(got))
		}
	}
	if !yielded {
		t.Fatalf("want yield, got %+v", summarize(got))
	}

	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := rt.Resume(ctx, id, durable.Resume{
		Responses: map[string][]byte{"ask1": payload},
		State:     map[string]any{"user": "Ryan"},
	}); err != nil {
		t.Fatal(err)
	}
	waitEvents(t, rt, id, sub, 8*time.Second)
	waitTurnPersisted(t, rt, id)
	snap, _, err := rt.snapshots.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Checkpoint.UserState()["user"]) != `"Ryan"` {
		t.Fatalf("resume state: %s", snap.Checkpoint.UserState()["user"])
	}
}

// TestPrompt_parallelBatchHitlRunsRemainder: durable driver used to abort the
// rest of a model tool batch when one call parked. Next inference then sent a
// function_call without function_call_output (Azure 400). Resume must still
// run the leftover tools so the following model turn sees every result.
func TestPrompt_parallelBatchHitlRunsRemainder(t *testing.T) {
	ctx := t.Context()
	var (
		invokes int
		results []string
	)
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokes++
			if invokes == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{
						{ID: "fc_alpha", CallID: "call_alpha", Name: "alpha", Arguments: `{}`},
						{ID: "fc_gate", CallID: "call_gate", Name: "gate", Arguments: `{}`},
						{ID: "fc_beta", CallID: "call_beta", Name: "beta", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleTool {
					results = append(results, m.Content)
				}
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "all-three", IsComplete: true}
		},
	}
	slow := func(out string) func(context.Context) (string, error) {
		return func(ctx context.Context) (string, error) {
			// Keep leftover calls in-flight when gate parks so Resume must wait,
			// not cancel the batch (that used to emit context.Canceled).
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(25 * time.Millisecond):
				return out, nil
			}
		}
	}
	gate := tacklr.NewTool(tacklr.ToolConfig{
		Name:   "gate",
		OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) {
			return "gate-ok", nil
		},
	})
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{
		Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{
			tacklr.NewTool(tacklr.ToolConfig{Name: "alpha", Handler: slow("from-alpha")}),
			gate,
			tacklr.NewTool(tacklr.ToolConfig{Name: "beta", Handler: slow("from-beta")}),
		}},
	}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "batch"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want HITL yield, got %+v", summarize(got))
	}
	payload, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	if err := rt.Resume(ctx, id, durable.Resume{Responses: map[string][]byte{"fc_gate": payload}}); err != nil {
		t.Fatal(err)
	}
	waitEvents(t, rt, id, sub, 8*time.Second)
	saw := map[string]bool{}
	for _, c := range results {
		saw[c] = true
	}
	if !saw["from-alpha"] || !saw["gate-ok"] || !saw["from-beta"] {
		t.Fatalf("next model turn missing leftover tool results: %v", results)
	}
}

func TestCancelWhileRunningEndsSubscriptionThenNextPromptRuns(t *testing.T) {
	ctx := t.Context()
	started := make(chan struct{}, 1)
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "slow"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	waitStart, stopWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopWait()
	select {
	case <-started:
	case <-waitStart.Done():
		t.Fatal("model did not start")
	}
	if err := rt.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	_ = sub.Close()
	subCancel, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subCancel.Close() })
	gotCancel := waitEvents(t, rt, id, subCancel, 5*time.Second)
	var sawCancel bool
	for _, ev := range gotCancel {
		if ev.Type == tacklr.StreamEventError && (errors.Is(ev.Error, context.Canceled) ||
			strings.Contains(ev.Content, "canceled") ||
			strings.Contains(fmt.Sprint(ev.Error), "canceled")) {
			sawCancel = true
		}
	}
	if !sawCancel {
		t.Fatalf("want canceled stream error, got %+v", summarize(gotCancel))
	}

	model2 := scriptedComplete("after-cancel")
	// Replace agent model by re-registering.
	cat := rt.catalog.(*durable.MemoryCatalog)
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model2, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	head, _ := rt.events.Head(ctx, id)
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	sub2, err := rt.Subscribe(ctx, id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	got := waitEvents(t, rt, id, sub2, 8*time.Second)
	var saw bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-cancel") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want next prompt to run, got %+v", summarize(got))
	}
}

func TestCloseDeletesSnapshotPromptNotFound(t *testing.T) {
	ctx := t.Context()
	rt := New(Config{Catalog: newCatalog(t, scriptedComplete("x"), durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitEvents(t, rt, id, sub, 5*time.Second)
	if err := rt.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	_, _, err = rt.snapshots.Load(ctx, id)
	if !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("want snapshot gone, got %v", err)
	}
	err = rt.Prompt(ctx, id, durable.Prompt{Text: "again"})
	if !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("want session-not-found, got %v", err)
	}
}

func TestTwoPromptsShareSnapshot(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{
		AgentID: "default",
		State:   map[string]any{"user": "Ryan"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "first"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitEvents(t, rt, id, sub, 5*time.Second)
	_ = sub.Close()

	head, _ := rt.events.Head(ctx, id)
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "second", State: map[string]any{"company": "Acme"}}); err != nil {
		t.Fatal(err)
	}
	sub2, err := rt.Subscribe(ctx, id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	_ = waitEvents(t, rt, id, sub2, 5*time.Second)

	var sawFirst bool
	for _, m := range model.LastInvokeMsgs {
		if m != nil && m.Role == tacklr.RoleUser && m.Content == "first" {
			sawFirst = true
		}
	}
	if !sawFirst {
		t.Fatalf("second prompt must see first snapshot messages, last=%+v", contents(model.LastInvokeMsgs))
	}
	waitTurnPersisted(t, rt, id)
	snap, _, err := rt.snapshots.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	us := snap.Checkpoint.UserState()
	if string(us["user"]) != `"Ryan"` || string(us["company"]) != `"Acme"` {
		t.Fatalf("prompt state: user=%s company=%s", us["user"], us["company"])
	}
}

func TestNewSessionDoesNotLoadPreviousSnapshot(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id1, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id1, durable.Prompt{Text: "secret-session-one"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id1, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitEvents(t, rt, id1, sub, 5*time.Second)
	_ = sub.Close()

	id2, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id2, durable.Prompt{Text: "session-two"}); err != nil {
		t.Fatal(err)
	}
	sub2, err := rt.Subscribe(ctx, id2, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	_ = waitEvents(t, rt, id2, sub2, 5*time.Second)

	var users []string
	for _, m := range model.LastInvokeMsgs {
		if m != nil && m.Role == tacklr.RoleUser {
			users = append(users, m.Content)
		}
	}
	if len(users) != 1 || users[0] != "session-two" {
		t.Fatalf("want window [session-two], got %v", users)
	}
}

func TestPrompt_readMissingPathCorrection(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "r1", CallID: "r1", Name: "read",
					Arguments: `{"path":"/workspace/docs/missing.txt"}`,
				}},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir)))}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{
		Text: "read missing",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Params:   map[string]string{vfs.ParamName: "docs"},
			Auth:     vfs.Credential{Token: "x"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var sentence string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "that path does not exist. List the parent") {
			sentence = ev.Content
		}
	}
	if sentence == "" {
		t.Fatalf("want not-exist correction tool_result, got %+v", summarize(got))
	}
}

func TestPrompt_toolFailedServiceString(t *testing.T) {
	ctx := t.Context()
	boom := tacklr.NewTool(tacklr.ToolConfig{
		Name: "boom",
		Handler: func(context.Context) (string, error) {
			return "", fmt.Errorf("upstream: the search provider failed: %w", tacklr.ErrFailed)
		},
	})
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "b1", CallID: "b1", Name: "boom", Arguments: `{}`,
				}},
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{
		Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{boom}},
	}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "boom"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var sentence string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "search provider failed") {
			sentence = ev.Content
		}
	}
	if sentence == "" {
		t.Fatalf("want service-failed tool_result, got %+v", summarize(got))
	}
}

func bindLocalDocs(dir string) vfs.OpenVFS {
	return func(ctx context.Context, sid string, req vfs.Request) (*vfs.MountSession, error) {
		if _, ok := vfs.BindingByName(req.Bindings, "docs"); !ok {
			if _, ok := vfs.BindingByName(req.Bindings, "local"); !ok {
				return vfs.Tree()(ctx, sid, req)
			}
		}
		return vfs.Tree(vfs.At("docs", builtins.Local(dir)))(ctx, sid, req)
	}
}

func workspaceReadModel(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
	if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
		return
	}
	id := "read-" + itoa(len(msgs))
	ch <- tacklr.LLMResponseChunk{
		Type: tacklr.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{{
			ID: id, CallID: id, Name: "read",
			Arguments: `{"path":"/workspace/docs/hello.txt"}`,
		}},
		IsComplete: true,
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func lastMsg(msgs []*tacklr.Message) *tacklr.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil {
			return msgs[i]
		}
	}
	return nil
}

func summarize(evs []tacklr.StreamEvent) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		out = append(out, string(ev.Type)+":"+ev.Content)
	}
	return out
}

func contents(msgs []*tacklr.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}
		out = append(out, string(m.Role)+":"+m.Content)
	}
	return out
}

func TestSnapshotStore_concurrentFirstSaveOneWins(t *testing.T) {
	ctx := t.Context()
	s := NewMemorySnapshot()
	id := durable.SessionID("create")
	const n = 32
	var wins, stale atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_, err := s.Save(ctx, id, durable.Snapshot{AgentID: strconv.Itoa(i)}, "")
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, durable.ErrStaleCheckpoint):
				stale.Add(1)
			default:
				t.Errorf("save: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 || wins.Load()+stale.Load() != n {
		t.Fatalf("wins=%d stale=%d want 1 winner of %d", wins.Load(), stale.Load(), n)
	}
	got, _, err := s.Load(ctx, id)
	if err != nil || got.AgentID == "" {
		t.Fatalf("stored %+v err=%v", got, err)
	}
}

func TestSnapshotStore_concurrentUpdateOneWins(t *testing.T) {
	ctx := t.Context()
	s := NewMemorySnapshot()
	id := durable.SessionID("update")
	if _, err := s.Save(ctx, id, durable.Snapshot{AgentID: "init"}, ""); err != nil {
		t.Fatal(err)
	}
	_, expected, err := s.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	const n = 32
	var wins, stale atomic.Int64
	var ready, wg sync.WaitGroup
	ready.Add(1)
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			ready.Wait()
			_, err := s.Save(ctx, id, durable.Snapshot{AgentID: strconv.Itoa(i)}, expected)
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, durable.ErrStaleCheckpoint):
				stale.Add(1)
			default:
				t.Errorf("save: %v", err)
			}
		}(i)
	}
	ready.Done()
	wg.Wait()
	if wins.Load() != 1 || wins.Load()+stale.Load() != n {
		t.Fatalf("wins=%d stale=%d want 1 winner of %d", wins.Load(), stale.Load(), n)
	}
	got, _, err := s.Load(ctx, id)
	if err != nil || got.AgentID == "init" {
		t.Fatalf("stored %+v err=%v", got, err)
	}
}

func TestSubscribeReplaysPastBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	log := NewMemoryEventLog()
	id := durable.SessionID("busy")
	for i := 0; i < 80; i++ {
		if err := log.Append(ctx, id, durable.TopicEvents, tacklr.StreamEvent{
			Type:    tacklr.StreamEventMessage,
			Content: strconv.Itoa(i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	ch, err := log.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	wait, stopWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopWait()
	for n < 80 {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("closed after %d events", n)
			}
			if ev.Content != strconv.Itoa(n) {
				t.Fatalf("event %d content=%q", n, ev.Content)
			}
			n++
		case <-wait.Done():
			t.Fatalf("timeout after %d events", n)
		}
	}
}

func TestInferenceErrorPublishesStreamEventError(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{InvokeErr: errors.New("model down")}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 5*time.Second)
	var sawErr bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventError {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("want StreamEventError, got %+v", summarize(got))
	}
}

func TestCreateSessionMountsThenPromptTokensRemount(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	rt := New(Config{Catalog: newCatalog(t, &testkit.ScriptedModel{InvokeFn: workspaceReadModel}, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir)))}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{
		AgentID: "default",
		Mounts: []durable.MountRecipe{{
			Provider: "local",
			Alias:    "docs",
			Params:   map[string]string{vfs.ParamName: "docs"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{
		Text: "read",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Auth:     vfs.Credential{Token: "x"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace from cached recipe + prompt token, got %q events=%+v", body, summarize(got))
	}
}

func TestBadWorkspaceBindingFailsTurn(t *testing.T) {
	ctx := t.Context()
	missing := filepath.Join(t.TempDir(), "missing")
	model := &testkit.ScriptedModel{InvokeFn: workspaceReadModel}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(missing)))}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{
		Text: "read",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Params:   map[string]string{vfs.ParamName: "docs"},
			Auth:     vfs.Credential{Token: "x"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	var sawErr bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventError || ev.Error != nil {
			sawErr = true
		}
		if ev.Type == tacklr.StreamEventToolResult && (strings.Contains(ev.Content, "not found") ||
			strings.Contains(ev.Content, "not a registered tool") || strings.Contains(ev.Content, "does not exist")) {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("want turn error for missing workspace root, got %+v", summarize(got))
	}
}

func TestChildSession_inheritsAndStatusStaysRunning(t *testing.T) {
	ctx := t.Context()
	childModel := scriptedComplete("from-child")
	cat := newCatalog(t, scriptedComplete("parent"), durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Specialists: []*tacklr.Specialist{{
				Name:         "researcher",
				Model:        childModel,
				Instructions: "research",
			}},
		},
	})
	rt := New(Config{Catalog: cat, Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	parent, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	kids, err := rt.Children(ctx, parent)
	if err != nil || len(kids) != 0 {
		t.Fatalf("empty children: %v %v", kids, err)
	}
	child, err := rt.CreateSession(ctx, durable.CreateSession{
		Parent:     parent,
		Specialist: "researcher",
		SessionID:  durable.ChildSessionID(parent, "researcher", "c1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	kids, err = rt.Children(ctx, parent)
	if err != nil || len(kids) != 1 || kids[0] != child {
		t.Fatalf("children=%v err=%v", kids, err)
	}
	st, err := rt.Status(ctx, child)
	if err != nil || st.State != durable.SessionRunning || st.Kind != durable.SessionKindSpecialist {
		t.Fatalf("status=%+v err=%v", st, err)
	}
	if err := rt.Prompt(ctx, child, durable.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, child, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	waitEvents(t, rt, child, sub, 8*time.Second)
	st = waitStatus(t, rt, child, func(s durable.SessionStatus) bool {
		return s.State == durable.SessionComplete
	})
	if st.Result != "from-child" {
		t.Fatalf("complete=%+v", st)
	}
	if err := rt.Close(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Status(ctx, child); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("close parent should close child: %v", err)
	}
}

func TestSpawnWorkerNestedDriverCompletes(t *testing.T) {
	ctx := t.Context()
	child := scriptedComplete("child-hello")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-after-spawn", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"do it"}`,
				}},
				IsComplete: true,
			}
		},
	}
	spec := durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Specialists: []*tacklr.Specialist{{
				Name:         "researcher",
				Model:        child,
				Instructions: "research",
			}},
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, spec), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	wait, stopWait := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopWait()
	var sawChild, sawParent bool
	for !sawChild || !sawParent {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("closed child=%v parent=%v", sawChild, sawParent)
			}
			if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "child-hello") {
				sawChild = true
			}
			if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn") {
				sawParent = true
			}
			if ev.Type == tacklr.StreamEventComplete && sawChild && sawParent {
				durtest.AssertStatusMatchesEvent(t, rt, id, ev)
				return
			}
		case <-wait.Done():
			t.Fatalf("timeout child=%v parent=%v", sawChild, sawParent)
		}
	}
}
