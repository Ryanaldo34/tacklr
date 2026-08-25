package inprocess

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/streaming"
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

func waitEvents(t *testing.T, sub durable.Subscription, timeout time.Duration) []streaming.StreamEvent {
	t.Helper()
	deadline := time.After(timeout)
	var got []streaming.StreamEvent
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
			if ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError || ev.Type == streaming.StreamEventInterrupt {
				return got
			}
		case <-deadline:
			t.Fatalf("timeout waiting for events, got %d", len(got))
		}
	}
}

func TestCreateSessionPromptCompletedMessage(t *testing.T) {
	ctx := t.Context()
	model := scriptedComplete("hello from agent")
	rt := New(newCatalog(t, model, durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))

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
	got := waitEvents(t, sub, 5*time.Second)
	var saw bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "hello from agent") {
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
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{
		InvokeFn: workspaceReadModel,
	}
	cat := newCatalog(t, model, durable.AgentSpec{FSRegistry: fsReg})
	rt := New(cat, WithProjection(vfs.DirectProjection{}))
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
	got := waitEvents(t, sub, 8*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace file content, got %q events=%+v", body, summarize(got))
	}
}

func TestUnbindThenPromptWorkspaceGone(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{
		InvokeFn: workspaceReadModel,
	}
	rt := New(newCatalog(t, model, durable.AgentSpec{FSRegistry: fsReg}), WithProjection(vfs.DirectProjection{}))
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
	_ = waitEvents(t, sub, 8*time.Second)
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
	got := waitEvents(t, sub2, 8*time.Second)
	var sawMissing bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventToolResult && (strings.Contains(ev.Content, "not found") ||
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
	rt := New(newCatalog(t, model, durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
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
	got := waitEvents(t, sub, 8*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield, got %+v", summarize(got))
	}

	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := rt.Resume(ctx, id, durable.Resume{Responses: map[string][]byte{"ask1": payload}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(8 * time.Second)
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("subscription closed before complete")
			}
			if ev.Type == streaming.StreamEventComplete {
				return
			}
			if ev.Type == streaming.StreamEventMessage && ev.Content == "chose" {
				return
			}
		case <-deadline:
			t.Fatal("timeout waiting for resume complete")
		}
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
	gate := tacklr.NewTool(tacklr.ToolConfig{
		Name:   "gate",
		OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) {
			return "gate-ok", nil
		},
	})
	rt := New(newCatalog(t, model, durable.AgentSpec{
		Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{
			tacklr.NewTool(tacklr.ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
			gate,
			tacklr.NewTool(tacklr.ToolConfig{Name: "beta", Handler: func(context.Context) (string, error) { return "from-beta", nil }}),
		}},
	}), WithProjection(vfs.DirectProjection{}))
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
	got := waitEvents(t, sub, 8*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
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
	deadline := time.After(8 * time.Second)
	complete := false
	for !complete {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("subscription closed before complete")
			}
			if ev.Type == streaming.StreamEventComplete || (ev.Type == streaming.StreamEventMessage && ev.Content == "all-three") {
				complete = true
			}
			if ev.Type == streaming.StreamEventError {
				t.Fatalf("resume error: %v %s", ev.Error, ev.Content)
			}
		case <-deadline:
			t.Fatal("timeout waiting for resume complete")
		}
	}
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
	rt := New(newCatalog(t, model, durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
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
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}
	if err := rt.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.Events():
		if ok {
			// drain until close
			for range sub.Events() {
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not end after cancel")
	}

	model2 := scriptedComplete("after-cancel")
	// Replace agent model by re-registering.
	cat := rt.Catalog().(*durable.MemoryCatalog)
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
	got := waitEvents(t, sub2, 8*time.Second)
	var saw bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "after-cancel") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want next prompt to run, got %+v", summarize(got))
	}
}

func TestCloseDeletesSnapshotPromptNotFound(t *testing.T) {
	ctx := t.Context()
	rt := New(newCatalog(t, scriptedComplete("x"), durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
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
	_ = waitEvents(t, sub, 5*time.Second)
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
	rt := New(newCatalog(t, model, durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
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
	_ = waitEvents(t, sub, 5*time.Second)
	_ = sub.Close()

	head, _ := rt.events.Head(ctx, id)
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "second"}); err != nil {
		t.Fatal(err)
	}
	sub2, err := rt.Subscribe(ctx, id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	_ = waitEvents(t, sub2, 5*time.Second)

	var sawFirst bool
	for _, m := range model.LastInvokeMsgs {
		if m != nil && m.Role == tacklr.RoleUser && m.Content == "first" {
			sawFirst = true
		}
	}
	if !sawFirst {
		t.Fatalf("second prompt must see first snapshot messages, last=%+v", contents(model.LastInvokeMsgs))
	}
}

func TestNewSessionDoesNotLoadPreviousSnapshot(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	rt := New(newCatalog(t, model, durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
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
	_ = waitEvents(t, sub, 5*time.Second)
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
	_ = waitEvents(t, sub2, 5*time.Second)

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

func summarize(evs []streaming.StreamEvent) []string {
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

func TestSnapshotEtagMismatchRejectsStaleWrite(t *testing.T) {
	ctx := t.Context()
	s := NewMemorySnapshot()
	id := durable.SessionID("s1")
	etag, err := s.Save(ctx, id, durable.Snapshot{AgentID: "a"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Save(ctx, id, durable.Snapshot{AgentID: "stale"}, "not-"+etag); !errors.Is(err, durable.ErrEtagMismatch) {
		t.Fatalf("want etag mismatch, got %v", err)
	}
	newEtag, err := s.Save(ctx, id, durable.Snapshot{AgentID: "b"}, etag)
	if err != nil {
		t.Fatal(err)
	}
	got, loaded, err := s.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != newEtag || got.AgentID != "b" {
		t.Fatalf("load=%+v etag=%s want b/%s", got, loaded, newEtag)
	}
}

func TestSubscribeReplaysPastBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	log := NewMemoryEventLog()
	id := durable.SessionID("busy")
	for i := 0; i < 80; i++ {
		if err := log.Append(ctx, id, durable.TopicEvents, streaming.StreamEvent{
			Type:    streaming.StreamEventMessage,
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
	deadline := time.After(5 * time.Second)
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
		case <-deadline:
			t.Fatalf("timeout after %d events", n)
		}
	}
}

func TestInferenceErrorPublishesStreamEventError(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{InvokeErr: errors.New("model down")}
	rt := New(newCatalog(t, model, durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
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
	got := waitEvents(t, sub, 5*time.Second)
	var sawErr bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventError {
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
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
	rt := New(newCatalog(t, &testkit.ScriptedModel{InvokeFn: workspaceReadModel}, durable.AgentSpec{FSRegistry: fsReg}), WithProjection(vfs.DirectProjection{}))
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
	got := waitEvents(t, sub, 8*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace from cached recipe + prompt token, got %q events=%+v", body, summarize(got))
	}
}

func TestBadWorkspaceBindingFailsTurn(t *testing.T) {
	ctx := t.Context()
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: filepath.Join(t.TempDir(), "missing")}); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{InvokeFn: workspaceReadModel}
	rt := New(newCatalog(t, model, durable.AgentSpec{FSRegistry: fsReg}), WithProjection(vfs.DirectProjection{}))
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
	got := waitEvents(t, sub, 8*time.Second)
	var sawErr bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventError || ev.Error != nil {
			sawErr = true
		}
		if ev.Type == streaming.StreamEventToolResult && (strings.Contains(ev.Content, "not found") ||
			strings.Contains(ev.Content, "not a registered tool") || strings.Contains(ev.Content, "does not exist")) {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("want turn error for missing workspace root, got %+v", summarize(got))
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
					ID: "sp1", CallID: "sp1", Name: "spawn_worker",
					Arguments: `{"worker_name":"researcher","task_description_and_context":"do it"}`,
				}},
				IsComplete: true,
			}
		},
	}
	spec := durable.AgentSpec{
		Options: tacklr.AgentOptions{
			SubAgents: []*tacklr.SubAgent{{
				WorkerName:   "researcher",
				Model:        child,
				Instructions: "research",
			}},
		},
	}
	rt := New(newCatalog(t, model, spec), WithProjection(vfs.DirectProjection{}))
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
	deadline := time.After(8 * time.Second)
	var sawChild, sawParent bool
	for !sawChild || !sawParent {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("closed child=%v parent=%v", sawChild, sawParent)
			}
			if ev.Type == streaming.StreamEventToolResult && strings.Contains(ev.Content, "child-hello") {
				sawChild = true
			}
			if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn") {
				sawParent = true
			}
			if ev.Type == streaming.StreamEventComplete && sawChild && sawParent {
				return
			}
		case <-deadline:
			t.Fatalf("timeout child=%v parent=%v", sawChild, sawParent)
		}
	}
}
