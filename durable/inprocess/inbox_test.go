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
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestPrompt_steerDuringInference(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var n atomic.Int32
	var sawAssistant, sawSteer, canceled atomic.Bool
	model := &testkit.ScriptedModel{InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if ctx.Err() != nil {
			canceled.Store(true)
			return
		}
		i := n.Add(1)
		if i == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				canceled.Store(true)
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "first-round", IsComplete: true}
			return
		}
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleAssistant && m.Content == "first-round" {
				sawAssistant.Store(true)
			}
			if m.Role == tacklr.RoleUser && m.Content == "steer" {
				sawSteer.Store(true)
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
	}}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("inference did not start")
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "steer"}); err != nil {
		t.Fatal(err)
	}
	sub2 := subscribe(t, rt, id)
	close(release)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	got2 := waitEvents(t, rt, id, sub2, 8*time.Second)
	if canceled.Load() {
		t.Fatalf("steer cancelled in-flight Invoke: %v", summarize(got))
	}
	assertSteerTurnComplete(t, got, "first Subscribe")
	assertSteerTurnComplete(t, got2, "second Subscribe")
	if !sawAssistant.Load() || !sawSteer.Load() {
		t.Fatalf("assistant=%v steer=%v events=%v", sawAssistant.Load(), sawSteer.Load(), summarize(got))
	}
}

func assertSteerTurnComplete(t *testing.T, got []tacklr.StreamEvent, who string) {
	t.Helper()
	var sawFirst, sawSecond, sawComplete bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventError && (errors.Is(ev.Error, context.Canceled) || strings.Contains(fmtErr(ev), "canceled")) {
			t.Fatalf("%s stream has context.Canceled from the steer: %v", who, summarize(got))
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "first-round" {
			sawFirst = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "second-round" {
			sawSecond = true
		}
		if ev.Type == tacklr.StreamEventComplete {
			sawComplete = true
		}
	}
	if !sawFirst || !sawSecond || !sawComplete {
		t.Fatalf("%s first=%v second=%v complete=%v events=%v", who, sawFirst, sawSecond, sawComplete, summarize(got))
	}
}

func TestPrompt_steerDuringBlockingTool(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var n atomic.Int32
	var order []string
	var sawCancelled atomic.Bool
	model := &testkit.ScriptedModel{InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		i := n.Add(1)
		if i == 1 {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "slow1", CallID: "slow1", Name: "slow", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		var seq []string
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleAssistant && len(m.ToolCalls) > 0 {
				seq = append(seq, "call")
			}
			if m.Role == tacklr.RoleTool && m.Content == "real-output" {
				seq = append(seq, "result")
			}
			if m.Role == tacklr.RoleUser && m.Content == "steer" {
				seq = append(seq, "steer")
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, tacklr.CancelledToolResultContent) {
				sawCancelled.Store(true)
			}
		}
		if i == 2 {
			order = seq
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-tool", IsComplete: true}
	}}
	slow := tacklr.NewTool(tacklr.ToolConfig{
		Name: "slow",
		Handler: func(ctx context.Context) (string, error) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			return "real-output", nil
		},
	})
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{slow}}}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool did not start")
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "steer"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	if sawCancelled.Load() {
		t.Fatalf("steer replaced tool output with cancelled: %v", summarize(got))
	}
	joined := strings.Join(order, ",")
	if !strings.Contains(joined, "call,result,steer") {
		t.Fatalf("want assistant function_call, RoleTool real-output, RoleUser steer, got %s events=%v", joined, summarize(got))
	}
}

func TestPrompt_cancelDuringInferenceDropsUnreadSteer(t *testing.T) {
	started := make(chan struct{})
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "slow1", CallID: "slow1", Name: "slow", Arguments: `{}`,
				}},
				IsComplete: true,
			}
		},
	}
	slow := tacklr.NewTool(tacklr.ToolConfig{
		Name: "slow",
		Handler: func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		},
	})
	var after atomic.Int32
	var sawCancelled, sawAgain atomic.Bool
	var afterUsers []string
	afterModel := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			after.Add(1)
			var users []string
			inAfter := false
			for _, m := range msgs {
				if m == nil {
					continue
				}
				if m.Role == tacklr.RoleTool && m.ToolCallID == "slow1" && strings.Contains(strings.ToLower(m.Content), "cancel") {
					sawCancelled.Store(true)
					inAfter = true
					continue
				}
				if inAfter && m.Role == tacklr.RoleUser {
					users = append(users, m.Content)
				}
				if m.Role == tacklr.RoleUser && m.Content == "again" {
					sawAgain.Store(true)
				}
			}
			afterUsers = users
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-cancel", IsComplete: true}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{slow}}}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "slow"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool did not start")
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "steer"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	cat := rt.catalog.(*durable.MemoryCatalog)
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: afterModel, Config: tacklr.Config{MaxWindowSize: 8192}, Tools: []*tacklr.Tool{slow}},
	})
	head, _ := rt.events.Head(t.Context(), id)
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(t.Context(), id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	if !sawCancelled.Load() {
		t.Fatalf("next Prompt must pair cancelled leftover work: %v", summarize(got))
	}
	if !sawAgain.Load() || len(afterUsers) != 1 || afterUsers[0] != "again" {
		t.Fatalf("want user [again] after cancelled tool, got %v events=%v", afterUsers, summarize(got))
	}
}

func TestPrompt_steerUnknownAgentKeepsLiveTurn(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var n atomic.Int32
	var canceled, sawSteer atomic.Bool
	model := &testkit.ScriptedModel{InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if ctx.Err() != nil {
			canceled.Store(true)
			return
		}
		i := n.Add(1)
		if i == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				canceled.Store(true)
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "first-round", IsComplete: true}
			return
		}
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleUser && m.Content == "steer" {
				sawSteer.Store(true)
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
	}}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("inference did not start")
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{AgentID: "ghost", Text: "steer"}); !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("ghost agent: %v", err)
	}
	close(release)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	if canceled.Load() {
		t.Fatalf("unknown AgentID aborted the live turn: %v", summarize(got))
	}
	var sawFirst, sawSecond bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventError && (errors.Is(ev.Error, context.Canceled) || strings.Contains(fmtErr(ev), "canceled")) {
			t.Fatalf("stream has context.Canceled: %v", summarize(got))
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "first-round" {
			sawFirst = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "second-round" {
			sawSecond = true
		}
	}
	if !sawFirst || sawSecond || sawSteer.Load() {
		t.Fatalf("ghost steer must not enter the window first=%v second=%v steer=%v events=%v", sawFirst, sawSecond, sawSteer.Load(), summarize(got))
	}
}

func TestPrompt_queuedAgentIDAppliesOnIdleConstruct(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var defaultN, otherN atomic.Int32
	var liveSawOtherTool, idleSawOtherTool, sawSteer atomic.Bool
	otherTool := tacklr.NewTool(tacklr.ToolConfig{
		Name:    "other_only",
		Handler: func(ctx context.Context) (string, error) { return "x", nil },
	})
	defaultModel := &testkit.ScriptedModel{InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		i := defaultN.Add(1)
		for _, tool := range tools {
			if tool != nil && tool.Name() == "other_only" {
				liveSawOtherTool.Store(true)
			}
		}
		if i == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "first-round", IsComplete: true}
			return
		}
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleUser && m.Content == "steer" {
				sawSteer.Store(true)
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
	}}
	otherModel := &testkit.ScriptedModel{InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		otherN.Add(1)
		for _, tool := range tools {
			if tool != nil && tool.Name() == "other_only" {
				idleSawOtherTool.Store(true)
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "from-other", IsComplete: true}
	}}
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: defaultModel, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	cat.Register("other", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: otherModel, Config: tacklr.Config{MaxWindowSize: 8192}, Tools: []*tacklr.Tool{otherTool}},
	})
	rt := New(Config{Catalog: cat, Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("inference did not start")
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{
		AgentID:    "other",
		MCPServers: []mcp.MCPConfig{},
		Text:       "steer",
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	got := waitEvents(t, rt, id, sub, 8*time.Second)
	if liveSawOtherTool.Load() {
		t.Fatalf("live turn used the queued agent: %v", summarize(got))
	}
	if !sawSteer.Load() {
		t.Fatalf("steer must still land on the live agent: %v", summarize(got))
	}
	head, _ := rt.events.Head(t.Context(), id)
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "next"}); err != nil {
		t.Fatal(err)
	}
	sub2, err := rt.Subscribe(t.Context(), id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	got2 := waitEvents(t, rt, id, sub2, 8*time.Second)
	var sawOther bool
	for _, ev := range got2 {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "from-other" {
			sawOther = true
		}
	}
	if !sawOther || !idleSawOtherTool.Load() || otherN.Load() == 0 {
		t.Fatalf("idle Prompt must construct queued agent other=%v tool=%v invokes=%d events=%v", sawOther, idleSawOtherTool.Load(), otherN.Load(), summarize(got2))
	}
}

func TestPrompt_steerDuringHITLKeepsPark(t *testing.T) {
	var n atomic.Int32
	var sawSteerAfterResume atomic.Bool
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			i := n.Add(1)
			if i == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
						Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
					}},
					IsComplete: true,
				}
				return
			}
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser && m.Content == "steer" {
					sawSteerAfterResume.Store(true)
				}
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "chose", IsComplete: true}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{}), Snapshots: NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "ask"}); err != nil {
		t.Fatal(err)
	}
	sub := subscribe(t, rt, id)
	waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventInterrupt
	})
	st, err := rt.Status(t.Context(), id)
	if err != nil || !st.Waiting {
		t.Fatalf("want parked: %+v %v", st, err)
	}
	if err := rt.Prompt(t.Context(), id, durable.Prompt{Text: "steer"}); err != nil {
		t.Fatal(err)
	}
	st, err = rt.Status(t.Context(), id)
	if err != nil || !st.Waiting || st.State != durable.SessionRunning {
		t.Fatalf("steer must keep the park: %+v %v", st, err)
	}
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := rt.Resume(t.Context(), id, durable.Resume{Responses: map[string][]byte{"ask1": payload}}); err != nil {
		t.Fatal(err)
	}
	_ = waitEvents(t, rt, id, sub, 8*time.Second)
	if !sawSteerAfterResume.Load() {
		t.Fatal("steer must be absorbed after Resume leftover tools")
	}
}

func fmtErr(ev tacklr.StreamEvent) string {
	if ev.Error != nil {
		return ev.Error.Error()
	}
	return ev.Content
}
