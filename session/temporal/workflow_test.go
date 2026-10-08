package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"path/filepath"
	"sync"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestSessionWorkflow_inferenceRefusedFailsTurn(t *testing.T) {
	env := newTestWorkflow(t)
	var attempts atomic.Int32
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		attempts.Add(1)
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: tacklr.ErrModelRefused}
	}),
		MaxWindowSize: 8192}
	_, fallback := registerSession(env, agent)

	id := session.SessionID("sess-model-refused")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, ActivityAttempts: 3})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("refusal attempts = %d, want 3", n)
	}
	st := querySession(t, env)
	if st.State != session.SessionFailed || st.Waiting {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventError && (errors.Is(ev.Error, tacklr.ErrModelRefused) ||
			strings.Contains(ev.Content, tacklr.ErrModelRefused.Error()) ||
			strings.Contains(ev.Fail, tacklr.ErrModelRefused.Error())) {
			saw = true
		}
	}
	if !saw {
		t.Fatal("want StreamEventError with model refused")
	}
}

func TestSessionWorkflow_permanentInferenceDoesNotRetry(t *testing.T) {
	env := newTestWorkflow(t)
	var attempts atomic.Int32
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		attempts.Add(1)
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: tacklr.ErrApiKeyNotSet, Content: tacklr.ErrApiKeyNotSet.Error()}
	}),
		MaxWindowSize: 8192}
	_, fallback := registerSession(env, agent)

	id := session.SessionID("sess-permanent")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, ActivityAttempts: 5})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("permanent attempts = %d, want 1", n)
	}
	st := querySession(t, env)
	if st.State != session.SessionFailed || st.Waiting {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventError && strings.Contains(ev.Fail, tacklr.ErrApiKeyNotSet.Error()) {
			saw = true
			if strings.Count(ev.Fail, tacklr.ErrApiKeyNotSet.Error()) != 1 {
				t.Fatalf("failure text repeated: %q", ev.Fail)
			}
		}
	}
	if !saw {
		t.Fatal("want StreamEventError with api key not set")
	}
}

func TestSessionWorkflow_activityRetryThenCompletes(t *testing.T) {
	env := newTestWorkflow(t)
	var attempts atomic.Int32
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if attempts.Add(1) == 1 {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: tacklr.Network(errors.New("transient"))}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-retry", IsComplete: true}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	fallback := &retryLog{EventLog: session.NewMemoryEventLog()}
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := session.SessionID("sess-retry")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fallback.retry) == 0 || fallback.retry[0].Content != "retry" {
		t.Fatalf("want retry event, got %+v", fallback.retry)
	}
	got := drainLog(t, fallback, id)
	var sawMsg bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-retry") {
			sawMsg = true
		}
	}
	if !sawMsg {
		t.Fatalf("want after-retry, got %+v", got)
	}
	if st := querySession(t, env); st.State != session.SessionComplete {
		t.Fatalf("Status after retry: %+v", st)
	}
}

func TestSessionWorkflow_authExpiredYieldThenResume(t *testing.T) {
	env := newTestWorkflow(t)
	var calls atomic.Int32
	cloud := tacklr.NewTool(tacklr.ToolConfig{
		Name: "cloud_read",
		Handler: func(context.Context) (string, error) {
			if calls.Add(1) == 1 {
				return "", fmt.Errorf("gdrive: %w", vfs.ErrAuthExpired)
			}
			return "from-cloud", nil
		},
	})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "c1", CallID: "c1", Name: "cloud_read", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{cloud}}
	_, fallback := registerSession(env, agent)

	id := session.SessionID("sess-auth")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "read"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalResume, session.ResumeIn{Responses: map[string][]byte{"c1": []byte(`{}`)}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, saw bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "from-cloud") {
			saw = true
		}
	}
	if !yielded || !saw {
		t.Fatalf("want yield + retried read, got %+v", got)
	}
	if st := querySession(t, env); st.State != session.SessionComplete {
		t.Fatalf("Status after auth resume: %+v", st)
	}
}

func TestSessionWorkflow_parallelBatchHitlRunsRemainder(t *testing.T) {
	env := newTestWorkflow(t)
	var (
		invokes int
		results []string
	)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
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
	})
	agent := tacklr.AgentOptions{MaxWindowSize: 8192,
		Model: model,
		Tools: []*tacklr.Tool{
			tacklr.NewTool(tacklr.ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
			tacklr.NewTool(tacklr.ToolConfig{
				Name:    "gate",
				OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
				Handler: func(context.Context) (string, error) { return "gate-ok", nil },
			}),
			tacklr.NewTool(tacklr.ToolConfig{Name: "beta", Handler: func(context.Context) (string, error) { return "from-beta", nil }}),
		}}
	registerSession(env, agent)

	id := session.SessionID("sess-parallel-hitl")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "batch"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
		env.SignalWorkflow(signalResume, session.ResumeIn{Responses: map[string][]byte{"fc_gate": payload}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	saw := map[string]bool{}
	for _, c := range results {
		saw[c] = true
	}
	if !saw["from-alpha"] || !saw["gate-ok"] || !saw["from-beta"] {
		t.Fatalf("next model turn missing leftover tool results: %v", results)
	}
}

func TestSessionWorkflow_hitlCancel(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
				Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	_, fallback := registerSession(env, agent)

	id := session.SessionID("sess-hitl-cancel")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "ask"})
	}, time.Millisecond)
	// Cancel only after the turn has parked. A fixed delay races the tool
	// activity under the race detector and drops the yield event.
	var cancelWhenParked func()
	cancelWhenParked = func() {
		if !querySession(t, env).Waiting {
			env.RegisterDelayedCallback(cancelWhenParked, 15*time.Millisecond)
			return
		}
		env.SignalWorkflow(signalCancel, nil)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(signalClose, nil)
		}, 20*time.Millisecond)
	}
	env.RegisterDelayedCallback(cancelWhenParked, 15*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield before cancel, got %+v", got)
	}
}

func TestSessionWorkflow_mixedBatchPairsBeforeNextRound(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "block-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "block-result", IsComplete: true}
			return
		}
		var sawBlock, sawList bool
		for _, m := range msgs {
			if m == nil || m.Role != tacklr.RoleTool {
				continue
			}
			if m.Content == "block-result" {
				sawBlock = true
			}
			if strings.Contains(m.Content, "Jobs:") || m.Content == "No jobs." {
				sawList = true
			}
		}
		if sawBlock && sawList {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{
				{ID: "b1", CallID: "b1", Name: "spawn_specialist", Arguments: `{"specialist":"blocker","task_description_and_context":"block-task","block":true}`},
				{ID: "l1", CallID: "l1", Name: "list_children", Arguments: `{}`},
			},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{
			{Name: "blocker", Model: model},
		}}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-mixed-spawn")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 120*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawBlock, sawList, sawSecond bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && ev.Content == "block-result" {
			sawBlock = true
		}
		if ev.Type == tacklr.StreamEventToolResult && (strings.Contains(ev.Content, "Jobs:") || ev.Content == "No jobs.") {
			sawList = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "second-round" {
			sawSecond = true
		}
	}
	if !sawBlock || !sawList || !sawSecond {
		t.Fatalf("pairing block=%v list=%v second=%v events=%+v", sawBlock, sawList, sawSecond, got)
	}
}

func TestSessionWorkflow_asyncSpawnDoesNotWaitForChild(t *testing.T) {
	env := newTestWorkflow(t)
	id := session.SessionID("sess-async-spawn")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "async-child", IsComplete: true}
			return
		}
		var scheduled, collected bool
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "scheduled") {
				scheduled = true
			}
			if m.Role == tacklr.RoleUser && strings.Contains(m.Content, "completed:") && strings.Contains(m.Content, "async-child") {
				collected = true
			}
		}
		switch {
		case collected:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
		case scheduled:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
				}},
				IsComplete: true,
			}
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "researcher",
			Model: model,
		}}}
	_, fallback := registerSession(env, agent)
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child SessionWorkflow started")
	}
	got := drainLog(t, fallback, id)
	var sawParent, sawScheduled bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "parent-continued") {
			sawParent = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "scheduled") {
			sawScheduled = true
		}
	}
	childGot := drainLog(t, fallback, session.ChildSessionID(id, "researcher", "sp1"))
	var sawChild bool
	for _, ev := range childGot {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "async-child") {
			sawChild = true
		}
	}
	if !sawScheduled || !sawParent || !sawChild {
		t.Fatalf("scheduled=%v parent=%v child=%v parentEv=%+v childEv=%+v", sawScheduled, sawParent, sawChild, got, childGot)
	}
}

func TestSessionWorkflow_listChildren(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			<-ctx.Done()
			return
		}
		if last != nil && last.Role == tacklr.RoleTool {
			if strings.Contains(last.Content, "cancelled and removed") {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "listed", IsComplete: true}
				return
			}
			if strings.Contains(last.Content, "Jobs:") {
				childID := string(session.ChildSessionID("sess-list-children", "researcher", "sp1"))
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "cc1", CallID: "cc1", Name: "cancel_child",
						Arguments: `{"child_id":"` + childID + `"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ls1", CallID: "ls1", Name: "list_children", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name: "researcher", Model: model,
		}}}
	_, fallback := registerSession(env, agent)
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	id := session.SessionID("sess-list-children")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 120*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var listed, cancelled bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "listed" {
			listed = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "cancelled and removed") {
			cancelled = true
		}
	}
	if !listed || !cancelled {
		t.Fatalf("want listed after cancel_child, listed=%v cancelled=%v got %+v", listed, cancelled, drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_cancelStopsAsyncChild(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "should-not-finish", IsComplete: true}
			}
			return
		}
		for _, m := range msgs {
			if m == nil {
				continue
			}
			for _, tc := range m.ToolCalls {
				if tc.Name == "spawn_specialist" {
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
					return
				}
			}
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "researcher",
			Model: model,
		}}}
	registerSession(env, agent)
	var childStarted atomic.Bool
	var childErr error
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
		env.SignalWorkflow(signalCancel, nil)
	})
	env.SetOnChildWorkflowCompletedListener(func(info *workflow.Info, result converter.EncodedValue, err error) {
		childErr = err
	})
	id := session.SessionID("sess-cancel-child")
	childID := session.ChildSessionID(id, "researcher", "sp1")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child started before cancel")
	}
	st := querySession(t, env)
	if st.State != session.SessionFailed || st.Waiting {
		t.Fatalf("parent status %+v", st)
	}
	if childErr == nil {
		if val, qerr := env.QueryWorkflowByID(string(childID), queryStatus); qerr == nil {
			var cst session.SessionStatus
			if err := val.Get(&cst); err == nil && cst.State == session.SessionFailed {
				return
			}
		}
		t.Fatalf("want child canceled/failed, parent=%+v", st)
	}
}

func TestSessionWorkflow_steerDuringYieldKeepsPark(t *testing.T) {
	env := newTestWorkflow(t)
	var n atomic.Int32
	var resumed, invoke2BeforeResume, sawSteer, toolThenSteer atomic.Bool
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
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
		if !resumed.Load() {
			invoke2BeforeResume.Store(true)
		}
		var seq []string
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && m.ToolCallID == "ask1" {
				seq = append(seq, "result")
			}
			if m.Role == tacklr.RoleUser && m.Content == "steer" {
				seq = append(seq, "steer")
				sawSteer.Store(true)
			}
		}
		if i == 2 && strings.Contains(strings.Join(seq, ","), "result,steer") {
			toolThenSteer.Store(true)
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "chose", IsComplete: true}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-steer-yield")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "ask"})
	}, time.Millisecond)
	var steerWhenParked func()
	steerWhenParked = func() {
		if !querySession(t, env).Waiting {
			env.RegisterDelayedCallback(steerWhenParked, 15*time.Millisecond)
			return
		}
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "steer"})
		if !querySession(t, env).Waiting {
			t.Error("steer must keep the park")
		}
		if n.Load() != 1 {
			t.Errorf("Invoke 2 must not run before Resume, got %d", n.Load())
		}
		env.RegisterDelayedCallback(func() {
			resumed.Store(true)
			payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
			env.SignalWorkflow(signalResume, session.ResumeIn{Responses: map[string][]byte{"ask1": payload}})
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(signalClose, nil)
			}, 20*time.Millisecond)
		}, 20*time.Millisecond)
	}
	env.RegisterDelayedCallback(steerWhenParked, 15*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if invoke2BeforeResume.Load() {
		t.Fatal("park was cleared; Invoke 2 ran before Resume")
	}
	if !sawSteer.Load() || !toolThenSteer.Load() {
		t.Fatal("Invoke 2 must see ask_user tool result then steer")
	}
	got := drainLog(t, fallback, id)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want park kept through steer, got %+v", got)
	}
}

func TestSessionWorkflow_failedAsyncChildJobAbsorbed(t *testing.T) {
	env := newTestWorkflow(t)
	id := session.SessionID("sess-async-fail")
	childID := session.ChildSessionID(id, "researcher", "sp1")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Content: "boom-child", IsComplete: true}
			return
		}
		var scheduled, collected bool
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "scheduled") {
				scheduled = true
			}
			if m.Role == tacklr.RoleUser && strings.Contains(m.Content, "failed:") && strings.Contains(m.Content, string(childID)) {
				collected = true
			}
		}
		switch {
		case collected:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
		case scheduled:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
				}},
				IsComplete: true,
			}
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "researcher",
			Model: model,
		}}}
	_, fallback := registerSession(env, agent)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawParent bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-continued" {
			sawParent = true
		}
	}
	if !sawParent {
		t.Fatalf("parent must absorb failed job RoleUser, events=%+v", got)
	}
	val, err := env.QueryWorkflow(queryChildren)
	if err != nil {
		t.Fatal(err)
	}
	var kids []session.SessionID
	if err := val.Get(&kids); err != nil {
		t.Fatal(err)
	}
	for _, k := range kids {
		if k == childID {
			t.Fatalf("failed child still listed: %v", kids)
		}
	}
}

func TestSessionWorkflow_workerChildCompletesParent(t *testing.T) {
	env := newTestWorkflow(t)
	id := session.SessionID("sess-worker")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		var scheduled, collected bool
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "scheduled") {
				scheduled = true
			}
			if m.Role == tacklr.RoleUser && strings.Contains(m.Content, "completed:") && strings.Contains(m.Content, "green") {
				collected = true
			}
		}
		switch {
		case collected:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
		case scheduled:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ci1", CallID: "ci1", Name: "watch_ci", Arguments: `{}`,
				}},
				IsComplete: true,
			}
		}
	})
	watch := tacklr.NewTool(tacklr.ToolConfig{
		Name: "watch_ci",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			job, err := runtime.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "pipe"})
			if err != nil {
				return "", err
			}
			return "Job " + job.ID + " scheduled (name=ci).", nil
		},
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Tools:         []*tacklr.Tool{watch}}
	acts, fallback := registerSession(env, agent)
	acts.Jobs = map[string]session.JobHandler{
		"ci": func(ctx context.Context, task string) (string, error) { return "green", nil },
	}
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want worker child SessionWorkflow")
	}
	got := drainLog(t, fallback, id)
	var sawDone bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
			sawDone = true
		}
	}
	if !sawDone {
		t.Fatalf("want parent-done after worker job, events=%+v", got)
	}
}

func TestSessionWorkflow_toolFailureStaysInTheWindow(t *testing.T) {
	env := newTestWorkflow(t)
	boom := tacklr.NewTool(tacklr.ToolConfig{
		Name: "boom",
		Handler: func(context.Context) (string, error) {
			return "", fmt.Errorf("upstream: the search provider failed: %w", tacklr.ErrFailed)
		},
	})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
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
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{boom}}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-tool-failed")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.SessionComplete {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "search provider failed") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the failure text in the assistant reply, got %+v", drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_missingPathIsACorrection(t *testing.T) {
	env := newTestWorkflow(t)
	dir := t.TempDir()
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
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
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(dir))),
	}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-missing-path")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "read"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.SessionComplete {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "does not exist") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the missing-path correction in the reply, got %+v", drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_badWorkspaceBindingFailsTurn(t *testing.T) {
	env := newTestWorkflow(t)
	missing := filepath.Join(t.TempDir(), "missing")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "r1", CallID: "r1", Name: "read",
				Arguments: `{"path":"/workspace/docs/hello.txt"}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(missing))),
	}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-bad-binding")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, session.PromptIn{
			Text: "read",
			Auth: session.AuthContext{Bindings: []vfs.Binding{{
				Provider: "local",
				Params:   map[string]string{vfs.ParamName: "docs"},
				Auth:     vfs.Credential{Token: "x"},
			}}},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.SessionFailed {
		t.Fatalf("status %+v events %+v", st, drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_grandchildResultReachesParent(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "leaf-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "leaf-answer", IsComplete: true}
			return
		}
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "go-deeper" {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "leaf1", CallID: "leaf1", Name: "spawn_specialist",
					Arguments: `{"specialist":"leaf","task_description_and_context":"leaf-task","block":true}`,
				}},
				IsComplete: true,
			}
			return
		}
		if last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "leaf-answer") {
			var parent bool
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser && m.Content == "go" {
					parent = true
				}
			}
			if parent {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "got-grandchild", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "leaf-answer", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "mid1", CallID: "mid1", Name: "spawn_specialist",
				Arguments: `{"specialist":"mid","task_description_and_context":"go-deeper","block":true}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "mid",
			Model: model,
			Specialists: []*tacklr.Specialist{{
				Name: "leaf", Model: model,
			}},
		}},
	}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-grandchild")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 150*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "got-grandchild" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the grandchild text to finish the parent, got %+v", drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_parkedParentLeavesAsyncChildRunning(t *testing.T) {
	env := newTestWorkflow(t)
	release := make(chan struct{})
	var once sync.Once
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			select {
			case <-ctx.Done():
				return
			case <-release:
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "child-alive", IsComplete: true}
			return
		}
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleTool && strings.Contains(m.Content, "User selected") {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-resumed", IsComplete: true}
				return
			}
		}
		if last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "scheduled") {
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
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}},
	}
	_, fallback := registerSession(env, agent)
	id := session.SessionID("sess-park-child")
	childID := session.ChildSessionID(id, "researcher", "sp1")
	var sawChild bool
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, session.PromptIn{Text: "go"}) }, time.Millisecond)
	var observeWhenParked func()
	observeWhenParked = func() {
		if !querySession(t, env).Waiting {
			env.RegisterDelayedCallback(observeWhenParked, 15*time.Millisecond)
			return
		}
		val, err := env.QueryWorkflow(queryChildren)
		if err != nil {
			t.Fatal(err)
		}
		var ids []session.SessionID
		if err := val.Get(&ids); err != nil {
			t.Fatal(err)
		}
		for _, got := range ids {
			if got == childID {
				sawChild = true
			}
		}
		once.Do(func() { close(release) })
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(signalResume, session.ResumeIn{
				Responses: map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)},
			})
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 20*time.Millisecond)
		}, 20*time.Millisecond)
	}
	env.RegisterDelayedCallback(observeWhenParked, 15*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !sawChild {
		t.Fatal("async child was gone while the parent was parked")
	}
	var resumed bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-resumed" {
			resumed = true
		}
	}
	if !resumed {
		t.Fatalf("want the parent to resume, got %+v", drainLog(t, fallback, id))
	}
}

func newTestWorkflow(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	return env
}

func registerSession(env *testsuite.TestWorkflowEnvironment, agent tacklr.AgentOptions) (*activities, session.EventLog) {
	log := session.NewMemoryEventLog()
	acts := newActs(agent, log, true)
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(acts)
	return acts, log
}

func signalAt(env *testsuite.TestWorkflowEnvironment, at time.Duration, name string, arg any) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(name, arg)
	}, at)
}

func finishSession(t *testing.T, env *testsuite.TestWorkflowEnvironment, log session.EventLog, id session.SessionID) []tacklr.StreamEvent {
	t.Helper()
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	return drainLog(t, log, id)
}

func visibleText(evs []tacklr.StreamEvent) string {
	var b strings.Builder
	for _, ev := range evs {
		b.WriteString(ev.Content)
		b.WriteByte('\n')
		b.WriteString(ev.Fail)
		b.WriteByte('\n')
	}
	return b.String()
}

func releaseLater(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	let := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(let)
	return ch, let
}

func TestSessionWorkflow_steerDuringInferenceJoinsTheNextReply(t *testing.T) {
	release, let := releaseLater(t)
	var n atomic.Int32
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if n.Add(1) == 1 {
			select {
			case <-release:
			case <-ctx.Done():
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "first-done", IsComplete: true}
			return
		}
		var steered bool
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleUser && m.Content == "meanwhile" {
				steered = true
			}
		}
		if !steered {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "missed-steer", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "saw-meanwhile", IsComplete: true}
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	_, log := registerSession(env, agent)
	id := session.SessionID("sess-steer-infer")
	signalAt(env, time.Millisecond, signalPrompt, session.PromptIn{Text: "go"})
	signalAt(env, 20*time.Millisecond, signalPrompt, session.PromptIn{Text: "meanwhile"})
	env.RegisterDelayedCallback(let, 30*time.Millisecond)
	signalAt(env, 90*time.Millisecond, signalClose, nil)
	got := finishSession(t, env, log, id)
	if !strings.Contains(visibleText(got), "saw-meanwhile") {
		t.Fatalf("steer during inference must show up in the next reply, got %s", visibleText(got))
	}
}

func TestSessionWorkflow_steerDuringToolJoinsTheNextReply(t *testing.T) {
	release, let := releaseLater(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleUser && m.Content == "after-tool" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "saw-after-tool", IsComplete: true}
				return
			}
		}
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "tool-only", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "h1", CallID: "h1", Name: "hold", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	hold := tacklr.NewTool(tacklr.ToolConfig{
		Name: "hold",
		Handler: func(ctx context.Context) (string, error) {
			select {
			case <-release:
				return "held", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{hold}}
	_, log := registerSession(env, agent)
	id := session.SessionID("sess-steer-tool")
	signalAt(env, time.Millisecond, signalPrompt, session.PromptIn{Text: "go"})
	signalAt(env, 20*time.Millisecond, signalPrompt, session.PromptIn{Text: "after-tool"})
	env.RegisterDelayedCallback(let, 30*time.Millisecond)
	signalAt(env, 90*time.Millisecond, signalClose, nil)
	got := finishSession(t, env, log, id)
	if !strings.Contains(visibleText(got), "saw-after-tool") {
		t.Fatalf("steer during a tool must show up in the next reply, got %s", visibleText(got))
	}
}

func TestSessionWorkflow_cancelDropsUnreadPromptThenNextPromptRuns(t *testing.T) {
	release, _ := releaseLater(t)
	var n atomic.Int32
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if n.Add(1) == 1 {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return
		}
		text := ""
		if last := lastMsg(msgs); last != nil {
			text = last.Content
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "reply:" + text, IsComplete: true}
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	_, log := registerSession(env, agent)
	id := session.SessionID("sess-cancel-unread")
	signalAt(env, time.Millisecond, signalPrompt, session.PromptIn{Text: "go"})
	signalAt(env, 15*time.Millisecond, signalPrompt, session.PromptIn{Text: "do-not-keep"})
	signalAt(env, 20*time.Millisecond, signalCancel, nil)
	signalAt(env, 40*time.Millisecond, signalPrompt, session.PromptIn{Text: "keep-this"})
	signalAt(env, 90*time.Millisecond, signalClose, nil)
	got := finishSession(t, env, log, id)
	text := visibleText(got)
	if strings.Contains(text, "do-not-keep") || !strings.Contains(text, "reply:keep-this") {
		t.Fatalf("cancel must drop the unread prompt and run the next one, got %s", text)
	}
}

func TestSessionWorkflow_unknownSpecialistStaysInTheWindow(t *testing.T) {
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "not registered") {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "no-such-specialist", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"ghost","task_description_and_context":"look","block":true}`,
			}},
			IsComplete: true,
		}
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}},
	}
	_, log := registerSession(env, agent)
	id := session.SessionID("sess-unknown-spec")
	signalAt(env, time.Millisecond, signalPrompt, session.PromptIn{Text: "go"})
	signalAt(env, 40*time.Millisecond, signalClose, nil)
	got := finishSession(t, env, log, id)
	if !strings.Contains(visibleText(got), "no-such-specialist") {
		t.Fatalf("unknown specialist must come back as a correction the model can answer, got %s", visibleText(got))
	}
}

func TestSessionWorkflow_failedWorkerEndsTheChild(t *testing.T) {
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleUser && strings.Contains(m.Content, "failed:") && strings.Contains(m.Content, "ci broke") {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-saw-failure", IsComplete: true}
				return
			}
		}
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "scheduled") {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "ci1", CallID: "ci1", Name: "watch_ci", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	watch := tacklr.NewTool(tacklr.ToolConfig{
		Name: "watch_ci",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			job, err := runtime.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "pipe"})
			if err != nil {
				return "", err
			}
			return "Job " + job.ID + " scheduled (name=ci).", nil
		},
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{watch}}
	acts, log := registerSession(env, agent)
	acts.Jobs = map[string]session.JobHandler{
		"ci": func(context.Context, string) (string, error) { return "", errors.New("ci broke") },
	}
	id := session.SessionID("sess-worker-fail")
	signalAt(env, time.Millisecond, signalPrompt, session.PromptIn{Text: "go"})
	signalAt(env, 80*time.Millisecond, signalClose, nil)
	got := finishSession(t, env, log, id)
	if !strings.Contains(visibleText(got), "parent-saw-failure") {
		t.Fatalf("a failed worker must reach the parent as a failed job, got %s", visibleText(got))
	}
}

func TestSessionWorkflow_readSkillReturnsTheSkillBody(t *testing.T) {
	pack := t.TempDir()
	dir := filepath.Join(pack, "research")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: research\ndescription: Research carefully\n---\n\nAlways verify claims.\n"
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "Always verify claims") {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "skill-loaded", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sk1", CallID: "sk1", Name: "read_skill", Arguments: `{"name":"research"}`,
			}},
			IsComplete: true,
		}
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		SkillsPath: pack,
	}
	_, log := registerSession(env, agent)
	id := session.SessionID("sess-skill")
	signalAt(env, time.Millisecond, signalPrompt, session.PromptIn{Text: "go"})
	signalAt(env, 40*time.Millisecond, signalClose, nil)
	got := finishSession(t, env, log, id)
	if !strings.Contains(visibleText(got), "skill-loaded") {
		t.Fatalf("read_skill must return the skill body, got %s", visibleText(got))
	}
}
