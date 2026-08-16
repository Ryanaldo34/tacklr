package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// TestSystemPrompt_listsSubAgentsSorted: registered workers appear in the
// system prompt catalog (stable lexical order is a product outcome for prompts).
// TestNewAgent_rejectsInvalidSubAgents: a harness must not start with a
// broken worker catalog (nil spec, empty name, nil model, or duplicate name).
func TestNewAgent_rejectsInvalidSubAgents(t *testing.T) {
	ok := &mockStrategy{}
	t.Run("nil model", func(t *testing.T) {
		if _, err := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 8192}}); err == nil {
			t.Fatal("expected constructor error")
		}
	})
	cases := []struct {
		name  string
		specs []*SubAgent
	}{
		{"nil spec", []*SubAgent{nil}},
		{"empty name", []*SubAgent{{WorkerName: "", Model: ok}}},
		{"nil model", []*SubAgent{{WorkerName: "w", Model: nil}}},
		{"duplicate name", []*SubAgent{
			{WorkerName: "w", Model: ok},
			{WorkerName: "w", Model: ok},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewAgent(context.Background(), AgentOptions{
				Config: Config{MaxWindowSize: 8192}, Model: ok, SubAgents: tc.specs,
			}); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}

func TestSpawnWorker_success(t *testing.T) {
	ping := NewTool(ToolConfig{
		Name:    "ping",
		Handler: func(ctx context.Context) (string, error) { return "pong", nil },
	})
	researcher := &mockStrategy{}
	researcher.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		switch researcher.callNum.Load() {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventReasoning, Content: "planning the lookup", IsComplete: true}
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				{ID: "p1", CallID: "p1", Name: "ping", Arguments: `{}`},
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker answer", IsComplete: true}
		}
	}
	down := &mockStrategy{invokeErr: errors.New("model down")}
	silent := &mockStrategy{invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {}}
	blank := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "", IsComplete: true}
		},
	}
	boom := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Error: errors.New("worker boom")}
		},
	}
	boomText := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: "worker boom text"}
		},
	}
	boomEmpty := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError}
		},
	}

	var step int
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("u1", "spawn_worker", `{"worker_name":"nosuch","task_description_and_context":"x"}`),
			}, IsComplete: true}
		case 2:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("e1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"  "}`),
			}, IsComplete: true}
		case 3:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("d1", "spawn_worker", `{"worker_name":"down","task_description_and_context":"fail"}`),
			}, IsComplete: true}
		case 4:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("s1", "spawn_worker", `{"worker_name":"silent","task_description_and_context":"quiet"}`),
			}, IsComplete: true}
		case 5:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("b1", "spawn_worker", `{"worker_name":"blank","task_description_and_context":"empty"}`),
			}, IsComplete: true}
		case 6:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("ok", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"research topic X"}`),
			}, IsComplete: true}
		case 7:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bm", "spawn_worker", `{"worker_name":"boom","task_description_and_context":"explode"}`),
			}, IsComplete: true}
		case 8:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bt", "spawn_worker", `{"worker_name":"boomtext","task_description_and_context":"explode"}`),
			}, IsComplete: true}
		case 9:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("be", "spawn_worker", `{"worker_name":"boomempty","task_description_and_context":"explode"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		}
	}

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192, MaxTurnRequests: 16},
		Model:  parent,
		Store:  stores.NewInMemoryStore(),
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: researcher, Description: "research", Tools: []*Tool{ping}},
			{WorkerName: "down", Model: down},
			{WorkerName: "silent", Model: silent},
			{WorkerName: "blank", Model: blank},
			{WorkerName: "boom", Model: boom},
			{WorkerName: "boomtext", Model: boomText},
			{WorkerName: "boomempty", Model: boomEmpty},
		},
	})

	events, err := h.Run(context.Background(), "use workers")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if hasEventType(got, StreamEventError) {
		t.Fatalf("turn error: %+v", summarizeEvents(got))
	}
	// Categories (ErrNotFound / ErrInvalid / ErrFailed) plus a specific wrap.
	requireToolResult(t, got, `"nosuch": not found`)
	requireToolResult(t, got, "empty task")
	requireToolResult(t, got, "model down")
	requireToolResult(t, got, "no output")
	requireToolResult(t, got, "worker answer")
	requireToolResult(t, got, "worker boom")
	requireToolResult(t, got, "worker boom text")
	requireToolResult(t, got, "worker stream error with no details")
	var sawStart, sawThink, sawTool bool
	for _, ev := range got {
		if ev.Type != streaming.StreamEventToolUpdate {
			continue
		}
		if strings.Contains(ev.Content, `Worker "researcher" started`) {
			sawStart = true
		}
		if strings.Contains(ev.Content, "thinking") {
			sawThink = true
		}
		if strings.Contains(ev.Content, "tool call: ping") {
			sawTool = true
		}
	}
	if !sawStart || !sawThink || !sawTool {
		t.Fatalf("worker progress start=%v think=%v tool=%v events=%v", sawStart, sawThink, sawTool, summarizeEvents(got))
	}
	parent.mu.Lock()
	prompts := append([]string(nil), parent.systemPrompts...)
	parent.mu.Unlock()
	var listedBare bool
	for _, p := range prompts {
		if strings.Contains(p, " - silent\n") {
			listedBare = true
		}
	}
	if !listedBare {
		t.Fatalf("system prompt must list workers without descriptions, prompts=%q", prompts)
	}
}

func TestSpawnWorker_contextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			cancel()
			<-ctx.Done()
		},
	}
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
			toolCall("c1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"do work"}`),
		}, IsComplete: true}
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	events, err := h.Run(ctx, "use worker")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasErrorIs(got, context.Canceled) {
		t.Fatalf("want cancelled turn, got %+v", summarizeEvents(got))
	}
}

func TestSpawnWorker_interruptPropagatesAndResumes(t *testing.T) {
	optionsJSON := `[{"title":"A","description":"a","isRecommended":true},{"title":"B","description":"b","isRecommended":false}]`
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	})

	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		n := workerModel.callNum.Load()
		if n == 1 {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "tc_intr", CallID: "call_intr", Name: "ask_user", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		// After tool result is in context, finish.
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker done with choice", IsComplete: true}
	}

	parentModel := &mockStrategy{}
	parentModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		n := parentModel.callNum.Load()
		if n == 1 {
			args, _ := json.Marshal(map[string]string{
				"worker_name":                  "researcher",
				"task_description_and_context": "ask the user something",
			})
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "spawn_1", CallID: "spawn_call_1", Name: "spawn_worker", Arguments: string(args),
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}

	// Store save failures must not drop a live interrupt — park stays in-process.
	store := failSaveStore{InMemoryStore: stores.NewInMemoryStore()}
	h := mustNewAgent(t, AgentOptions{
		SessionID: "sess-root",
		Config:    Config{MaxWindowSize: 8192},
		Model:     parentModel,
		Store:     store,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	})

	events, err := h.Run(context.Background(), "use worker with interrupt")
	if err != nil {
		t.Fatal(err)
	}

	var interruptID string
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				t.Fatal(err)
			}
			interruptID = payload.InterruptId
		}
		if ev.Type == StreamEventToolResult {
			t.Fatalf("should not get tool result before resolve, got %q", ev.Content)
		}
	}
	if interruptID == "" {
		t.Fatal("expected StreamEventInterrupt from bubbled worker interrupt")
	}

	resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptID)
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(resolution),
	})
	if err != nil {
		t.Fatal(err)
	}

	var toolResults []string
	for ev := range resumed {
		switch ev.Type {
		case StreamEventToolResult:
			toolResults = append(toolResults, ev.Content)
		case StreamEventError:
			t.Fatalf("error after resume: %v", ev.Error)
		}
	}
	if len(toolResults) != 1 {
		t.Fatalf("tool results = %v, want one spawn result", toolResults)
	}
	if toolResults[0] != "worker done with choice" {
		// spawn returns final assistant text, not the intermediate tool result
		t.Errorf("spawn result = %q, want worker done with choice", toolResults[0])
	}

	// No store: dropping the live park must fail resume (nothing durable to attach).
	volatile := mustNewAgent(t, AgentOptions{
		SessionID: "sess-volatile",
		Config:    Config{MaxWindowSize: 8192},
		Model:     parentModel,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	})
	parentModel.callNum.Store(0)
	workerModel.callNum.Store(0)
	ev2, err := volatile.Run(context.Background(), "park then lose live")
	if err != nil {
		t.Fatal(err)
	}
	interruptID = ""
	for ev := range ev2 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			interruptID = payload.InterruptId
		}
	}
	if interruptID == "" {
		t.Fatal("expected interrupt without store")
	}
	// Close is process teardown: live parks are released. No store → resume cannot attach.
	volatile.Close()
	resolution = fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptID)
	resumed, err = volatile.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(resolution),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(resumed)
	if !hasToolResultContent(got, "parked worker state is missing") {
		t.Fatalf("want parked-state detail, got %+v", summarizeEvents(got))
	}
	if !hasToolResultContent(got, "not found") {
		t.Fatalf("want ErrNotFound category in wrap, got %+v", summarizeEvents(got))
	}
}

func TestSpawnWorker_nestedInterruptPropagates(t *testing.T) {
	optionsJSON := `[{"title":"NestedA","description":"a","isRecommended":true}]`
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			return "picked:" + intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})

	// Leaf worker model: interrupt then finish.
	leafModel := &mockStrategy{}
	leafModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if leafModel.callNum.Load() == 1 {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "leaf_tc", CallID: "leaf_call", Name: "ask_user", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "leaf finished", IsComplete: true}
	}

	// Mid worker model: spawn leaf, then finish after tool result.
	midModel := &mockStrategy{}
	midModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if midModel.callNum.Load() == 1 {
			args, _ := json.Marshal(map[string]string{
				"worker_name":                  "leaf",
				"task_description_and_context": "leaf task",
			})
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "mid_spawn", CallID: "mid_spawn_call", Name: "spawn_worker", Arguments: string(args),
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "mid finished", IsComplete: true}
	}

	rootModel := &mockStrategy{}
	rootModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if rootModel.callNum.Load() == 1 {
			args, _ := json.Marshal(map[string]string{
				"worker_name":                  "mid",
				"task_description_and_context": "mid task",
			})
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "root_spawn", CallID: "root_spawn_call", Name: "spawn_worker", Arguments: string(args),
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "root finished", IsComplete: true}
	}

	store := stores.NewInMemoryStore()
	h := mustNewAgent(t, AgentOptions{
		SessionID: "sess-nested",
		Config:    Config{MaxWindowSize: 8192},
		Model:     rootModel,
		Store:     store,
		SubAgents: []*SubAgent{
			{
				WorkerName: "mid",
				Model:      midModel,
				SubAgents: []*SubAgent{
					{WorkerName: "leaf", Model: leafModel, Tools: []*Tool{interruptTool}},
				},
			},
		},
	})

	events, err := h.Run(context.Background(), "nested interrupt")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			interruptID = payload.InterruptId
		}
	}
	if interruptID == "" {
		t.Fatal("expected bubbled interrupt from nested leaf")
	}

	resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptID)
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(resolution),
	})
	if err != nil {
		t.Fatal(err)
	}
	var toolResults []string
	for ev := range resumed {
		if ev.Type == StreamEventToolResult {
			toolResults = append(toolResults, ev.Content)
		}
		if ev.Type == StreamEventError {
			t.Fatalf("error: %v", ev.Error)
		}
	}
	if len(toolResults) != 1 || toolResults[0] != "mid finished" {
		t.Fatalf("tool results = %v, want [mid finished]", toolResults)
	}
}

func TestSpawnWorker_interruptSurvivesSessionReload(t *testing.T) {
	// Durable park meta + child session: raise interrupt, drop live cache
	// (simulate process boundary), reload parent from store, resolve.
	optionsJSON := `[{"title":"ReloadA","description":"a","isRecommended":true}]`
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			return "ok:" + intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})

	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if workerModel.callNum.Load() == 1 {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "tc_r", CallID: "call_r", Name: "ask_user", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "after reload", IsComplete: true}
	}

	parentModel := &mockStrategy{}
	parentModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if parentModel.callNum.Load() == 1 {
			args, _ := json.Marshal(map[string]string{
				"worker_name":                  "researcher",
				"task_description_and_context": "reload task",
			})
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "spawn_r", CallID: "spawn_call_r", Name: "spawn_worker", Arguments: string(args),
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent after", IsComplete: true}
	}

	store := stores.NewInMemoryStore()
	opts := AgentOptions{
		SessionID: "sess-reload",
		Config:    Config{MaxWindowSize: 8192},
		Model:     parentModel,
		Store:     store,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	}
	h := mustNewAgent(t, opts)

	events, err := h.Run(context.Background(), "start")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			interruptID = payload.InterruptId
		}
	}
	if interruptID == "" {
		t.Fatal("expected interrupt")
	}

	// Host teardown: persist + drop live parks. Reload is the process-restart path.
	h.Close()
	reloaded, err := NewAgentFromSession(context.Background(), "sess-reload", opts)
	if err != nil {
		t.Fatal(err)
	}
	// Parent model call count continues for invoke after tool resume.
	// Fresh mock would break the second invoke expectation — reuse same opts.Model.

	resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptID)
	resumed, err := reloaded.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(resolution),
	})
	if err != nil {
		t.Fatal(err)
	}
	var toolResults []string
	for ev := range resumed {
		if ev.Type == StreamEventToolResult {
			toolResults = append(toolResults, ev.Content)
		}
		if ev.Type == StreamEventError {
			t.Fatalf("error after reload resume: %v", ev.Error)
		}
	}
	if len(toolResults) != 1 || toolResults[0] != "after reload" {
		t.Fatalf("tool results = %v, want [after reload]", toolResults)
	}
}

// failSaveStore keeps LoadSession working but every SaveSession fails so a
// worker interrupt must still bubble from the live park.
type failSaveStore struct {
	*stores.InMemoryStore
}

func (failSaveStore) SaveSession(context.Context, string, stores.SessionCheckpoint) error {
	return errors.New("checkpoint disk full")
}

// newWorkerHost builds a parent with VFS+Brain+namespace and a spawn-shaped
// worker that shares the mount, engine, and index bridge.
func newWorkerHost(t *testing.T) (*AgentHarness, *AgentHarness, *vfs.MountSession, *brain.Engine, uuid.UUID) {
	t.Helper()
	parent, ms, eng, ns := vfsIndexHarness(t, true)
	worker := mustNewAgent(t, parent.workerOptsForSpawn(&SubAgent{WorkerName: "researcher", Model: &mockStrategy{}}))
	t.Cleanup(worker.Close)
	return parent, worker, ms, eng, ns
}

func TestSpawnWorker_sharesHostMountWriteAndCatalog(t *testing.T) {
	_, worker, ms, eng, ns := newWorkerHost(t)
	ctx := context.Background()
	scope := brain.Scope{Namespace: &ns}

	for _, name := range []string{"read", "write", "run_command", "read_object", "search", "index_file"} {
		if worker.findTool(name, "") == nil {
			t.Fatalf("worker catalog missing %s", name)
		}
	}

	const body = "from-worker-body\n"
	if _, err := runWriteTool(t, worker, worker.findTool("write", ""), `{"path":"/work/from-worker.txt","content":`+jsonString(body)+`}`); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/work/from-worker.txt")
	if err != nil || got.Text() != body {
		text := ""
		if got != nil {
			text = got.Text()
		}
		t.Fatalf("parent ReadText after worker write: %q err=%v", text, err)
	}
	rout, err := worker.findTool("read", "").invoke(ctx, `{"path":"/work/from-worker.txt"}`, turnRuntime(worker))
	if err != nil || !strings.Contains(rout.output, "from-worker-body") {
		t.Fatalf("worker read after write: %v %s", err, rout.output)
	}

	if _, err := worker.findTool("run_command", "").invoke(ctx, `{"command":"ls work"}`, turnRuntime(worker)); !errors.Is(err, vfs.ErrFuseNotMounted) {
		t.Fatalf("run_command without HostDir: %v", err)
	}

	const phraseA = "worker-index-share-aaa"
	if err := ms.WriteFile(ctx, "/work/tracked.txt", []byte(phraseA+"\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := runWriteTool(t, worker, worker.findTool("index_file", ""), `{"path":"/work/tracked.txt"}`); err != nil {
		t.Fatal(err)
	}
	if hit := waitSearchHit(t, eng, scope, phraseA, 3*time.Second); hit.Properties[vfsindex.PropVFSPath] != "/work/tracked.txt" {
		t.Fatalf("index vfs_path: %+v", hit.Properties)
	}

	if vfs.FuseAvailable() {
		if err := ms.FuseMount(t.TempDir()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ms.Close() })
		echo := "from-worker-echo\n"
		cmdOut, err := worker.findTool("run_command", "").invoke(ctx, `{"command":"printf 'from-worker-echo\n' > work/from-cmd.txt"}`, turnRuntime(worker))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(cmdOut.output, "exit=0") {
			t.Fatalf("worker run_command: %s", cmdOut.output)
		}
		got, err := ms.ReadText(ctx, "/work/from-cmd.txt")
		if err != nil || got.Text() != echo {
			text := ""
			if got != nil {
				text = got.Text()
			}
			t.Fatalf("parent ReadText after worker echo: %q err=%v", text, err)
		}
	}

	worker.Close()

	const phraseB = "worker-index-share-bbb"
	if err := ms.WriteFile(ctx, "/work/tracked.txt", []byte(phraseB+"\n")); err != nil {
		t.Fatal(err)
	}
	if hit := waitSearchHit(t, eng, scope, phraseB, 3*time.Second); hit.Properties[vfsindex.PropVFSPath] != "/work/tracked.txt" {
		t.Fatalf("reindex after worker Close: %+v", hit.Properties)
	}
}

func TestSpawnWorker_resumeKeepsParentVFS(t *testing.T) {
	optionsJSON := `[{"title":"A","description":"a","isRecommended":true}]`
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			return "selected:" + intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})

	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if workerModel.callNum.Load() == 1 {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "tc_intr", CallID: "call_intr", Name: "ask_user", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker done", IsComplete: true}
	}

	parentModel := &mockStrategy{}
	parentModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if parentModel.callNum.Load() == 1 {
			args, _ := json.Marshal(map[string]string{
				"worker_name":                  "researcher",
				"task_description_and_context": "ask",
			})
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID: "spawn_1", CallID: "spawn_call_1", Name: "spawn_worker", Arguments: string(args),
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}

	ctx := context.Background()
	bstore := brain.NewMemoryStore()
	ns := uuid.New()
	docID := uuid.New()
	if err := bstore.Put(ctx, brain.Object{
		ID: docID, Kind: "Document", Title: "ResumeDoc", Content: "resume-ns-token",
		NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	chunkID := uuid.New()
	pos := 1
	if err := bstore.Put(ctx, brain.Object{
		ID: chunkID, Kind: "Chunk", Content: "resume-ns-token",
		ParentID: &docID, Position: &pos, NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(bstore)
	if err != nil {
		t.Fatal(err)
	}
	opts := AgentOptions{
		SessionID:       "sess-resume-vfs",
		Config:          Config{MaxWindowSize: 8192},
		Model:           parentModel,
		Store:           stores.NewInMemoryStore(),
		MountSession:    ms,
		Brain:           eng,
		SearchNamespace: &ns,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	}
	parent := mustNewAgent(t, opts)

	events, err := parent.Run(ctx, "park worker")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			interruptID = payload.InterruptId
		}
	}
	if interruptID == "" {
		t.Fatal("expected worker interrupt")
	}

	parent.Close()
	reloadOpts := opts
	reloadOpts.SearchNamespace = nil
	reloaded, err := NewAgentFromSession(ctx, "sess-resume-vfs", reloadOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reloaded.Close)

	const spawnID = "spawn_1"
	meta := reloaded.getParkMeta(spawnID)
	if meta == nil {
		t.Fatal("expected park metadata after session reload")
	}
	worker, err := reloaded.attachParkedWorker(ctx, spawnID, *meta, reloaded.subagents["researcher"])
	if err != nil {
		t.Fatal(err)
	}

	const afterBody = "after-resume-body\n"
	if _, err := runWriteTool(t, worker, worker.findTool("write", ""), `{"path":"/work/after-resume.txt","content":`+jsonString(afterBody)+`}`); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/work/after-resume.txt")
	if err != nil || got.Text() != afterBody {
		text := ""
		if got != nil {
			text = got.Text()
		}
		t.Fatalf("parent ReadText after resume write: %q err=%v", text, err)
	}

	reloaded.SetSearchNamespace(uuid.New())
	rout, err := worker.findTool("read_object", "").invoke(ctx, `{"object_id":"`+docID.String()+`"}`, turnRuntime(worker))
	if err != nil || !strings.Contains(rout.output, "resume-ns-token") {
		t.Fatalf("worker read_object after parent namespace change: %v %s", err, rout.output)
	}
	sout, err := worker.findTool("search", "").invoke(ctx, `{"query":"resume-ns-token"}`, turnRuntime(worker))
	if err != nil || (!strings.Contains(sout.output, "resume-ns-token") && !strings.Contains(sout.output, docID.String())) {
		t.Fatalf("worker search after parent namespace change: %v %s", err, sout.output)
	}
}
