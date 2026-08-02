package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/session"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// TestSystemPrompt_listsSubAgentsSorted: registered workers appear in the
// system prompt catalog (stable lexical order is a product outcome for prompts).
func TestSystemPrompt_listsSubAgentsSorted(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  model,
		SubAgents: []*SubAgent{
			nil,
			{WorkerName: "", Model: model},
			{WorkerName: "researcher", Model: model, Description: "does research"},
			{WorkerName: "researcher", Model: model, Description: "duplicate ignored"},
			{WorkerName: "no_model", Model: nil, Description: "skipped"},
			{WorkerName: "coder", Model: model, Description: "writes code"},
		},
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	model.mu.Lock()
	prompts := append([]string(nil), model.systemPrompts...)
	model.mu.Unlock()
	var prompt string
	for _, p := range prompts {
		if strings.Contains(p, "AVAILABLE SUB-AGENTS:") {
			prompt = p
			break
		}
	}
	if prompt == "" {
		t.Fatal("system prompt missing sub-agents section")
	}
	if !strings.Contains(prompt, "coder") || !strings.Contains(prompt, "researcher") {
		t.Fatalf("missing workers in prompt: %q", prompt)
	}
	if strings.Contains(prompt, "no_model") {
		t.Fatal("nil-model worker should not appear in prompt")
	}
	ci := strings.Index(prompt, "coder")
	ri := strings.Index(prompt, "researcher")
	if ci > ri {
		t.Errorf("expected coder before researcher in prompt catalog")
	}
	// First registration kept for duplicate name.
	if strings.Contains(prompt, "duplicate ignored") {
		t.Fatal("duplicate worker name should keep first description only")
	}
	if !strings.Contains(prompt, "writes code") || !strings.Contains(prompt, "does research") {
		t.Fatalf("descriptions missing: %q", prompt)
	}
}

// TestSpawnWorker_viaParentTurn_unknownWorker: spawn_worker tool reports missing workers.
func TestSpawnWorker_viaParentTurn_unknownWorker(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "sp1", CallID: "sp1", Name: "spawn_worker",
						Arguments: `{"worker_name":"missing","task_description_and_context":"do work"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		SubAgents: []*SubAgent{
			{WorkerName: "only", Model: &mockStrategy{}},
		},
	})
	events, err := h.Run(context.Background(), "spawn")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "not found") && !hasToolResultContent(got, "missing") {
		t.Fatalf("want unknown worker error, got %+v", summarizeEvents(got))
	}
}

func TestSpawnWorker_unknownWorker(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	_, err := h.runWorker(context.Background(), "missing", "do something", h.runtime)
	if !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("err = %v, want ErrWorkerNotFound", err)
	}
}

func TestSpawnWorker_emptyTask(t *testing.T) {
	workerModel := &mockStrategy{}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	_, err := h.runWorker(context.Background(), "researcher", "   ", h.runtime)
	if !errors.Is(err, ErrEmptyWorkerTask) {
		t.Fatalf("err = %v, want ErrEmptyWorkerTask", err)
	}
}

func TestSpawnWorker_success(t *testing.T) {
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker answer", IsComplete: true}
		},
	}
	parentModel := &mockStrategy{}
	parentModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		n := parentModel.callNum.Load()
		if n == 1 {
			args, _ := json.Marshal(map[string]string{
				"worker_name":                  "researcher",
				"task_description_and_context": "research topic X",
			})
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID:        "tc1",
					CallID:    "call_1",
					Name:      "spawn_worker",
					Arguments: string(args),
				}},
				IsComplete: true,
			}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
	}

	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parentModel,
		Store:  stores.NewInMemoryStore(),
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Description: "research"},
		},
	})

	events, err := h.Run(context.Background(), "use a worker")
	if err != nil {
		t.Fatal(err)
	}

	var toolResults []string
	var updates []string
	for ev := range events {
		switch ev.Type {
		case StreamEventToolResult:
			toolResults = append(toolResults, ev.Content)
		case streaming.StreamEventToolUpdate:
			updates = append(updates, ev.Content)
		case StreamEventError:
			t.Fatalf("unexpected error event: %v", ev.Error)
		}
	}

	if len(toolResults) != 1 {
		t.Fatalf("expected 1 tool result, got %d: %v", len(toolResults), toolResults)
	}
	if toolResults[0] != "worker answer" {
		t.Errorf("tool result = %q, want worker answer", toolResults[0])
	}
	foundStart := false
	for _, u := range updates {
		if strings.Contains(u, `Worker "researcher" started`) {
			foundStart = true
		}
	}
	if !foundStart {
		t.Errorf("expected start update, got %v", updates)
	}
}

func TestSpawnWorker_streamError(t *testing.T) {
	// invokeErr causes worker.Run to emit StreamEventError; drain must surface it
	// instead of swallowing it as "no output".
	workerModel := &mockStrategy{invokeErr: errors.New("model down")}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	out := make(chan StreamEvent, 16)
	session.SetOutputChannel(&h.runtime, out)

	_, err := h.runWorker(context.Background(), "researcher", "do work", h.runtime)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "model down") {
		t.Fatalf("expected model down in error chain, got %v", err)
	}
	if errors.Is(err, ErrWorkerNoOutput) {
		t.Fatal("stream error must not be reported as ErrWorkerNoOutput")
	}
}

func TestSpawnWorker_noOutput(t *testing.T) {
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			// Complete turn with empty assistant content.
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	out := make(chan StreamEvent, 16)
	session.SetOutputChannel(&h.runtime, out)

	_, err := h.runWorker(context.Background(), "researcher", "do work", h.runtime)
	if !errors.Is(err, ErrWorkerNoOutput) {
		t.Fatalf("err = %v, want ErrWorkerNoOutput", err)
	}
}

func TestSpawnWorker_contextCancel(t *testing.T) {
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			<-ctx.Done()
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	out := make(chan StreamEvent, 16)
	session.SetOutputChannel(&h.runtime, out)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := h.runWorker(ctx, "researcher", "do work", h.runtime)
	if err == nil {
		t.Fatal("expected error on cancel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestSpawnWorker_interruptPropagatesAndResumes(t *testing.T) {
	optionsJSON := `[{"title":"A","description":"a","isRecommended":true},{"title":"B","description":"b","isRecommended":false}]`
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
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

	store := stores.NewInMemoryStore()
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parentModel,
		Store:  store,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	})
	h.sessionId = "sess-root"

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
	if len(h.pendingToolCalls) != 1 {
		t.Fatalf("pendingToolCalls = %d, want 1", len(h.pendingToolCalls))
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
}

func TestSpawnWorker_nestedInterruptPropagates(t *testing.T) {
	optionsJSON := `[{"title":"NestedA","description":"a","isRecommended":true}]`
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
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
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  rootModel,
		Store:  store,
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
	h.sessionId = "sess-nested"

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
		Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
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
		Config: Config{MaxWindowSize: 8192},
		Model:  parentModel,
		Store:  store,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	}
	h := NewAgent(context.Background(), opts)
	h.sessionId = "sess-reload"

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

	// Drop live parks to force durable path on resume.
	h.parkMu.Lock()
	for id, w := range h.parkedWorkersLive {
		w.Close()
		delete(h.parkedWorkersLive, id)
	}
	h.parkMu.Unlock()

	// Reload parent from store (checkpoint was written on interrupt).
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

func TestSpawnWorker_toolsSliceIsolation(t *testing.T) {
	sharedTool := NewTool(ToolConfig{
		Name:    "shared",
		Handler: func(ctx context.Context) (string, error) { return "ok", nil },
	})
	tools := []*Tool{sharedTool}
	initialLen := len(tools)

	var sawWorkerTools int
	var mu sync.Mutex
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			mu.Lock()
			sawWorkerTools = len(tools)
			mu.Unlock()
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}

	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: tools},
		},
	})
	out := make(chan StreamEvent, 16)
	session.SetOutputChannel(&h.runtime, out)

	result, err := h.runWorker(context.Background(), "researcher", "isolate tools", h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if result != "done" {
		t.Errorf("result = %q", result)
	}
	if len(tools) != initialLen {
		t.Errorf("shared tools slice grew from %d to %d", initialLen, len(tools))
	}
	mu.Lock()
	defer mu.Unlock()
	if sawWorkerTools <= initialLen {
		// Worker should have plan builtins injected on top of the clone.
		t.Errorf("worker tools = %d, expected > %d (builtins injected on clone)", sawWorkerTools, initialLen)
	}
}

func TestBuiltinToolsInjectedOnce(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "hi", IsComplete: true}
		},
	}
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "w", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  model,
		Store:  stores.NewInMemoryStore(),
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})

	countNamed := func(name string) int {
		n := 0
		for _, tool := range h.tools {
			if tool.Name == name {
				n++
			}
		}
		return n
	}
	if countNamed("spawn_worker") != 1 {
		t.Fatalf("after NewAgent spawn_worker count = %d", countNamed("spawn_worker"))
	}
	if countNamed("create_plan") != 1 {
		t.Fatalf("after NewAgent create_plan count = %d", countNamed("create_plan"))
	}

	// Run twice (multi-turn style) and ensure no duplication.
	for i := 0; i < 2; i++ {
		events, err := h.Run(context.Background(), "hello")
		if err != nil {
			t.Fatal(err)
		}
		for range events {
		}
	}
	if countNamed("spawn_worker") != 1 {
		t.Errorf("after 2 Runs spawn_worker count = %d, want 1", countNamed("spawn_worker"))
	}
	if countNamed("create_plan") != 1 {
		t.Errorf("after 2 Runs create_plan count = %d, want 1", countNamed("create_plan"))
	}
	if countNamed("edit_plan") != 1 || countNamed("complete_todo") != 1 {
		t.Errorf("plan tools duplicated: edit=%d complete=%d", countNamed("edit_plan"), countNamed("complete_todo"))
	}
}
