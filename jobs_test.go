package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
)

func toolResultByName(events []StreamEvent, name string) string {
	for _, ev := range events {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name == name {
				return ev.Content
			}
		}
	}
	return ""
}

func waitJobStatus(t *testing.T, h *AgentHarness, jobID, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j := h.getJob(jobID); j != nil {
			status, _, _ := j.snapshot()
			if status == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %q did not reach status %q", jobID, want)
}

func TestBackgroundJobs_schedulePollAwait(t *testing.T) {
	release := make(chan struct{})
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "bg research done", IsComplete: true}
	}

	ping := NewTool(ToolConfig{
		Name:    "ping",
		Handler: func(ctx context.Context) (string, error) { return "pong", nil },
	})

	var (
		h    *AgentHarness
		step int
	)
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"dig","run_in_background":true}`),
			}, IsComplete: true}
		case 2:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("p1", "ping", `{}`),
				toolCall("l1", "list_jobs", `{}`),
				toolCall("g_run", "get_job", `{"job_id":"bg1"}`),
			}, IsComplete: true}
		case 3:
			close(release)
			waitJobStatus(t, h, "bg1", jobStatusCompleted)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("g_done", "get_job", `{"job_id":"bg1"}`),
			}, IsComplete: true}
		case 4:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("a1", "await_job", `{"job_id":"bg1"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent wrapped up", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Tools:  []*Tool{ping},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "background research while I ping"))
	if !strings.Contains(toolResultByName(got, "spawn_worker"), "Job bg1 scheduled") {
		t.Fatalf("spawn = %q", toolResultByName(got, "spawn_worker"))
	}
	if toolResultByName(got, "ping") != "pong" {
		t.Fatalf("ping = %q", toolResultByName(got, "ping"))
	}
	listOut := toolResultByName(got, "list_jobs")
	if !strings.Contains(listOut, "id=bg1") || !strings.Contains(listOut, "worker=researcher") {
		t.Fatalf("list_jobs = %q", listOut)
	}
	if !strings.Contains(listOut, "get_job") {
		t.Fatalf("list_jobs should point at get_job: %q", listOut)
	}
	var getRunning, getDone string
	for _, ev := range got {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name != "get_job" {
				continue
			}
			if strings.Contains(ev.Content, "status=running") {
				getRunning = ev.Content
			}
			if strings.Contains(ev.Content, "status=completed") {
				getDone = ev.Content
			}
		}
	}
	if getRunning == "" || !strings.Contains(getRunning, "Still running") {
		t.Fatalf("get_job while running = %q", getRunning)
	}
	if !strings.Contains(getDone, "bg research done") {
		t.Fatalf("get_job when completed = %q", getDone)
	}
	if toolResultByName(got, "await_job") != "bg research done" {
		t.Fatalf("await = %q", toolResultByName(got, "await_job"))
	}
	if hasEventType(got, StreamEventError) {
		t.Fatalf("unexpected error: %+v", summarizeEvents(got))
	}
}

func TestBackgroundJobs_awaitAlreadyComplete(t *testing.T) {
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "fast result", IsComplete: true}
	}

	var (
		h    *AgentHarness
		step int
	)
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("fast1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"go","run_in_background":true}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "fast1", jobStatusCompleted)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("a1", "await_job", `{"job_id":"fast1"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "await completed job"))
	if out := toolResultByName(got, "await_job"); out != "fast result" {
		t.Fatalf("await = %q, events=%v", out, summarizeEvents(got))
	}
}

func TestBackgroundJobs_syncSpawnStillBlocks(t *testing.T) {
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "sync answer", IsComplete: true}
	}
	var step int
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		if step == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("s1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"sync"}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})
	defer h.Close()
	got := drainEvents(mustRun(t, h, "sync spawn"))
	if out := toolResultByName(got, "spawn_worker"); out != "sync answer" {
		t.Fatalf("sync spawn = %q", out)
	}
}

func TestBackgroundJobs_interruptOnAwait(t *testing.T) {
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
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker done with choice", IsComplete: true}
	}

	var (
		h    *AgentHarness
		step int
	)
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg_intr", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"ask","run_in_background":true}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "bg_intr", jobStatusInterrupted)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("a1", "await_job", `{"job_id":"bg_intr"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		SessionID: "sess-bg-intr",
		Config:    Config{MaxWindowSize: 8192},
		Model:     parent,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
		},
	})
	defer h.Close()

	events, err := h.Run(context.Background(), "bg interrupt")
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
			for _, tc := range ev.ToolCalls {
				if tc.Name == "await_job" {
					t.Fatalf("await should not complete before resolve, got %q", ev.Content)
				}
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected interrupt from await_job")
	}

	resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptID)
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(resolution),
	})
	if err != nil {
		t.Fatal(err)
	}
	var awaitOut string
	for ev := range resumed {
		if ev.Type == StreamEventToolResult {
			for _, tc := range ev.ToolCalls {
				if tc.Name == "await_job" {
					awaitOut = ev.Content
				}
			}
		}
		if ev.Type == StreamEventError {
			t.Fatalf("error after resume: %v", ev.Error)
		}
	}
	if awaitOut != "worker done with choice" {
		t.Fatalf("await after resolve = %q", awaitOut)
	}
}

func TestBackgroundJobs_closeCancelsRunning(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	}

	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if parent.callNum.Load() == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg_close", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"hang","run_in_background":true}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})

	_ = drainEvents(mustRun(t, h, "schedule then close"))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never started")
	}

	done := make(chan struct{})
	go func() {
		h.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung with running background job")
	}
}

func mustRun(t *testing.T, h *AgentHarness, prompt string) <-chan StreamEvent {
	t.Helper()
	events, err := h.Run(context.Background(), prompt)
	if err != nil {
		t.Fatal(err)
	}
	return events
}
