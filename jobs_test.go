package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
)

func TestHarnessRuntime_customToolSpawnsChild(t *testing.T) {
	worker := &mockStrategy{}
	worker.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "research result", IsComplete: true}
	}
	var step int
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		if step == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("c1", "delegate", `{"specialist":"researcher","task":"dig"}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
	}
	h := mustNewAgent(t, AgentOptions{
		Model:       parent,
		Config:      Config{MaxWindowSize: 8192},
		Specialists: []*Specialist{{Name: "researcher", Model: worker}},
		Tools: []*Tool{NewTool(ToolConfig{
			Name: "delegate",
			Handler: func(ctx context.Context, args struct {
				Specialist string `json:"specialist"`
				Task       string `json:"task"`
			}, runtime HarnessRuntime) (string, error) {
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
		})},
	})
	defer h.Close()
	got := drainEvents(mustRun(t, h, "go"))
	if out := toolResultByName(got, "delegate"); out != "research result" {
		t.Fatalf("custom spawn = %q", out)
	}
}

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

func lastToolResultByName(events []StreamEvent, name string) string {
	var result string
	for _, ev := range events {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name == name {
				result = ev.Content
			}
		}
	}
	return result
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
				toolCall("bg1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"dig","block":false}`),
			}, IsComplete: true}
		case 2:
			if h.getJob("bg1") == nil {
				t.Error("background worker not registered")
			}
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("p1", "ping", `{}`),
				toolCall("l1", "list_children", `{}`),
				toolCall("g_run", "get_child", `{"child_id":"bg1"}`),
			}, IsComplete: true}
		case 3:
			close(release)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("g_done", "get_child", `{"child_id":"bg1","block":true}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent wrapped up", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Tools:  []*Tool{ping},
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "background research while I ping"))
	if !strings.Contains(toolResultByName(got, "spawn_specialist"), "Child bg1 scheduled") {
		t.Fatalf("spawn = %q", toolResultByName(got, "spawn_specialist"))
	}
	if toolResultByName(got, "ping") != "pong" {
		t.Fatalf("ping = %q", toolResultByName(got, "ping"))
	}
	listOut := toolResultByName(got, "list_children")
	if !strings.Contains(listOut, "id=bg1") || !strings.Contains(listOut, "specialist=researcher") {
		t.Fatalf("list_children = %q", listOut)
	}
	if !strings.Contains(listOut, "get_child") {
		t.Fatalf("list_children should point at get_child: %q", listOut)
	}
	var getRunning string
	for _, ev := range got {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name != "get_child" {
				continue
			}
			if strings.Contains(ev.Content, "status=running") {
				getRunning = ev.Content
			}
		}
	}
	if getRunning == "" || !strings.Contains(getRunning, "Still running") {
		t.Fatalf("get_child while running = %q", getRunning)
	}
	if lastToolResultByName(got, "get_child") != "bg research done" {
		t.Fatalf("blocking get = %q", lastToolResultByName(got, "get_child"))
	}
	if hasEventType(got, StreamEventError) {
		t.Fatalf("unexpected error: %+v", summarizeEvents(got))
	}
}

func TestWorkerLifecycle_syncAndAsyncUseSharedRegistry(t *testing.T) {
	// Arrange
	var harness *AgentHarness
	sawSync := false
	worker := &mockStrategy{
		invokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			if harness.getJob("sync-worker") != nil {
				sawSync = true
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker complete", IsComplete: true}
		},
	}
	step := 0
	parent := &mockStrategy{
		invokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			step++
			if step == 1 {
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					toolCall("sync-worker", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"work","block":true}`),
				}, IsComplete: true}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	harness = mustNewAgent(t, AgentOptions{
		Model:       parent,
		Specialists: []*Specialist{{Name: "researcher", Model: worker}},
	})
	t.Cleanup(harness.Close)

	// Act
	events := drainEvents(mustRun(t, harness, "run worker"))

	// Assert
	if !sawSync {
		t.Fatal("synchronous worker did not use the shared lifecycle registry")
	}
	if result := toolResultByName(events, "spawn_specialist"); result != "worker complete" {
		t.Fatalf("spawn result = %q", result)
	}
	if harness.getJob("sync-worker") != nil {
		t.Fatal("completed synchronous worker remained registered")
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
				toolCall("fast1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"go","block":false}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "fast1", jobStatusCompleted)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("a1", "get_child", `{"child_id":"fast1"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "await completed job"))
	if out := toolResultByName(got, "get_child"); out != "fast result" {
		t.Fatalf("non-blocking completed get = %q, events=%v", out, summarizeEvents(got))
	}
}

func TestBackgroundJobs_failedWorkersAreCollectable(t *testing.T) {
	streamFailure := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Error: fmt.Errorf("worker stream failed")}
		},
	}
	noOutput := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "", IsComplete: true}
		},
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
				toolCall("failed_stream", "spawn_specialist", `{"specialist":"stream_failure","task_description_and_context":"fail","block":false}`),
				toolCall("no_output", "spawn_specialist", `{"specialist":"no_output","task_description_and_context":"finish blank","block":false}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "failed_stream", jobStatusFailed)
			waitJobStatus(t, h, "no_output", jobStatusFailed)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("get_failed_stream", "get_child", `{"child_id":"failed_stream"}`),
				toolCall("get_no_output", "get_child", `{"child_id":"no_output"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "failures handled", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "stream_failure", Model: streamFailure},
			{Name: "no_output", Model: noOutput},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "run failure cases"))
	var errors []string
	for _, ev := range got {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name == "get_child" && tc.Status == "error" {
				errors = append(errors, ev.Content)
			}
		}
	}
	if len(errors) != 2 {
		t.Fatalf("get_child errors = %v, want stream and empty-output failures", errors)
	}
	if !strings.Contains(strings.Join(errors, "\n"), "worker stream failed") {
		t.Fatalf("missing stream failure: %v", errors)
	}
	if !strings.Contains(strings.Join(errors, "\n"), "no output") {
		t.Fatalf("missing empty-output failure: %v", errors)
	}
	if h.getJob("failed_stream") != nil || h.getJob("no_output") != nil {
		t.Fatal("collected failed jobs remain registered")
	}
}

func TestBackgroundJobTools_reportEmptyAndInvalidRequests(t *testing.T) {
	var step int
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		if step == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("list", "list_children", `{}`),
				toolCall("get_empty", "get_child", `{"child_id":""}`),
				toolCall("get_missing", "get_child", `{"child_id":"missing"}`),
				toolCall("cancel_empty", "cancel_child", `{"child_id":""}`),
				toolCall("cancel_missing", "cancel_child", `{"child_id":"missing"}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "validation handled", IsComplete: true}
	}

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: &mockStrategy{}},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "inspect jobs"))
	if out := toolResultByName(got, "list_children"); out != "No child sessions." {
		t.Fatalf("empty list = %q", out)
	}
	var invalid, missing int
	for _, ev := range got {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name != "get_child" && tc.Name != "cancel_child" {
				continue
			}
			if strings.Contains(ev.Content, "required") {
				invalid++
			}
			if strings.Contains(ev.Content, "unknown") {
				missing++
			}
		}
	}
	if invalid != 2 || missing != 2 {
		t.Fatalf("invalid=%d missing=%d, events=%v", invalid, missing, summarizeEvents(got))
	}
}

func TestSpawnBatch_blockingPairsBeforeNextRound(t *testing.T) {
	release := make(chan struct{})
	var blockingStarted, released atomic.Bool
	blocker := &mockStrategy{}
	blocker.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		blockingStarted.Store(true)
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "block-result", IsComplete: true}
	}
	asyncWorker := &mockStrategy{}
	asyncWorker.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "async-result", IsComplete: true}
	}
	var (
		step           int
		secondTooEarly bool
		sawBlock       bool
		sawAsync       bool
		sawPing        bool
	)
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		if step == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("b1", "spawn_specialist", `{"specialist":"blocker","task_description_and_context":"wait","block":true}`),
				toolCall("a1", "spawn_specialist", `{"specialist":"async","task_description_and_context":"go","block":false}`),
				toolCall("p1", "ping", `{}`),
			}, IsComplete: true}
			return
		}
		if !released.Load() {
			secondTooEarly = true
		}
		for _, m := range msgs {
			if m == nil || m.Role != RoleTool {
				continue
			}
			switch m.Content {
			case "block-result":
				sawBlock = true
			case "pong":
				sawPing = true
			default:
				if strings.Contains(m.Content, "Child a1 scheduled") {
					sawAsync = true
				}
			}
		}
		if step == 2 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("c1", "cancel_child", `{"child_id":"a1"}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "second-round", IsComplete: true}
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Tools: []*Tool{NewTool(ToolConfig{
			Name:    "ping",
			Handler: func(ctx context.Context) (string, error) { return "pong", nil },
		})},
		Specialists: []*Specialist{
			{Name: "blocker", Model: blocker},
			{Name: "async", Model: asyncWorker},
		},
	})
	defer h.Close()
	go func() {
		for !blockingStarted.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		released.Store(true)
		close(release)
	}()
	got := drainEvents(mustRun(t, h, "mixed spawn batch"))
	if secondTooEarly {
		t.Fatal("next model round started before blocking child finished")
	}
	if !sawBlock || !sawAsync || !sawPing {
		t.Fatalf("second round missing paired results block=%v async=%v ping=%v", sawBlock, sawAsync, sawPing)
	}
	if toolResultByName(got, "ping") != "pong" {
		t.Fatalf("ping = %q", toolResultByName(got, "ping"))
	}
	var blockOut, asyncOut string
	for _, ev := range got {
		if ev.Type != StreamEventToolResult {
			continue
		}
		for _, tc := range ev.ToolCalls {
			if tc.Name != "spawn_specialist" {
				continue
			}
			if tc.ID == "b1" || tc.CallID == "b1" {
				blockOut = ev.Content
			}
			if tc.ID == "a1" || tc.CallID == "a1" {
				asyncOut = ev.Content
			}
		}
	}
	if blockOut != "block-result" {
		t.Fatalf("blocking spawn result = %q, want child output", blockOut)
	}
	if !strings.Contains(asyncOut, "Child a1 scheduled") {
		t.Fatalf("async spawn result = %q, want scheduled", asyncOut)
	}
	var sawSecond bool
	for _, ev := range got {
		if ev.Type == StreamEventMessage && strings.Contains(ev.Content, "second-round") {
			sawSecond = true
		}
	}
	if !sawSecond {
		t.Fatalf("want second-round after pairs, got %+v", summarizeEvents(got))
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
				toolCall("s1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"sync"}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})
	defer h.Close()
	got := drainEvents(mustRun(t, h, "sync spawn"))
	if out := toolResultByName(got, "spawn_specialist"); out != "sync answer" {
		t.Fatalf("sync spawn = %q", out)
	}
}

func TestBackgroundJobs_interruptsResumeAndCancel(t *testing.T) {
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

	newInterruptWorkerModel := func(interrupts int) *mockStrategy {
		worker := &mockStrategy{}
		worker.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n := worker.callNum.Load()
			if int(n) <= interrupts {
				id := fmt.Sprintf("intr_%d", n)
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: id, CallID: id, Name: "ask_user", Arguments: `{}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker done with choice", IsComplete: true}
		}
		return worker
	}
	workerModel := newInterruptWorkerModel(2)
	cancelledWorkerModel := newInterruptWorkerModel(1)

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
				toolCall("bg_intr", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"ask","block":false}`),
				toolCall("cancel_intr", "spawn_specialist", `{"specialist":"disposable","task_description_and_context":"ask","block":false}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "bg_intr", jobStatusInterrupted)
			waitJobStatus(t, h, "cancel_intr", jobStatusInterrupted)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("a1", "get_child", `{"child_id":"bg_intr","block":true}`),
				toolCall("c1", "cancel_child", `{"child_id":"cancel_intr"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		SessionID: "sess-bg-intr",
		Config:    Config{MaxWindowSize: 8192},
		Model:     parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel, Tools: []*Tool{interruptTool}},
			{Name: "disposable", Model: cancelledWorkerModel, Tools: []*Tool{interruptTool}},
		},
	})
	defer h.Close()

	events, err := h.Run(context.Background(), "bg interrupt")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID, cancelOut string
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
				if tc.Name == "get_child" {
					t.Fatalf("blocking get should not complete before resolve, got %q", ev.Content)
				}
				if tc.Name == "cancel_child" {
					cancelOut = ev.Content
				}
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected interrupt from blocking get_child")
	}
	if cancelOut != "Child cancel_intr cancelled and removed." {
		t.Fatalf("interrupted cancellation = %q", cancelOut)
	}

	resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptID)
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(resolution),
	})
	if err != nil {
		t.Fatal(err)
	}
	var secondInterruptID string
	for ev := range resumed {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				t.Fatal(err)
			}
			secondInterruptID = payload.InterruptId
		}
		if ev.Type == StreamEventError {
			t.Fatalf("error after resume: %v", ev.Error)
		}
	}
	if secondInterruptID == "" {
		t.Fatal("expected worker's second interrupt")
	}

	secondResolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":1}`, secondInterruptID)
	resumed, err = h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		secondInterruptID: []byte(secondResolution),
	})
	if err != nil {
		t.Fatal(err)
	}
	var awaitOut string
	for ev := range resumed {
		if ev.Type == StreamEventToolResult {
			for _, tc := range ev.ToolCalls {
				if tc.Name == "get_child" {
					awaitOut = ev.Content
				}
			}
		}
		if ev.Type == StreamEventError {
			t.Fatalf("error after second resume: %v", ev.Error)
		}
	}
	if awaitOut != "worker done with choice" {
		t.Fatalf("blocking get after two resolutions = %q", awaitOut)
	}
}

func TestBackgroundJobs_nudgesBeforeTurnCompletion(t *testing.T) {
	release := make(chan struct{})
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		select {
		case <-release:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "nudged result", IsComplete: true}
		case <-ctx.Done():
		}
	}

	var (
		step     int
		sawNudge bool
	)
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("nudge_job", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"work","block":false}`),
			}, IsComplete: true}
		case 2:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "premature finish", IsComplete: true}
		case 3:
			for _, msg := range msgs {
				if msg.Role == RoleUser && strings.Contains(msg.Content, "Automated harness nudge") {
					sawNudge = true
					break
				}
			}
			close(release)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("collect", "get_child", `{"child_id":"nudge_job","block":true}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "finished with result", IsComplete: true}
		}
	}

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "run background work"))
	if !sawNudge {
		t.Fatal("model did not receive the automated background-jobs nudge")
	}
	if out := toolResultByName(got, "get_child"); out != "nudged result" {
		t.Fatalf("collected result = %q", out)
	}
	completes := 0
	for _, ev := range got {
		if ev.Type == StreamEventComplete {
			completes++
		}
	}
	if completes != 1 {
		t.Fatalf("complete events = %d, want one after job collection", completes)
	}
}

func TestBackgroundJobs_cancelJob(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		close(started)
		<-ctx.Done()
		close(stopped)
	}

	var step int
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("cancel_me", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"work","block":false}`),
			}, IsComplete: true}
		case 2:
			select {
			case <-started:
			case <-ctx.Done():
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("cancel", "cancel_child", `{"child_id":"cancel_me"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "cancelled", IsComplete: true}
		}
	}

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "cancel background work"))
	if out := toolResultByName(got, "cancel_child"); out != "Child cancel_me cancelled and removed." {
		t.Fatalf("cancel result = %q", out)
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel_child did not stop worker")
	}
	if h.getJob("cancel_me") != nil {
		t.Fatal("cancelled job remains registered")
	}
}

func TestBackgroundJobs_closeCancelsRunning(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(stopped)
	}

	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if parent.callNum.Load() == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg_close", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"hang","block":false}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "schedule then close")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never started")
	}
	cancel()
	_ = drainEvents(events)

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Run context cancel must stop background children")
	}
	if j := h.getJob("bg_close"); j != nil {
		status, _, _ := j.snapshot()
		if status == jobStatusRunning {
			t.Fatalf("job still running after request cancel: %s", status)
		}
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

func TestBackgroundJobs_nonBlockingGetReportsRunningStatus(t *testing.T) {
	// Arrange
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			time.Sleep(400 * time.Millisecond)
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	var h *AgentHarness
	var step int
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("running_job", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"work","block":false}`),
			}, IsComplete: true}
		case 2:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("poll", "get_child", `{"child_id":"running_job"}`),
			}, IsComplete: true}
		case 3:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("collect", "get_child", `{"child_id":"running_job","block":true}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "finished", IsComplete: true}
		}
	}
	h = mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  parent,
		Specialists: []*Specialist{
			{Name: "researcher", Model: workerModel},
		},
	})
	t.Cleanup(h.Close)

	// Act
	got := drainEvents(mustRun(t, h, "poll running job"))
	runningOut := toolResultByName(got, "get_child")

	// Assert
	if !strings.Contains(runningOut, "Still running") {
		t.Fatalf("running status = %q events=%v", runningOut, summarizeEvents(got))
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
