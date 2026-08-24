package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	mcpruntime "github.com/ryanaldo34/tacklr/internal/mcp"
	"github.com/ryanaldo34/tacklr/mcp"
)

// drainEvents collects all events until the channel closes.
func drainEvents(events <-chan StreamEvent) []StreamEvent {
	var out []StreamEvent
	for ev := range events {
		out = append(out, ev)
	}
	return out
}

func hasEventType(events []StreamEvent, typ StreamEventType) bool {
	for _, ev := range events {
		if ev.Type == typ {
			return true
		}
	}
	return false
}

func hasErrorIs(events []StreamEvent, target error) bool {
	for _, ev := range events {
		if ev.Type == StreamEventError && ev.Error != nil && errors.Is(ev.Error, target) {
			return true
		}
	}
	return false
}

func hasToolResultContent(events []StreamEvent, substr string) bool {
	for _, ev := range events {
		if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, substr) {
			return true
		}
	}
	return false
}

// sequentialToolModel emits each tool-call batch on successive Invokes, then a final message.
func sequentialToolModel(batches ...[]ToolCall) *mockStrategy {
	var n int
	return &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n <= len(batches) {
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: batches[n-1], IsComplete: true}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
}

func toolCall(id, name, args string) ToolCall {
	return ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}

// runPrompt builds a harness, runs one prompt, drains events. opts may be nil.
func runPrompt(t *testing.T, model *mockStrategy, opts AgentOptions) (*AgentHarness, []StreamEvent) {
	t.Helper()
	if opts.Model == nil {
		opts.Model = model
	}
	if opts.Config.MaxWindowSize == 0 {
		opts.Config.MaxWindowSize = 8192
	}
	h := mustNewAgent(t, opts)
	t.Cleanup(h.Close)
	_ = h.SessionID() // exercise getter (empty when no store/session id set)
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	return h, drainEvents(events)
}

func TestRunMessage_imageAcceptedOnlyWhenModelAllows(t *testing.T) {
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	t.Cleanup(h.Close)
	if _, err := h.RunMessage(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "requires a user message") {
		t.Fatalf("nil message: %v", err)
	}

	img := &Message{
		Content: "see this",
		ContentParts: []ContentPart{
			{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/png;base64,AAAA"}},
			{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/png;base64,BBBB"}},
			{Type: ContentTypeInputFile, FileData: &FileData{MIMEType: "", Filename: "skip"}},
		},
	}
	if _, err := h.RunMessage(context.Background(), img); err == nil || !strings.Contains(err.Error(), "unsupported content type") {
		t.Fatalf("text-only model: %v", err)
	}

	vision := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model: &mockStrategy{
			supportsMIMEFn: func(string) bool { return true },
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "a cat", IsComplete: true}
			},
		},
	})
	t.Cleanup(vision.Close)
	events, err := vision.RunMessage(context.Background(), img)
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	var saw bool
	for _, ev := range got {
		if ev.Type == StreamEventMessage && strings.Contains(ev.Content, "a cat") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("vision turn: %+v", summarizeEvents(got))
	}
}

func requireToolResult(t *testing.T, got []StreamEvent, substr string) {
	t.Helper()
	if !hasToolResultContent(got, substr) {
		t.Fatalf("want tool result containing %q, got %+v", substr, summarizeEvents(got))
	}
}

// TestRun_parallelToolResultsBothLandInWindow: one model round with two tool
// calls absorbs both results so the next Turn sees them (not last-writer-wins).
func TestRun_parallelToolResultsBothLandInWindow(t *testing.T) {
	var (
		n   int
		got []string
	)
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					toolCall("a", "alpha", `{}`),
					toolCall("b", "beta", `{}`),
				}, IsComplete: true}
				return
			}
			for _, m := range msgs {
				if m != nil && m.Role == RoleTool {
					got = append(got, m.Content)
				}
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  model,
		Tools: []*Tool{
			NewTool(ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
			NewTool(ToolConfig{Name: "beta", Handler: func(context.Context) (string, error) { return "from-beta", nil }}),
		},
	})
	t.Cleanup(h.Close)
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)
	saw := map[string]bool{}
	for _, c := range got {
		saw[c] = true
	}
	if !saw["from-alpha"] || !saw["from-beta"] {
		t.Fatalf("next turn window tool results = %q", got)
	}
}

// TestRun_parallelHITLKeepsSiblingResults: the whole batch starts; siblings
// finish while one parks; the next model turn sees every result (Azure pairing).
func TestRun_parallelHITLKeepsSiblingResults(t *testing.T) {
	var (
		invokes int
		second  []*Message
	)
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			invokes++
			if invokes == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "fc_alpha", CallID: "call_alpha", Name: "alpha", Arguments: `{}`},
						{ID: "fc_gate", CallID: "call_gate", Name: "gate", Arguments: `{}`},
						{ID: "fc_beta", CallID: "call_beta", Name: "beta", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			second = append([]*Message(nil), msgs...)
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "all-three", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  model,
		Tools: []*Tool{
			NewTool(ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
			NewTool(ToolConfig{
				Name:    "gate",
				OnCall:  []OnCallFunc{ToolPermissionOnCall},
				Handler: func(context.Context) (string, error) { return "gate-ok", nil },
			}),
			NewTool(ToolConfig{Name: "beta", Handler: func(context.Context) (string, error) { return "from-beta", nil }}),
		},
	})
	t.Cleanup(h.Close)

	events, err := h.Run(context.Background(), "batch")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for _, ev := range drainEvents(events) {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected permission park")
	}
	if invokes != 1 {
		t.Fatalf("inferred while parked: invokes=%d", invokes)
	}
	sawParked := map[string]bool{}
	for _, m := range h.Messages() {
		if m != nil && m.Role == RoleTool {
			sawParked[m.Content] = true
		}
	}
	if !sawParked["from-alpha"] || !sawParked["from-beta"] {
		t.Fatalf("siblings should finish during park: %v window=%+v", sawParked, h.Messages())
	}
	if sawParked["gate-ok"] {
		t.Fatal("parked tool should not have a result yet")
	}

	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"allow-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(resumed)
	if !hasEventType(got, StreamEventComplete) && !hasToolResultContent(got, "gate-ok") {
		t.Fatalf("resume: %+v", summarizeEvents(got))
	}
	saw := map[string]bool{}
	for _, m := range second {
		if m != nil && m.Role == RoleTool {
			saw[m.Content] = true
		}
	}
	if !saw["from-alpha"] || !saw["gate-ok"] || !saw["from-beta"] {
		t.Fatalf("flushed window missing results: %v msgs=%+v", saw, second)
	}
}

// TestRun_uninitializedHarnessFails: Run without constructor setup is rejected.
func TestRun_maxTurnRequests_emitsStopReason(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name: "ping",
		Handler: func(ctx context.Context) (string, error) {
			return "pong", nil
		},
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			// Always request another tool call so the turn would re-invoke forever.
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "c1", CallID: "c1", Name: "ping", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192, MaxTurnRequests: 1},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "loop")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasErrorIs(got, ErrMaxTurnRequests) {
		t.Fatalf("want ErrMaxTurnRequests in events, got %+v", summarizeEvents(got))
	}
}

// TestRun_invokeError_emitsErrorEvent: model Invoke failures end the turn with an error event.
func TestRun_invokeError_emitsErrorEvent(t *testing.T) {
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{invokeErr: errors.New("provider down")},
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasEventType(got, StreamEventError) {
		t.Fatalf("want error event, got %+v", summarizeEvents(got))
	}
	var saw bool
	for _, ev := range got {
		if ev.Type == StreamEventError && ev.Error != nil && strings.Contains(ev.Error.Error(), "provider down") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want provider down in error, got %+v", summarizeEvents(got))
	}
}

func TestRun_invokeErrorAfterTools_emitsWrappedError(t *testing.T) {
	// Arrange
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "ok", nil },
	})
	calls := 0
	strategy := &mockStrategy{
		invokeErrFn: func(context.Context, []*Message, []*Tool) error {
			calls++
			if calls > 1 {
				return errors.New("after tools")
			}
			return nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "t1", CallID: "t1", Name: "echo", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	t.Cleanup(h.Close)

	// Act
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)

	// Assert
	if !hasErrorIs(got, ErrModelAfterTools) {
		t.Fatalf("events = %+v", summarizeEvents(got))
	}
}

func TestReturnFromInterrupt_rejectsUnknownInterrupt(t *testing.T) {
	// Arrange
	model := sequentialToolModel([]ToolCall{toolCall("ask1", "ask_user_choice",
		`{"question":"Pick?","choices":[{"title":"A"}]}`)})
	h := mustNewAgent(t, AgentOptions{
		Model: model, Config: Config{MaxWindowSize: 8192},
	})
	t.Cleanup(h.Close)
	events, err := h.Run(context.Background(), "ask")
	if err != nil {
		t.Fatal(err)
	}
	drainEvents(events)

	// Act
	_, err = h.ReturnFromInterrupt(context.Background(), map[string][]byte{"missing": []byte(`{"selectionIdx":0}`)})

	// Assert
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error = %v", err)
	}
}

// TestRun_modelStreamError_afterToolAnnounce_emitsFailedToolResults: incomplete
// tool announcements are closed with an error status when the model stream fails.
func TestRun_modelStreamError_afterToolAnnounce_emitsFailedToolResults(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "ann1", CallID: "ann1", Name: "missing_tool", Arguments: `{}`},
				},
				IsComplete: false,
			}
			ch <- LLMResponseChunk{
				Type:    StreamEventError,
				Content: "upstream failed",
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "model error") {
		t.Fatalf("want failed tool result for announced call, got %+v", summarizeEvents(got))
	}
}

// TestRun_incompleteToolCall_emitsFailedToolResult: announced but non-executable
// tool calls get a terminal tool_result so clients leave in_progress.
func TestRun_incompleteToolCall_emitsFailedToolResult(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "", CallID: "", Name: "ghost", Arguments: `{}`},
					{ID: "inc1", CallID: "inc1", Name: "ghost", Arguments: `{}`},
				},
				IsComplete: false,
			}
			// Stream ends without a complete executable function_call.
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "incomplete") {
		t.Fatalf("want incomplete tool result, got %+v", summarizeEvents(got))
	}
	if !hasEventType(got, StreamEventComplete) {
		t.Fatalf("want complete after incomplete tools closed, got %+v", summarizeEvents(got))
	}
}

// TestRun_unknownTool_surfacesToolResultError: missing tools produce an error tool_result, not a panic.
func TestRun_unknownTool_surfacesToolResultError(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeErrFn: func(context.Context, []*Message, []*Tool) error {
			if n >= 1 {
				return errors.New("after tools down")
			}
			return nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "u1", CallID: "u1", Name: "does_not_exist", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "not found") && !hasToolResultContent(got, "does_not_exist") {
		t.Fatalf("want unknown tool error result, got %+v", summarizeEvents(got))
	}
	if !hasErrorIs(got, ErrModelAfterTools) {
		t.Fatalf("want after-tools model error, got %+v", summarizeEvents(got))
	}
}

// TestRun_functionCallRecordedBeforeToolResult: Azure-style pairing — the next
// model invoke sees an assistant function_call whose call_id matches the tool output.
func TestRun_functionCallRecordedBeforeToolResult(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "pong", nil },
	})
	var n int
	var second []*Message
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "fc_item", CallID: "call_abc123", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			second = append([]*Message(nil), msgs...)
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)
	if n < 2 {
		t.Fatalf("want second model invoke, got %d", n)
	}
	var sawFC, sawOut bool
	for _, m := range second {
		if m.Role == RoleAssistant && len(m.ToolCalls) == 1 {
			tc := m.ToolCalls[0]
			if tc.CallID == "call_abc123" && tc.ID == "fc_item" {
				sawFC = true
			}
		}
		if m.Role == RoleTool && m.ToolCallID == "call_abc123" {
			sawOut = true
		}
	}
	if !sawFC || !sawOut {
		t.Fatalf("second invoke msgs missing paired function_call/tool: sawFC=%v sawOut=%v msgs=%+v", sawFC, sawOut, second)
	}
}

// TestRun_modelErrorAfterTools_tagsAndCheckpointsPairs: after a successful tool
// batch, a provider stream failure is marked ErrModelAfterTools and the durable
// window retains user + function_call + tool result for resume.
func TestRun_modelErrorAfterTools_tagsAndCheckpointsPairs(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "tool-ok", nil },
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "fc_echo", CallID: "call_echo", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{
				Type:       StreamEventError,
				Error:      fmt.Errorf("provider HTTP 200: stream ended without a usable response; status=failed"),
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	h.sessionId = "sess-after-tools-ckpt"
	events, err := h.Run(context.Background(), "do the tool then die")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "tool-ok") {
		t.Fatalf("want successful tool result, got %+v", summarizeEvents(got))
	}
	var sawTagged bool
	for _, ev := range got {
		if ev.Type == StreamEventError && ev.Error != nil && errors.Is(ev.Error, ErrModelAfterTools) {
			sawTagged = true
		}
	}
	if !sawTagged {
		t.Fatalf("want ErrModelAfterTools, got %+v", summarizeEvents(got))
	}

	loaded := reloadHarness(t, h, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		Tools:  []*Tool{tool},
	})
	var sawUser, sawFC, sawTool bool
	for _, m := range loaded.Messages() {
		if m == nil {
			continue
		}
		switch m.Role {
		case RoleUser:
			if m.Content == "do the tool then die" {
				sawUser = true
			}
		case RoleAssistant:
			if len(m.ToolCalls) == 1 && m.ToolCalls[0].CallID == "call_echo" {
				sawFC = true
			}
		case RoleTool:
			if m.ToolCallID == "call_echo" && m.Content == "tool-ok" {
				sawTool = true
			}
		}
	}
	if !sawUser || !sawFC || !sawTool {
		t.Fatalf("checkpoint missing pairs: user=%v fc=%v tool=%v window=%+v",
			sawUser, sawFC, sawTool, loaded.Messages())
	}
}

// TestRun_pairsOpenToolCallsBeforeTurn: a restored open function_call is paired
// with an error tool result so the next model turn sees a valid window.
func TestRun_pairsOpenToolCallsBeforeTurn(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type:       StreamEventError,
				Error:      fmt.Errorf("provider down"),
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	h.sessionId = "sess-pair-open"
	h.context.Restore([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{
			{ID: "fc_orphan", CallID: "orphan", Name: "echo", Arguments: `{}`},
		}},
		{Role: RoleAssistant, ToolCalls: []ToolCall{
			{CallID: "good", Name: "echo", Arguments: `{}`},
		}},
		{Role: RoleTool, ToolCallID: "good", Content: "done"},
	})

	events, err := h.Run(context.Background(), "continue")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)

	loaded := reloadHarness(t, h, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	var sawGood, sawOrphanCall, sawOrphanResult bool
	for _, m := range loaded.Messages() {
		if m == nil {
			continue
		}
		if m.Role == RoleAssistant {
			for _, tc := range m.ToolCalls {
				if tc.CallID == "orphan" {
					sawOrphanCall = true
				}
			}
		}
		if m.Role == RoleTool {
			if m.ToolCallID == "good" && m.Content == "done" {
				sawGood = true
			}
			if m.ToolCallID == "orphan" {
				sawOrphanResult = true
			}
		}
	}
	if !sawOrphanCall || !sawOrphanResult {
		t.Fatalf("open tool call must be paired: call=%v result=%v window=%+v",
			sawOrphanCall, sawOrphanResult, loaded.Messages())
	}
	if !sawGood {
		t.Fatalf("paired tool result missing: %+v", loaded.Messages())
	}
}

// TestRun_permissionResumeDoesNotPairItemID: Azure function_call items have
// ID=fc_… and CallID=call_…. pairOpenToolCalls must not inject a RoleTool
// under the item id on permission resume — that output has no matching
// function_call on the wire and Azure 400s.
func TestRun_permissionResumeDoesNotPairItemID(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "write",
		OnCall:  []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) { return "wrote", nil },
	})
	var n int
	var second []*Message
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "fc_06f6item", CallID: "call_7Ge4wire", Name: "write", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			second = append([]*Message(nil), msgs...)
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "write the doc")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for _, ev := range drainEvents(events) {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected tool_permission yield")
	}

	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"allow-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(resumed)
	if !hasToolResultContent(got, "wrote") {
		t.Fatalf("want write result after allow, got %+v", summarizeEvents(got))
	}
	if n < 2 {
		t.Fatalf("want second model invoke, got %d", n)
	}

	var results []*Message
	for _, m := range second {
		if m != nil && m.Role == RoleTool {
			results = append(results, m)
		}
	}
	if len(results) != 1 {
		t.Fatalf("want exactly one tool result on resume invoke, got %d: %+v", len(results), results)
	}
	if results[0].ToolCallID != "call_7Ge4wire" {
		t.Fatalf("tool result id = %q, want wire call_id", results[0].ToolCallID)
	}
	if results[0].Content != "wrote" {
		t.Fatalf("tool result = %q, want wrote", results[0].Content)
	}
	for _, m := range second {
		if m != nil && m.Role == RoleTool && m.ToolCallID == "fc_06f6item" {
			t.Fatalf("item-id phantom tool result must not be sent to the model: %+v", second)
		}
	}
}

// TestRun_toolCallKeyUsesCallIDWhenIDEmpty: CallID alone is enough for lifecycle ids.
func TestRun_customInstructionsInSystemPrompt(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192, SystemPrompt: "Always greet formally."},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	drainEvents(events)
	strategy.mu.Lock()
	prompts := append([]string(nil), strategy.systemPrompts...)
	strategy.mu.Unlock()
	found := false
	for _, p := range prompts {
		if strings.Contains(p, "Always greet formally.") {
			found = true
		}
	}
	if !found {
		t.Fatalf("system prompts missing custom instructions: %v", prompts)
	}
}

type testMCPCredentialResolver func(context.Context, string) (mcp.Credentials, error)

func (f testMCPCredentialResolver) ResolveMCP(ctx context.Context, ref string) (mcp.Credentials, error) {
	return f(ctx, ref)
}

// TestRun_mcpToolsDiscoveredAndInvokable: MCP configs inject callable tools
// into the turn, and Close runs discovery cleanup.
func TestRun_mcpToolsDiscoveredAndInvokable(t *testing.T) {
	var cleaned bool
	prev := discoverAllTools
	discoverAllTools = func(ctx context.Context, configs []mcp.MCPConfig, register mcpruntime.RegisterTool) func() {
		if len(configs) != 1 || len(configs[0].Headers) != 1 || configs[0].Headers[0].Value != "Bearer resolved" {
			t.Fatalf("resolved MCP configs = %#v", configs)
		}
		register("mcp_echo", "echo", "fake", nil, func(ctx context.Context, args map[string]any) (string, error) {
			return "from-mcp", nil
		})
		return func() { cleaned = true }
	}
	t.Cleanup(func() { discoverAllTools = prev })

	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "m1", CallID: "m1", Name: "mcp_echo", Namespace: "fake", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		MCPConfigs: []mcp.MCPConfig{
			{Name: "fake", Type: "http", URL: "http://127.0.0.1:1", CredentialRef: "vault://fake"},
		},
		MCPCredentialResolver: testMCPCredentialResolver(func(_ context.Context, ref string) (mcp.Credentials, error) {
			if ref != "vault://fake" {
				t.Fatalf("credential ref = %q", ref)
			}
			return mcp.Credentials{Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer resolved"}}}, nil
		}),
	})
	events, err := h.Run(context.Background(), "use mcp")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "from-mcp") {
		t.Fatalf("want MCP tool result, got %+v", summarizeEvents(got))
	}
	h.Close()
	if !cleaned {
		t.Fatal("expected MCP cleanup on Close")
	}
	// Second Close is a no-op.
	h.Close()
}

func TestRun_mcpCredentialResolveFailureSkipsServer(t *testing.T) {
	// Arrange
	var discovered int
	prev := discoverAllTools
	discoverAllTools = func(_ context.Context, configs []mcp.MCPConfig, _ mcpruntime.RegisterTool) func() {
		discovered = len(configs)
		return func() {}
	}
	t.Cleanup(func() { discoverAllTools = prev })

	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		MCPConfigs: []mcp.MCPConfig{
			{Name: "bad", Type: "http", URL: "http://127.0.0.1:1", CredentialRef: "vault://missing"},
		},
		MCPCredentialResolver: testMCPCredentialResolver(func(context.Context, string) (mcp.Credentials, error) {
			return mcp.Credentials{}, errors.New("missing secret")
		}),
	})
	t.Cleanup(h.Close)

	// Act
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	drainEvents(events)

	// Assert
	if discovered != 0 {
		t.Fatalf("discovered configs = %d", discovered)
	}
}

// TestRun_emptyToolInterceptorChain_allowsWriteWithoutPlan: replacing the
// built-in interceptor chain with an empty slice disables planning write lock.
func TestRun_watchdogRecordsToolResults(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "tool-out", nil },
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "t1", CallID: "t1", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	wd := &recordingWatchdog{}
	h := mustNewAgent(t, AgentOptions{
		Config:   Config{MaxWindowSize: 8192},
		Model:    strategy,
		Tools:    []*Tool{tool},
		WatchDog: wd,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	drainEvents(events)
	if len(wd.toolResults) == 0 {
		t.Fatal("expected RecordToolResult")
	}
}

// TestRun_displayNameOnFunctionCall_stream: DisplayName templates fill Title;
// Name stays programmatic so execution keeps working.
func TestRun_cancelMidTool_pairsCancelledResultsInWindow(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	slow := NewTool(ToolConfig{
		Name: "slow",
		Handler: func(ctx context.Context) (string, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return "", ctx.Err()
		},
	})
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "c_slow", CallID: "c_slow", Name: "slow", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*Tool{slow},
		SessionID: "steer-cancel-mid-tool",
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "run slow tool")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not start")
	}
	cancel()
	_ = drainEvents(events)

	loaded := reloadHarness(t, h, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*Tool{slow},
		SessionID: "steer-cancel-mid-tool",
	})
	var sawToolCancel bool
	hasCall := false
	for _, m := range loaded.Messages() {
		if m.Role == RoleAssistant && len(m.ToolCalls) > 0 {
			hasCall = true
		}
		if m.Role == RoleTool && m.ToolCallID == "c_slow" && strings.Contains(m.Content, "cancelled") {
			sawToolCancel = true
		}
	}
	if !hasCall {
		t.Fatalf("want assistant tool_calls in window, got %+v", loaded.Messages())
	}
	if !sawToolCancel {
		t.Fatalf("want cancelled tool result in checkpoint, got %+v", loaded.Messages())
	}
}

// TestRun_cancelAfterToolAnnounce_closesAnnouncedTools: cancel while the model
// is still streaming closes announced tool calls as cancelled.
func TestRun_cancelAfterToolAnnounce_closesAnnouncedTools(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "can1", CallID: "can1", Name: "ghost", Arguments: `{}`},
				},
				IsComplete: false,
			}
			once.Do(func() { close(started) })
			<-ctx.Done()
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "hi")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("invoke did not start")
	}
	// Let the function_call event be delivered, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()
	got := drainEvents(events)
	if !hasErrorIs(got, context.Canceled) && !hasToolResultContent(got, "cancelled") {
		t.Fatalf("want cancel outcome, got %+v", summarizeEvents(got))
	}
}

func summarizeEvents(events []StreamEvent) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		s := string(ev.Type)
		if ev.Content != "" {
			s += ":" + ev.Content
		}
		if ev.Error != nil {
			s += ":" + ev.Error.Error()
		}
		out = append(out, s)
	}
	return out
}

func TestTagModelAfterToolsError_wrapsProviderFailures(t *testing.T) {
	// Arrange
	base := errors.New("upstream failed")
	wrapped := fmt.Errorf("%w: already", ErrModelAfterTools)

	// Act
	fromError := tagModelAfterToolsError(LLMResponseChunk{Error: base})
	fromWrapped := tagModelAfterToolsError(LLMResponseChunk{Error: wrapped})
	fromContent := tagModelAfterToolsError(LLMResponseChunk{Content: "provider said no"})

	// Assert
	if !errors.Is(fromError.Error, ErrModelAfterTools) || fromError.Content == "" {
		t.Fatalf("from error = %+v", fromError)
	}
	if !errors.Is(fromWrapped.Error, ErrModelAfterTools) || strings.Count(fromWrapped.Error.Error(), "model request failed") != 1 {
		t.Fatalf("double wrap = %v", fromWrapped.Error)
	}
	if !errors.Is(fromContent.Error, ErrModelAfterTools) || !strings.Contains(fromContent.Content, "provider said no") {
		t.Fatalf("from content = %+v", fromContent)
	}
}
