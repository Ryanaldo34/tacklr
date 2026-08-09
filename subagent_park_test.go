package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
)

// TestRun_usesDefaultContextPolicyWhenCleared: zeroing policy after construct
// still allows a successful turn (DefaultContextPolicy applies).
func TestRun_usesDefaultContextPolicyWhenCleared(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	h.contextPolicy = ContextPolicy{}
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasEventType(got, StreamEventComplete) {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

// TestRun_watchdogRecordErrorIsNonFatal: watchdog failures do not fail the turn.
func TestRun_watchdogRecordErrorIsNonFatal(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "out", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config:   Config{MaxWindowSize: 8192},
		Model:    strategy,
		WatchDog: failingWatchdog{},
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasEventType(got, StreamEventComplete) {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

type failingWatchdog struct{}

func (failingWatchdog) RecordThinking(*Message) error  { return errors.New("wd") }
func (failingWatchdog) RecordOutput(*Message) error    { return errors.New("wd") }
func (failingWatchdog) RecordError(error) error        { return errors.New("wd") }
func (failingWatchdog) RecordTokens(int, int) error    { return errors.New("wd") }
func (failingWatchdog) RecordToolCalls(*Message) error { return errors.New("wd") }
func (failingWatchdog) RecordToolResult(*Message) error {
	return errors.New("wd")
}

// TestSpawnWorker_parkMetaLoadShapes: park metadata rehydrates from string,
// []byte, typed map, and map[string]any (checkpoint shapes).
func TestSpawnWorker_parkMetaLoadShapes(t *testing.T) {
	meta := parkedWorkerMeta{
		WorkerName:        "researcher",
		WorkerSessionID:   "w/sess",
		Task:              "task",
		ChildInterruptIDs: []string{"c1"},
	}
	parks := map[string]parkedWorkerMeta{"tc": meta}
	b, err := json.Marshal(parks)
	if err != nil {
		t.Fatal(err)
	}

	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: &mockStrategy{}},
		},
	})
	ps := parkStore{h: h}

	// string
	turnRuntime(h).StateSet(parkedWorkersStateKey, string(b))
	if got := ps.load(); got["tc"].WorkerName != "researcher" {
		t.Fatalf("string load: %+v", got)
	}
	// []byte
	turnRuntime(h).StateSet(parkedWorkersStateKey, b)
	if got := ps.load(); got["tc"].Task != "task" {
		t.Fatalf("bytes load: %+v", got)
	}
	// typed map
	turnRuntime(h).StateSet(parkedWorkersStateKey, parks)
	if got := ps.load(); len(got["tc"].ChildInterruptIDs) != 1 {
		t.Fatalf("typed map: %+v", got)
	}
	// map[string]any (JSON round-trip shape)
	var anyMap map[string]any
	if err := json.Unmarshal(b, &anyMap); err != nil {
		t.Fatal(err)
	}
	turnRuntime(h).StateSet(parkedWorkersStateKey, anyMap)
	if got := ps.load(); got["tc"].WorkerSessionID != "w/sess" {
		t.Fatalf("any map: %+v", got)
	}
	// corrupt string → empty
	turnRuntime(h).StateSet(parkedWorkersStateKey, "{not-json")
	if got := ps.load(); len(got) != 0 {
		t.Fatalf("corrupt: %+v", got)
	}
	// store empty deletes key
	ps.store(map[string]parkedWorkerMeta{})
	if _, ok := turnRuntime(h).StateGet(parkedWorkersStateKey); ok {
		t.Fatal("empty store should delete key")
	}
	// store non-empty
	ps.store(parks)
	if _, ok := turnRuntime(h).StateGet(parkedWorkersStateKey); !ok {
		t.Fatal("expected park state set")
	}
}

// TestSpawnWorker_resumeMissingPayload_errors: parked meta without resolution payloads fails resume.
func TestSpawnWorker_resumeMissingPayload_errors(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: &mockStrategy{
				invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
					ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "x", IsComplete: true}
				},
			}},
		},
		Store: testStore(t),
	})
	h.sessionId = "parent"
	rt := turnRuntime(h)
	rt.CurrentToolCallID = "spawn_x"
	// Plant park meta without child resolution payloads.
	parkStore{h: h}.set("spawn_x", parkedWorkerMeta{
		WorkerName:        "researcher",
		WorkerSessionID:   workerSessionID(h.sessionId, "researcher", "spawn_x"),
		Task:              "resume me",
		ChildInterruptIDs: []string{"missing-child"},
	}, nil)
	// Resume uses CurrentToolCallID to load park meta.
	_, err := h.runWorker(context.Background(), "researcher", "resume me", rt)
	if err == nil {
		t.Fatal("want resume error")
	}
	if !strings.Contains(err.Error(), "resume") && !strings.Contains(err.Error(), "payload") && !strings.Contains(err.Error(), "park") {
		t.Fatalf("err = %v", err)
	}
}

// TestParseInterruptID_shapes: empty/invalid interrupt envelopes yield no id;
// well-formed data returns the interrupt id used for park/resume.
func TestParseInterruptID_shapes(t *testing.T) {
	if parseInterruptID(nil) != "" {
		t.Fatal("empty id")
	}
	if parseInterruptID([]byte(`{`)) != "" {
		t.Fatal("bad json")
	}
	if parseInterruptID([]byte(`{"interruptId":"x"}`)) != "x" {
		t.Fatal("want x")
	}
}

// TestReturnFromInterrupt_nilPayloadMap_initializes: first resume with nil map still works.
func TestReturnFromInterrupt_nilPayloadMap_initializes(t *testing.T) {
	optionsJSON := `[{"title":"A","description":"","isRecommended":true},{"title":"B","description":"","isRecommended":false}]`
	tool := NewTool(ToolConfig{
		Name: "ask",
		Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			return intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "a1", CallID: "a1", Name: "ask", Arguments: `{}`,
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
		Tools:  []*Tool{tool},
		Store:  testStore(t),
	})
	h.sessionId = "ret-nil"
	events, err := h.Run(context.Background(), "q")
	if err != nil {
		t.Fatal(err)
	}
	var iid string
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			var env struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &env)
			iid = env.InterruptId
		}
	}
	if iid == "" {
		t.Fatal("no interrupt")
	}
	h.interruptPayloads = nil // force re-init branch
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		iid: []byte(`{"selectionIdx":0}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(resumed)
	if !hasToolResultContent(got, "A") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

// TestPermissionRemember_boolMapMerge: second allow_always keeps prior tools.
func TestPermissionRemember_boolMapMerge(t *testing.T) {
	// Outcome via two tools with allow_always on the first, then second also allowed
	// after rehydrate as map[string]bool (native remember shape).
	t1 := NewTool(ToolConfig{
		Name:               "t1",
		PermissionRequired: true,
		Handler:            func(ctx context.Context) (string, error) { return "1", nil },
	})
	t2 := NewTool(ToolConfig{
		Name:               "t2",
		PermissionRequired: true,
		Handler:            func(ctx context.Context) (string, error) { return "2", nil },
	})
	// Pre-seed bool map with both allowed.
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model: &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "1", CallID: "1", Name: "t1", Arguments: `{}`},
						{ID: "2", CallID: "2", Name: "t2", Arguments: `{}`},
					},
					IsComplete: true,
				}
			},
		},
		Tools: []*Tool{t1, t2},
	})
	// After first remember cycle, shape is map[string]bool — seed both.
	turnRuntime(h).StateSet(permissionAlwaysAllowKey, map[string]bool{"t1": true, "t2": true})
	// One more remember merge path
	permissionRemember(turnRuntime(h), permissionAlwaysAllowKey, "t3")
	if !permissionSetHas(turnRuntime(h), permissionAlwaysAllowKey, "t1") || !permissionSetHas(turnRuntime(h), permissionAlwaysAllowKey, "t3") {
		t.Fatal("bool map merge lost entries")
	}
	// unknown type default
	turnRuntime(h).StateSet(permissionAlwaysAllowKey, 42)
	if permissionSetHas(turnRuntime(h), permissionAlwaysAllowKey, "t1") {
		t.Fatal("unknown type should miss")
	}
	// remember over unknown starts fresh
	permissionRemember(turnRuntime(h), permissionAlwaysAllowKey, "fresh")
	if !permissionSetHas(turnRuntime(h), permissionAlwaysAllowKey, "fresh") {
		t.Fatal("fresh")
	}
	// any-map merge
	turnRuntime(h).StateSet(permissionAlwaysDenyKey, map[string]any{"bad": true})
	permissionRemember(turnRuntime(h), permissionAlwaysDenyKey, "also")
	if !permissionSetHas(turnRuntime(h), permissionAlwaysDenyKey, "bad") || !permissionSetHas(turnRuntime(h), permissionAlwaysDenyKey, "also") {
		t.Fatal("any-map merge")
	}
}

// TestNewTool_depthLimitAndTimeFields: deeply nested schema degrades safely;

// Ensure stores import used if needed — checkpoint type reference.
var _ stores.BaseStore = (*stores.InMemoryStore)(nil)
