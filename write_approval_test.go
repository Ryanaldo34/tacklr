package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
)

func writeApprovalTool(t *testing.T, seen *string, calls *int) *Tool {
	t.Helper()
	return NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		Handler: func(ctx context.Context, args struct {
			Path string `json:"path"`
		}) (string, error) {
			*calls++
			*seen = args.Path
			return "wrote:" + args.Path, nil
		},
	})
}

func runUntilYield(t *testing.T, ch <-chan StreamEvent) (interruptID, kind string) {
	t.Helper()
	for ev := range ch {
		if ev.Type != StreamEventInterrupt {
			continue
		}
		var payload struct {
			InterruptId string `json:"interruptId"`
			Type        string `json:"type"`
		}
		if err := json.Unmarshal(ev.Data, &payload); err != nil {
			t.Fatal(err)
		}
		return payload.InterruptId, payload.Type
	}
	t.Fatal("expected write_approval yield")
	return "", ""
}

func oneShotWriteStrategy(t *testing.T, args string) *mockStrategy {
	t.Helper()
	var n int
	return &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "w1", CallID: "w1", Name: "mutate", Arguments: args},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
}

// TestWriteApproval_parksUntilApprove: write parks, handler stays inert, approve
// runs original args and records the decision.
func TestWriteApproval_parksUntilApprove(t *testing.T) {
	var seen string
	var calls int
	tool := writeApprovalTool(t, &seen, &calls)
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		Model:               oneShotWriteStrategy(t, `{"path":"/orig"}`),
		Tools:               []*Tool{tool},
		DisablePlanningLock: true,
	})

	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, kind := runUntilYield(t, ch)
	if kind != WriteApprovalType {
		t.Fatalf("kind=%q", kind)
	}
	if calls != 0 {
		t.Fatal("handler must not run before approve")
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"approve"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var wrote string
	for ev := range ch2 {
		if ev.Type == StreamEventToolResult {
			wrote = ev.Content
		}
	}
	if calls != 1 || seen != "/orig" || wrote != "wrote:/orig" {
		t.Fatalf("calls=%d seen=%q wrote=%q", calls, seen, wrote)
	}
	recs := ah.WriteApprovals()
	if len(recs) != 1 || recs[0].Action != WriteApprovalApprove || recs[0].ToolName != "mutate" || recs[0].Args != `{"path":"/orig"}` || recs[0].UnixTime == 0 {
		t.Fatalf("audit = %+v", recs)
	}
}

// TestWriteApproval_rejectDeniesWrite: reject leaves the handler inert and
// reports a permission denial.
func TestWriteApproval_rejectDeniesWrite(t *testing.T) {
	var seen string
	var calls int
	tool := writeApprovalTool(t, &seen, &calls)
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		Model:               oneShotWriteStrategy(t, `{"path":"/nope"}`),
		Tools:               []*Tool{tool},
		DisablePlanningLock: true,
	})

	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := runUntilYield(t, ch)
	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"reject"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var denied bool
	for ev := range ch2 {
		if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, "permission denied") {
			denied = true
		}
	}
	if calls != 0 || seen != "" {
		t.Fatalf("handler ran: calls=%d seen=%q", calls, seen)
	}
	if !denied {
		t.Fatal("expected permission denial after reject")
	}
	recs := ah.WriteApprovals()
	if len(recs) != 1 || recs[0].Action != WriteApprovalReject {
		t.Fatalf("audit = %+v", recs)
	}
}

// TestWriteApproval_editReplacesArgs: edit runs the handler with replacement args.
func TestWriteApproval_editReplacesArgs(t *testing.T) {
	var seen string
	var calls int
	tool := writeApprovalTool(t, &seen, &calls)
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		Model:               oneShotWriteStrategy(t, `{"path":"/orig"}`),
		Tools:               []*Tool{tool},
		DisablePlanningLock: true,
	})

	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := runUntilYield(t, ch)
	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"edit","args":"{\"path\":\"/edited\"}"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var wrote string
	for ev := range ch2 {
		if ev.Type == StreamEventToolResult {
			wrote = ev.Content
		}
	}
	if calls != 1 || seen != "/edited" || wrote != "wrote:/edited" {
		t.Fatalf("calls=%d seen=%q wrote=%q", calls, seen, wrote)
	}
	recs := ah.WriteApprovals()
	if len(recs) != 1 || recs[0].Action != WriteApprovalEdit || recs[0].Args != `{"path":"/edited"}` {
		t.Fatalf("audit = %+v", recs)
	}
}

// TestWriteApproval_checkpointResumeApproves: a parked write survives reload
// and completes after approve.
func TestWriteApproval_checkpointResumeApproves(t *testing.T) {
	var seen string
	var calls int
	tool := writeApprovalTool(t, &seen, &calls)
	store := stores.NewInMemoryStore()
	opts := AgentOptions{
		SessionID:           "write-appr-reload",
		Config:              Config{MaxWindowSize: 8192},
		Model:               oneShotWriteStrategy(t, `{"path":"/ckpt"}`),
		Store:               store,
		Tools:               []*Tool{tool},
		DisablePlanningLock: true,
	}
	ah := mustNewAgent(t, opts)
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := runUntilYield(t, ch)
	ah.Close()

	reloaded, err := NewAgentFromSession(context.Background(), "write-appr-reload", opts)
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := reloaded.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"approve"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}
	if calls != 1 || seen != "/ckpt" {
		t.Fatalf("calls=%d seen=%q", calls, seen)
	}
}

// TestWriteApproval_planningLockFailsFirst: no plan yields a lock error, not
// a write_approval interrupt.
func TestWriteApproval_planningLockFailsFirst(t *testing.T) {
	var calls int
	var seen string
	tool := writeApprovalTool(t, &seen, &calls)
	ah := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  oneShotWriteStrategy(t, `{"path":"/x"}`),
		Tools:  []*Tool{tool},
	})
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	var locked bool
	var yielded string
	for ev := range ch {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			yielded = payload.Type
		}
		if ev.Type == StreamEventToolResult && (strings.Contains(ev.Content, "locked") || strings.Contains(ev.Content, "permission denied")) {
			locked = true
		}
	}
	if yielded != "" {
		t.Fatalf("unexpected yield %q", yielded)
	}
	if !locked || calls != 0 {
		t.Fatalf("locked=%v calls=%d", locked, calls)
	}
}

// TestWriteApproval_thenPermissionRequired: after approve, PermissionRequired
// still yields tool_permission.
func TestWriteApproval_thenPermissionRequired(t *testing.T) {
	var calls int
	tool := NewTool(ToolConfig{
		Name:               "mutate",
		Access:             ToolWriteAccess,
		PermissionRequired: true,
		Handler: func(ctx context.Context) (string, error) {
			calls++
			return "ok", nil
		},
	})
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		Model:               oneShotWriteStrategy(t, `{}`),
		Tools:               []*Tool{tool},
		DisablePlanningLock: true,
	})
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, kind := runUntilYield(t, ch)
	if kind != WriteApprovalType {
		t.Fatalf("first yield = %q", kind)
	}
	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"approve"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	id2, kind2 := runUntilYield(t, ch2)
	if kind2 != "tool_permission" {
		t.Fatalf("second yield = %q", kind2)
	}
	if calls != 0 {
		t.Fatal("handler ran before permission")
	}
	ch3, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id2: []byte(`{"optionId":"allow-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch3 {
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}

// TestWriteApproval_disableSkipsGate: DisableWriteApproval lets a write run
// without a yield.
func TestWriteApproval_disableSkipsGate(t *testing.T) {
	var seen string
	var calls int
	tool := writeApprovalTool(t, &seen, &calls)
	ah := mustNewAgent(t, AgentOptions{
		Config:               Config{MaxWindowSize: 8192},
		Model:                oneShotWriteStrategy(t, `{"path":"/fast"}`),
		Tools:                []*Tool{tool},
		DisablePlanningLock:  true,
		DisableWriteApproval: true,
	})
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	var yielded bool
	var wrote string
	for ev := range ch {
		if ev.Type == StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == StreamEventToolResult {
			wrote = ev.Content
		}
	}
	if yielded {
		t.Fatal("disabled gate must not yield")
	}
	if calls != 1 || wrote != "wrote:/fast" {
		t.Fatalf("calls=%d wrote=%q", calls, wrote)
	}
}
