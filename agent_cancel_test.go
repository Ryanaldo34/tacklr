package tacklr

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
)

// TestAgent_HasOpenToolWorkAndFinalizeCancelled: parked/open tools → HasOpenToolWork;
// FinalizeCancelledWork pairs cancelled results and clears park state.
func TestAgent_HasOpenToolWorkAndFinalizeCancelled(t *testing.T) {
	h := mustNewAgent(t, AgentOptions{
		SessionID: "cancel-open-tools",
		Store:     stores.NewInMemoryStore(),
		Model:     &mockStrategy{},
	})
	t.Cleanup(h.Close)

	if h.hasOpenToolWork() {
		t.Fatal("fresh agent should have no open tool work")
	}

	// Unpaired assistant tool_call in the window
	h.restoreMessages([]*Message{
		{Role: RoleUser, Content: "do it"},
		{Role: RoleAssistant, Content: "", ToolCalls: []ToolCall{
			{ID: "c1", CallID: "c1", Name: "list", Arguments: `{}`},
		}},
	})
	if !h.hasOpenToolWork() {
		t.Fatal("unpaired tool_call should count as open work")
	}

	// Also pending map entry (same id seen once)
	h.pendingMu.Lock()
	h.pendingToolCalls["c1"] = stores.PendingToolCall{
		ToolCall: &ToolCall{ID: "c1", CallID: "c1", Name: "list", Arguments: `{}`},
	}
	h.pendingMu.Unlock()
	if !h.hasOpenToolWork() {
		t.Fatal("pending tool call should count")
	}

	h.finalizeCancelledWork(nil)

	if h.hasOpenToolWork() {
		t.Fatal("after finalize, no open tool work")
	}
	// Window should include cancelled tool result
	var sawCancelled bool
	for _, m := range h.Messages() {
		if m != nil && m.Role == RoleTool && m.ToolCallID == "c1" &&
			strings.Contains(m.Content, "cancelled") {
			sawCancelled = true
		}
	}
	if !sawCancelled {
		t.Fatalf("expected cancelled tool result in window: %+v", h.Messages())
	}

	// Empty tool call id is ignored by openToolCalls
	h.restoreMessages([]*Message{
		{Role: RoleAssistant, ToolCalls: []ToolCall{{Name: "x"}}}, // no WireID
	})
	if h.hasOpenToolWork() {
		t.Fatal("tool call without id should not count")
	}

	// Already paired: not open
	h.restoreMessages([]*Message{
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "c2", CallID: "c2", Name: "list"}}},
		{Role: RoleTool, ToolCallID: "c2", Content: "ok"},
	})
	if h.hasOpenToolWork() {
		t.Fatal("paired tool should not be open")
	}

	// Live-turn path: pairCancelledToolResults streams when out != nil
	h.restoreMessages([]*Message{
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "c3", CallID: "c3", Name: "list", Arguments: `{}`}}},
	})
	out := make(chan StreamEvent, 8)
	h.pairCancelledToolResults(out)
	close(out)
	var nEv int
	for range out {
		nEv++
	}
	var paired bool
	for _, m := range h.Messages() {
		if m != nil && m.Role == RoleTool && m.ToolCallID == "c3" &&
			strings.Contains(m.Content, "cancelled") {
			paired = true
		}
	}
	if !paired {
		t.Fatal("pairCancelled with out should pair cancelled tool result into window")
	}
	if nEv == 0 {
		t.Fatal("pairCancelled with out should emit at least one stream event")
	}
}
