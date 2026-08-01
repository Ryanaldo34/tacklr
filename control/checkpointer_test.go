package control

import (
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestCheckpointer_captureApply_roundTrip(t *testing.T) {
	sm := NewSessionManager()
	sm.Plan().SetDocument("CoS: ship")
	sm.Plan().Set([]Todo{{Title: "A", Status: streaming.TodoStatusInProgress}})
	sm.stateSet("user_flag", true)
	rt := NewRuntime(nil, nil, sm)
	rt.CurrentToolCallID = "tc1"
	_, err := rt.RaiseInterrupt("tool_permission", []byte(`{"toolName":"x"}`))
	if err == nil {
		t.Fatal("expected interrupt as error")
	}

	window := []*streaming.Message{{Role: streaming.RoleUser, Content: "hi"}}
	ptc := map[string]stores.PendingToolCall{
		"tc1": {InterruptActive: true},
	}
	itr := map[string]string{"intr1": "tc1"}

	cp, err := NewCheckpointer().Capture(window, sm, ptc, itr)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.ContextWindow) != 1 {
		t.Fatalf("window = %d", len(cp.ContextWindow))
	}
	if _, ok := cp.State.RuntimeState["_plan"]; !ok {
		t.Fatal("checkpoint missing plan key")
	}
	if cp.State.RuntimeState["user_flag"] != true {
		t.Fatalf("user state = %v", cp.State.RuntimeState["user_flag"])
	}
	if len(cp.State.PendingInterrupts) == 0 {
		t.Fatal("expected pending interrupts JSON")
	}

	sm2 := NewSessionManager()
	applied, err := NewCheckpointer().Apply(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Window) != 1 || applied.Window[0].Content != "hi" {
		t.Fatalf("window = %+v", applied.Window)
	}
	if !sm2.HasActivePlan() || sm2.Plan().Document() != "CoS: ship" {
		t.Fatalf("plan restore failed: active=%v doc=%q", sm2.HasActivePlan(), sm2.Plan().Document())
	}
	if v, ok := sm2.stateGet("user_flag"); !ok || v != true {
		t.Fatalf("user_flag = %v %v", v, ok)
	}
	if !sm2.hasPendingInterrupt() {
		t.Fatal("pending interrupt not restored")
	}
	if applied.PendingToolCalls["tc1"].InterruptActive != true {
		t.Fatalf("ptc = %+v", applied.PendingToolCalls)
	}
	if applied.InterruptToRequester["intr1"] != "tc1" {
		t.Fatalf("itr = %+v", applied.InterruptToRequester)
	}
}
