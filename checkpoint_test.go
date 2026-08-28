package tacklr

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewCheckpoint_rejectsInvalidContextWindow(t *testing.T) {
	_, err := NewCheckpoint(
		[]*Message{{Role: RoleTool, Content: "missing id"}},
		nil, nil, nil, nil, nil,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid context window") {
		t.Fatalf("error = %v", err)
	}
}

func TestNewCheckpoint_marshalInterruptErrors(t *testing.T) {
	if _, err := NewCheckpoint(nil, nil, nil, nil, make(chan int), nil); err == nil {
		t.Fatal("expected pending interrupt marshal error")
	}
	if _, err := NewCheckpoint(nil, nil, nil, nil, nil, make(chan int)); err == nil {
		t.Fatal("expected resolved interrupt marshal error")
	}
}

func TestSessionCheckpoint_pendingToolCallRoundTrip(t *testing.T) {
	ptc := map[string]PendingToolCall{
		"fc_g": {
			ToolCall:        &ToolCall{ID: "fc_g", CallID: "call_g", Name: "gate"},
			InterruptActive: true,
		},
	}
	cp, err := NewCheckpoint([]*Message{{Role: RoleUser, Content: "hi"}}, ptc, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	var got SessionCheckpoint
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	g := got.PendingToolCalls()["fc_g"]
	if !g.InterruptActive || g.ToolCall == nil || g.ToolCall.Name != "gate" {
		t.Fatalf("gate pending: %+v", g)
	}
}

func TestSessionCheckpoint_copyHelpersAndJSON(t *testing.T) {
	cp, err := NewCheckpoint([]*Message{{Role: RoleUser, Content: "hi"}}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	v := cp.WithVersion(9)
	if v.Version() != 9 || cp.Version() != CheckpointVersion {
		t.Fatalf("version copy: got %d orig %d", v.Version(), cp.Version())
	}
	mod := v.WithModule("plan", json.RawMessage(`{"x":1}`))
	if string(mod.Modules()["plan"]) != `{"x":1}` {
		t.Fatalf("module = %s", mod.Modules()["plan"])
	}
	mod2 := mod.WithModule("onCall", json.RawMessage(`{}`))
	if len(mod2.Modules()) != 2 {
		t.Fatalf("modules = %v", mod2.Modules())
	}
	us := mod2.WithUserStateKey("k", json.RawMessage(`"v"`))
	if string(us.UserState()["k"]) != `"v"` {
		t.Fatalf("user state = %s", us.UserState()["k"])
	}
	us2 := us.WithUserStateKey("k2", json.RawMessage(`1`))
	if len(us2.UserState()) != 2 {
		t.Fatalf("user state keys = %v", us2.UserState())
	}
	var decoded SessionCheckpoint
	if err := json.Unmarshal([]byte(`{`), &decoded); err == nil {
		t.Fatal("want unmarshal error")
	}
	if err := decoded.UnmarshalJSON([]byte("not-json")); err == nil {
		t.Fatal("want UnmarshalJSON error")
	}
}
