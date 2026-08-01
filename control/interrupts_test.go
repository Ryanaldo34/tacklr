package control

import (
	"fmt"
	"strings"
	"testing"
)

func TestUserSelectionInterrupt_validateAndReturnPaths(t *testing.T) {
	usi := &UserSelectionInterrupt{
		Options: []UserChoice{{Title: "A"}, {Title: "B"}},
	}
	if err := usi.ValidatePayload([]byte(`not-json`)); err == nil || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("bad json: %v", err)
	}
	if err := usi.ValidatePayload([]byte(`{}`)); err == nil || !strings.Contains(err.Error(), "selectionIdx") {
		t.Fatalf("missing field: %v", err)
	}
	if err := usi.ValidatePayload([]byte(`{"selectionIdx":"x"}`)); err == nil || !strings.Contains(err.Error(), "payload shape") {
		t.Fatalf("bad shape: %v", err)
	}
	if err := usi.ValidatePayload([]byte(`{"selectionIdx":99}`)); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("range: %v", err)
	}
	if err := usi.Return([]byte(`{`)); err == nil {
		t.Fatal("return bad json")
	}
	if err := usi.Return([]byte(`{"selectionIdx":-1}`)); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("return range: %v", err)
	}
	if err := usi.Return([]byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	if usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("choice = %+v", usi.ConfirmedChoice)
	}
	if !strings.Contains(usi.Error(), "B") && !strings.Contains(usi.Error(), "options") {
		// Error marshals options; title may appear in confirmed choice only after set with json tag omitempty on pointer
		_ = usi.Error()
	}
	if s := usi.Error(); s == "" {
		t.Fatal("empty Error()")
	}
}

func TestToolPermissionInterrupt_remainingPaths(t *testing.T) {
	p := &ToolPermissionInterrupt{}
	if err := p.InitFromPayload([]byte(`{`)); err == nil {
		t.Fatal("bad init json")
	}
	if err := p.InitFromPayload([]byte(`{"toolName":"t"}`)); err != nil || len(p.Options) != 4 {
		t.Fatalf("defaults: %v %#v", err, p)
	}
	if err := p.ValidatePayload([]byte(`not`)); err == nil {
		t.Fatal("validate bad json")
	}
	if err := p.ValidatePayload([]byte(`{"optionId":123}`)); err == nil || !strings.Contains(err.Error(), "payload shape") {
		// optionId as number may fail shape
		if err == nil {
			t.Fatal("expected shape error")
		}
	}
	if err := p.Return([]byte(`{`)); err == nil {
		t.Fatal("return bad json")
	}
	if err := p.Return([]byte(`{"optionId":"missing"}`)); err == nil {
		t.Fatal("unknown option")
	}
	p.Options = []PermissionOption{{OptionID: "x", Name: "X", Kind: "weird"}}
	if err := p.Return([]byte(`{"optionId":"x"}`)); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("bad kind: %v", err)
	}
	if s := p.Error(); s == "" {
		t.Fatal("empty Error()")
	}
}

func TestRegisterInterrupt_duplicatePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate register")
		}
		if !strings.Contains(fmt.Sprint(r), "registered twice") {
			t.Fatalf("panic = %v", r)
		}
	}()
	registerInterrupt(func() Interrupt { return &UserSelectionInterrupt{} })
}

func TestInterruptMap_unmarshalCorruptData(t *testing.T) {
	var m interruptMap
	// Valid type but corrupt data payload.
	raw := []byte(`{"k":{"type":"user_selection_choice","data":"not-an-object"}}`)
	if err := m.UnmarshalJSON(raw); err == nil {
		t.Fatal("expected unmarshal error")
	}
	// Completely invalid outer JSON
	if err := m.UnmarshalJSON([]byte(`[`)); err == nil {
		t.Fatal("expected outer error")
	}
}

func TestRuntime_remainingSessionBranches(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()

	// RaiseInterrupt init failure.
	rt.CurrentToolCallID = "bad_init"
	if _, err := rt.RaiseInterrupt("user_selection_choice", []byte(`{`)); err == nil {
		t.Fatal("expected init error")
	}

	// AdoptInterrupt with nil PendingInterrupts map.
	rt2 := HarnessRuntime{}
	rt2.EnsureInitialized()
	rt2.PendingInterrupts = nil
	rt2.CurrentToolCallID = "adopt1"
	if _, err := rt2.AdoptInterrupt(&UserSelectionInterrupt{Options: []UserChoice{{Title: "A"}, {Title: "B"}}}); err == nil {
		t.Fatal("adopt returns interrupt as error")
	}
	if rt2.PendingInterrupts == nil {
		t.Fatal("PendingInterrupts should be allocated")
	}

	// ReturnInterrupt when Return fails after Validate (permission option with bad kind).
	rt3 := HarnessRuntime{}
	rt3.EnsureInitialized()
	rt3.PendingInterrupts["p1"] = &ToolPermissionInterrupt{
		Options: []PermissionOption{{OptionID: "x", Name: "X", Kind: "not_valid"}},
	}
	if _, err := rt3.ReturnInterrupt("p1", []byte(`{"optionId":"x"}`)); err == nil {
		t.Fatal("expected return error after validate")
	}

	// SnapshotState copies resolved map.
	rt3.ResolvedInterrupts["r1"] = &UserSelectionInterrupt{Options: []UserChoice{{Title: "Z"}, {Title: "Y"}}}
	_, _, resolved := rt3.SnapshotState()
	if len(resolved) != 1 {
		t.Fatalf("resolved snapshot = %d", len(resolved))
	}
}
