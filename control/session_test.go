package control

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRaiseInterrupt_firstCallReturnsAsError(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "call_1"

	options := []UserChoice{{Title: "A"}, {Title: "B"}}
	payload, _ := json.Marshal(options)

	intr, err := rt.RaiseInterrupt("user_selection_choice", payload)
	if err == nil {
		t.Fatal("first RaiseInterrupt should return nil interrupt, non-nil error")
	}
	if intr != nil {
		t.Fatal("first RaiseInterrupt should return nil interrupt value")
	}
	if !rt.HasPendingInterrupt() {
		t.Fatal("PendingInterrupts should have one entry after raise")
	}
}

func TestRaiseInterrupt_resumeAfterResolveReturnsResolved(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "call_1"

	options := []UserChoice{{Title: "A"}, {Title: "B"}}
	payload, _ := json.Marshal(options)

	// First raise
	_, _ = rt.RaiseInterrupt("user_selection_choice", payload)

	// Consumer resolves with selection index 1
	resolution := `{"interruptId":"call_1","selectionIdx":1}`
	if _, err := rt.ReturnInterrupt("call_1", []byte(resolution)); err != nil {
		t.Fatalf("ReturnInterrupt failed: %v", err)
	}

	if rt.HasPendingInterrupt() {
		t.Fatal("PendingInterrupts should be empty after resolve")
	}

	// Second raise (re-execution): should return the resolved interrupt, no error
	intr, err := rt.RaiseInterrupt("user_selection_choice", payload)
	if err != nil {
		t.Fatalf("second RaiseInterrupt should return nil error, got: %v", err)
	}
	if intr == nil {
		t.Fatal("second RaiseInterrupt should return non-nil interrupt")
	}

	usi, ok := intr.(*UserSelectionInterrupt)
	if !ok {
		t.Fatalf("expected *UserSelectionInterrupt, got %T", intr)
	}
	if usi.ConfirmedChoice == nil {
		t.Fatal("ConfirmedChoice should be populated from resolution")
	}
	if usi.ConfirmedChoice.Title != "B" {
		t.Errorf("ConfirmedChoice.Title = %q, want 'B'", usi.ConfirmedChoice.Title)
	}
}

func TestRaiseInterrupt_unknownKindReturnsError(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "call_1"

	_, err := rt.RaiseInterrupt("bogus_kind", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for unknown interrupt type")
	}
}

func TestReturnInterrupt_notFoundReturnsError(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()

	_, err := rt.ReturnInterrupt("nonexistent", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for unknown interrupt id")
	}
}

func TestReturnInterrupt_invalidPayload_returnsError(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "invalid JSON",
			payload: `not json`,
			want:    "invalid JSON",
		},
		{
			name:    "missing selectionIdx",
			payload: `{"interruptId":"call_1"}`,
			want:    "missing required field",
		},
		{
			name:    "negative selectionIdx",
			payload: `{"interruptId":"call_1","selectionIdx":-1}`,
			want:    "out of range",
		},
		{
			name:    "out of bounds selectionIdx",
			payload: `{"interruptId":"call_1","selectionIdx":5}`,
			want:    "out of range",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := HarnessRuntime{}
			rt.EnsureInitialized()
			rt.CurrentToolCallID = "call_1"

			options := []UserChoice{{Title: "A"}, {Title: "B"}}
			payload, _ := json.Marshal(options)
			_, _ = rt.RaiseInterrupt("user_selection_choice", payload)

			_, err := rt.ReturnInterrupt("call_1", []byte(tt.payload))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want contains %q", err.Error(), tt.want)
			}
		})
	}
}

func TestInterruptMap_jsonRoundTrip(t *testing.T) {
	original := interruptMap{
		"call_1": &UserSelectionInterrupt{
			Options: []UserChoice{{Title: "A", Description: "desc", IsRecommended: true}},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var restored interruptMap
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}

	if len(restored) != 1 {
		t.Fatalf("restored map has %d entries, want 1", len(restored))
	}

	intr, ok := restored["call_1"]
	if !ok {
		t.Fatal("call_1 entry missing after round trip")
	}

	usi, ok := intr.(*UserSelectionInterrupt)
	if !ok {
		t.Fatalf("expected *UserSelectionInterrupt, got %T", intr)
	}
	if len(usi.Options) != 1 {
		t.Fatalf("Options len = %d, want 1", len(usi.Options))
	}
	if usi.Options[0].Title != "A" {
		t.Errorf("Options[0].Title = %q, want 'A'", usi.Options[0].Title)
	}
}

func TestStateGet_stateSetConcurrent(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()

	rt.StateSet("key", "value")
	v, ok := rt.StateGet("key")
	if !ok {
		t.Fatal("StateGet should find key")
	}
	if v != "value" {
		t.Errorf("StateGet = %v, want 'value'", v)
	}

	rt.StateDelete("key")
	if _, ok := rt.StateGet("key"); ok {
		t.Error("StateGet should return ok=false after delete")
	}
}
