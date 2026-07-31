package server

import (
	"encoding/json"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
)

// Thin wire-shape checks for paths that protocol integration tests do not hit
// directly (cancelled outcome, empty tool title fallback, empty options default).

func TestRequestPermissionResultToPayload_cancelled(t *testing.T) {
	raw := json.RawMessage(`{"outcome":{"outcome":"cancelled"}}`)
	_, cancelled, err := RequestPermissionResultToPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !cancelled {
		t.Fatal("expected cancelled")
	}
	if _, _, err := RequestPermissionResultToPayload(json.RawMessage(`{`)); err == nil {
		t.Fatal("bad json")
	}
	if _, _, err := RequestPermissionResultToPayload(json.RawMessage(`{"outcome":{"outcome":"weird"}}`)); err == nil {
		t.Fatal("unknown outcome")
	}
	if _, _, err := RequestPermissionResultToPayload(json.RawMessage(`{"outcome":{"outcome":"selected"}}`)); err == nil {
		t.Fatal("selected without optionId")
	}
}

func TestParseInterruptEnvelope_missingID(t *testing.T) {
	if _, err := ParseInterruptEnvelope([]byte(`{"type":"x","data":{}}`)); err == nil {
		t.Fatal("expected missing interruptId")
	}
}

func TestPermissionToACPParams_emptyToolName(t *testing.T) {
	params := PermissionToACPParams("sess", "call_1", control.ToolPermissionInterrupt{
		Options: control.DefaultPermissionOptions(),
	})
	tc := params["toolCall"].(map[string]any)
	if tc["title"] != "Tool call" {
		t.Fatalf("title = %v", tc["title"])
	}
	opts := params["options"].([]map[string]any)
	if len(opts) != 4 {
		t.Fatalf("options len = %d, want 4", len(opts))
	}
}

func TestParseToolPermissionFromInterruptData_defaultsOptions(t *testing.T) {
	// Empty options in serialized data should fall back to defaults.
	serialized, _ := json.Marshal(map[string]any{"toolName": "x", "options": []any{}})
	envelope, _ := json.Marshal(map[string]any{
		"interruptId": "intr-p",
		"type":        "tool_permission",
		"data":        json.RawMessage(serialized),
	})
	id, got, err := ParseToolPermissionFromInterruptData(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if id != "intr-p" || len(got.Options) != 4 {
		t.Fatalf("id=%q options=%d", id, len(got.Options))
	}
}
