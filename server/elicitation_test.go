package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// TestElicitation_paramsAndResultOutcomes covers selection elicitation
// param/result return paths once each.
func TestElicitation_paramsAndResultOutcomes(t *testing.T) {
	opts := []interrupt.UserChoice{{Title: "A", Description: "first", IsRecommended: true}, {Title: "B", Description: "second"}}

	if _, err := SelectionToElicitationParams("s", "tc", "", []interrupt.UserChoice{{Title: "only"}}); err == nil {
		t.Fatal("want error for <2 options")
	}
	if _, err := SelectionToElicitationParams("s", "tc", "Q", []interrupt.UserChoice{{Title: ""}, {Title: "B"}}); err == nil {
		t.Fatal("want error for empty title")
	}
	params, err := SelectionToElicitationParams("sess1", "tc1", "Pick one", opts)
	if err != nil {
		t.Fatal(err)
	}
	if params["mode"] != "form" || params["sessionId"] != "sess1" || params["toolCallId"] != "tc1" {
		t.Fatalf("params = %#v", params)
	}
	msg, _ := params["message"].(string)
	for _, part := range []string{"Pick one", "A", "recommended", "B"} {
		if !strings.Contains(msg, part) {
			t.Fatalf("message missing %q: %q", part, msg)
		}
	}

	action, res, err := ElicitationResultToSelectionPayload([]byte(`{"action":"decline"}`), opts)
	if err != nil || action != "decline" || res != nil {
		t.Fatalf("decline: action=%s res=%s err=%v", action, res, err)
	}
	action, res, err = ElicitationResultToSelectionPayload([]byte(`{"action":"cancel"}`), opts)
	if err != nil || action != "cancel" || res != nil {
		t.Fatalf("cancel: action=%s res=%s err=%v", action, res, err)
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{"action":"accept","content":{}}`), opts); err == nil {
		t.Fatal("accept without choice")
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{"action":"accept","content":{"choice":"Z"}}`), opts); err == nil {
		t.Fatal("unknown choice")
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{"action":"wat"}`), opts); err == nil {
		t.Fatal("unknown action")
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{`), opts); err == nil {
		t.Fatal("bad json")
	}
	_, res, err = ElicitationResultToSelectionPayload([]byte(`{"action":"accept","content":{"choice":"A"}}`), opts)
	if err != nil || res == nil {
		t.Fatalf("accept A: %v %s", err, res)
	}
	rawB, _ := json.Marshal(map[string]any{
		"action":  "accept",
		"content": map[string]any{"choice": "B"},
	})
	action, res, err = ElicitationResultToSelectionPayload(rawB, opts)
	if err != nil || action != "accept" {
		t.Fatalf("accept B: action=%s err=%v", action, err)
	}
	var payload map[string]any
	_ = json.Unmarshal(res, &payload)
	if int(payload["selectionIdx"].(float64)) != 1 {
		t.Fatalf("payload = %v", payload)
	}
}
