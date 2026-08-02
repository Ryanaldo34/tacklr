package server

import (
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
)

func TestElicitation_paramsAndResultOutcomes(t *testing.T) {
	opts := []interrupt.UserChoice{{Title: "A"}, {Title: "B"}}
	if _, err := SelectionToElicitationParams("s", "tc", "", []interrupt.UserChoice{{Title: "only"}}); err == nil {
		t.Fatal("want error for <2 options")
	}
	if _, err := SelectionToElicitationParams("s", "tc", "Q", []interrupt.UserChoice{{Title: ""}, {Title: "B"}}); err == nil {
		t.Fatal("want error for empty title")
	}
	params, err := SelectionToElicitationParams("s", "tc", "Pick", opts)
	if err != nil {
		t.Fatal(err)
	}
	if params["message"] == nil || params["toolCallId"] != "tc" {
		t.Fatalf("params = %#v", params)
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
}
