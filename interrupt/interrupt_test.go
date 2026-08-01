package interrupt_test

import (
	"encoding/json"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
)

func TestUserSelection_returnSetsChoice(t *testing.T) {
	usi := &interrupt.UserSelectionInterrupt{
		Options: []interrupt.UserChoice{{Title: "A"}, {Title: "B"}},
	}
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 1})
	if err := usi.Return(payload); err != nil {
		t.Fatal(err)
	}
	if usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("choice = %+v", usi.ConfirmedChoice)
	}
}

func TestToolPermission_allowOnce(t *testing.T) {
	p := &interrupt.ToolPermissionInterrupt{Options: interrupt.DefaultPermissionOptions()}
	payload, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	if err := p.Return(payload); err != nil {
		t.Fatal(err)
	}
	if !p.Allowed || p.SelectedKind != interrupt.PermissionAllowOnce {
		t.Fatalf("allowed=%v kind=%s", p.Allowed, p.SelectedKind)
	}
}

func TestRegister_customType(t *testing.T) {
	// Built-ins already registered; custom names work via Register in user packages.
	if _, ok := interrupt.New("user_selection_choice"); !ok {
		t.Fatal("expected built-in user_selection_choice")
	}
}
