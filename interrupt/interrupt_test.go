package interrupt_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// TestUserSelection_fullLifecycle covers serialize, validate, return, and error string.
func TestUserSelection_fullLifecycle(t *testing.T) {
	usi := &interrupt.UserSelectionInterrupt{
		Options: []interrupt.UserChoice{
			{Title: "A", Description: "first", IsRecommended: true},
			{Title: "B", Description: "second"},
		},
	}
	if usi.TypeName() != "user_selection_choice" {
		t.Fatal(usi.TypeName())
	}
	raw, err := usi.Serialize()
	if err != nil || len(raw) == 0 {
		t.Fatalf("serialize: %v %s", err, raw)
	}
	if usi.Error() == "" {
		t.Fatal("error string")
	}

	if err := usi.ValidatePayload([]byte(`not-json`)); err == nil {
		t.Fatal("invalid json")
	}
	if err := usi.ValidatePayload([]byte(`{}`)); err == nil || !strings.Contains(err.Error(), "selectionIdx") {
		t.Fatalf("missing field: %v", err)
	}
	if err := usi.ValidatePayload([]byte(`{"selectionIdx":99}`)); err == nil {
		t.Fatal("out of range validate")
	}
	if err := usi.ValidatePayload([]byte(`{"selectionIdx":"x"}`)); err == nil {
		t.Fatal("selectionIdx wrong type")
	}
	if err := usi.ValidatePayload([]byte(`{"selectionIdx":0}`)); err != nil {
		t.Fatal(err)
	}

	if err := usi.Return([]byte(`not-json`)); err == nil {
		t.Fatal("return bad json")
	}
	if err := usi.Return([]byte(`{"selectionIdx":-1}`)); err == nil {
		t.Fatal("return out of range")
	}
	if err := usi.Return([]byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	if usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("choice = %+v", usi.ConfirmedChoice)
	}

	// InitFromPayload replaces options list.
	fresh := &interrupt.UserSelectionInterrupt{}
	if err := fresh.InitFromPayload([]byte(`[{"title":"only"}]`)); err != nil {
		t.Fatal(err)
	}
	if len(fresh.Options) != 1 || fresh.Options[0].Title != "only" {
		t.Fatalf("%+v", fresh.Options)
	}
}

// TestToolPermission_allKinds covers init, validate, allow/reject, and unknown option.
func TestToolPermission_allKinds(t *testing.T) {
	p := &interrupt.ToolPermissionInterrupt{}
	if p.Predecided() {
		t.Fatal("zero value is not predecided")
	}
	if p.TypeName() != "tool_permission" {
		t.Fatal(p.TypeName())
	}
	if _, err := p.Serialize(); err != nil {
		t.Fatal(err)
	}
	if p.Error() == "" {
		t.Fatal("error string")
	}

	// Empty / null payload → default options.
	if err := p.InitFromPayload(nil); err != nil {
		t.Fatal(err)
	}
	if len(p.Options) != 4 {
		t.Fatalf("defaults = %d", len(p.Options))
	}
	if err := p.InitFromPayload([]byte("null")); err != nil {
		t.Fatal(err)
	}
	if err := p.InitFromPayload([]byte(`{`)); err == nil {
		t.Fatal("bad init json")
	}
	if err := p.InitFromPayload([]byte(`{"toolName":"rm","options":[{"optionId":"custom","name":"C","kind":"allow_once"}]}`)); err != nil {
		t.Fatal(err)
	}
	if p.ToolName != "rm" || len(p.Options) != 1 {
		t.Fatalf("%+v", p)
	}
	// Empty options in payload → defaults again.
	if err := p.InitFromPayload([]byte(`{"toolName":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if len(p.Options) != 4 {
		t.Fatal("defaults after empty options")
	}

	if err := p.ValidatePayload([]byte(`x`)); err == nil {
		t.Fatal("bad json")
	}
	if err := p.ValidatePayload([]byte(`{}`)); err == nil {
		t.Fatal("missing optionId")
	}
	if err := p.ValidatePayload([]byte(`{"optionId":"nope"}`)); err == nil {
		t.Fatal("unknown option validate")
	}
	if err := p.ValidatePayload([]byte(`{"optionId":1}`)); err == nil {
		t.Fatal("optionId wrong type")
	}
	if err := p.ValidatePayload([]byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}

	if err := p.Return([]byte(`not-json`)); err == nil {
		t.Fatal("return bad json")
	}
	if err := p.Return([]byte(`{"optionId":"missing"}`)); err == nil {
		t.Fatal("unknown option return")
	}

	cases := []struct {
		id      string
		allowed bool
		kind    string
	}{
		{"allow-once", true, interrupt.PermissionAllowOnce},
		{"allow-always", true, interrupt.PermissionAllowAlways},
		{"reject-once", false, interrupt.PermissionRejectOnce},
		{"reject-always", false, interrupt.PermissionRejectAlways},
	}
	for _, tc := range cases {
		p2 := &interrupt.ToolPermissionInterrupt{Options: interrupt.DefaultPermissionOptions()}
		payload, _ := json.Marshal(map[string]string{"optionId": tc.id})
		if err := p2.Return(payload); err != nil {
			t.Fatal(err)
		}
		if p2.Allowed != tc.allowed || p2.SelectedKind != tc.kind || p2.SelectedOptionID != tc.id {
			t.Fatalf("%s: allowed=%v kind=%s id=%s", tc.id, p2.Allowed, p2.SelectedKind, p2.SelectedOptionID)
		}
		if p2.CallDenied() == tc.allowed || !p2.Predecided() {
			t.Fatalf("%s: CallDenied=%v predecided=%v", tc.id, p2.CallDenied(), p2.Predecided())
		}
	}

	// Unknown kind on a custom option.
	p3 := &interrupt.ToolPermissionInterrupt{
		Options: []interrupt.PermissionOption{{OptionID: "weird", Name: "W", Kind: "nope"}},
	}
	if err := p3.Return([]byte(`{"optionId":"weird"}`)); err == nil {
		t.Fatal("unknown kind")
	}
}

// TestRegister_New_Clone covers registry and deep clone outcomes.
func TestRegister_New_Clone(t *testing.T) {
	if _, ok := interrupt.New("user_selection_choice"); !ok {
		t.Fatal("builtin selection")
	}
	if _, ok := interrupt.New("tool_permission"); !ok {
		t.Fatal("builtin permission")
	}
	if _, ok := interrupt.New("missing_type"); ok {
		t.Fatal("unknown type")
	}

	src := &interrupt.UserSelectionInterrupt{
		Options: []interrupt.UserChoice{{Title: "A"}},
	}
	cp := interrupt.Clone(src)
	if cp == nil {
		t.Fatal("clone")
	}
	cloned, ok := cp.(*interrupt.UserSelectionInterrupt)
	if !ok || len(cloned.Options) != 1 || cloned.Options[0].Title != "A" {
		t.Fatalf("%+v", cp)
	}
	// Mutate original; clone stays independent after re-serialize path.
	src.Options[0].Title = "mutated"
	if cloned.Options[0].Title != "A" {
		t.Fatal("clone should be independent")
	}

	if interrupt.Clone(nil) != nil {
		t.Fatal("nil clone")
	}

	// Unregistered type name via fake interrupt.
	fake := fakeInterrupt{name: "not_in_registry"}
	if interrupt.Clone(fake) != nil {
		t.Fatal("clone unknown type")
	}

	// Double-register panics.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("want panic on double register")
		}
	}()
	interrupt.Register(func() interrupt.Interrupt { return &interrupt.UserSelectionInterrupt{} })
}

type fakeInterrupt struct{ name string }

func (f fakeInterrupt) TypeName() string           { return f.name }
func (f fakeInterrupt) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (f fakeInterrupt) Return([]byte) error        { return nil }
func (f fakeInterrupt) Error() string              { return f.name }

// TestInterrupt_asError confirms errors.As works for tool return paths.
func TestInterrupt_asError(t *testing.T) {
	var err error = &interrupt.UserSelectionInterrupt{Options: []interrupt.UserChoice{{Title: "A"}}}
	var target interrupt.Interrupt
	if !errors.As(err, &target) {
		t.Fatal("errors.As")
	}
	if !errors.Is(interrupt.ErrInterruptNotFound, interrupt.ErrInterruptNotFound) {
		t.Fatal("sentinel")
	}
}
