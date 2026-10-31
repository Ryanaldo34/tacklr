package tacklr_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

// TestUserSelection_fullLifecycle covers serialize, validate, return, and error string.
func TestUserSelection_fullLifecycle(t *testing.T) {
	usi := &tacklr.UserSelectionInterrupt{
		Options: []tacklr.UserChoice{
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

	if err := usi.Return([]byte(`not-json`)); err == nil || !errors.Is(err, tacklr.ErrInvalidPayload) {
		t.Fatalf("invalid json: %v", err)
	}
	if err := usi.Return([]byte(`{}`)); err == nil || !errors.Is(err, tacklr.ErrInvalidPayload) || !strings.Contains(err.Error(), "selectionIdx") {
		t.Fatalf("missing field: %v", err)
	}
	if err := usi.Return([]byte(`{"selectionIdx":99}`)); err == nil {
		t.Fatal("out of range validate")
	}
	if err := usi.Return([]byte(`{"selectionIdx":"x"}`)); err == nil {
		t.Fatal("selectionIdx wrong type")
	}
	if err := usi.Return([]byte(`{"selectionIdx":0}`)); err != nil {
		t.Fatal(err)
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
	fresh := &tacklr.UserSelectionInterrupt{}
	if err := fresh.InitFromPayload([]byte(`[{"title":"only"}]`)); err != nil {
		t.Fatal(err)
	}
	if len(fresh.Options) != 1 || fresh.Options[0].Title != "only" {
		t.Fatalf("%+v", fresh.Options)
	}
}

// TestToolPermission_allKinds covers init, validate, allow/reject, and unknown option.
func TestToolPermission_allKinds(t *testing.T) {
	p := &tacklr.ToolPermissionInterrupt{}
	if p.SelectedKind != "" {
		t.Fatal("zero value has no selected kind")
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

	if err := p.Return([]byte(`x`)); err == nil {
		t.Fatal("bad json")
	}
	if err := p.Return([]byte(`{}`)); err == nil {
		t.Fatal("missing optionId")
	}
	if err := p.Return([]byte(`{"optionId":"nope"}`)); err == nil {
		t.Fatal("unknown option validate")
	}
	if err := p.Return([]byte(`{"optionId":1}`)); err == nil {
		t.Fatal("optionId wrong type")
	}
	if err := p.Return([]byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}

	if err := p.Return([]byte(`{"optionId":"missing"}`)); err == nil {
		t.Fatal("unknown option return")
	}

	cases := []struct {
		id      string
		allowed bool
		kind    string
	}{
		{"allow-once", true, tacklr.PermissionAllowOnce},
		{"allow-always", true, tacklr.PermissionAllowAlways},
		{"reject-once", false, tacklr.PermissionRejectOnce},
		{"reject-always", false, tacklr.PermissionRejectAlways},
	}
	for _, tc := range cases {
		p2 := &tacklr.ToolPermissionInterrupt{Options: tacklr.DefaultPermissionOptions()}
		payload, _ := json.Marshal(map[string]string{"optionId": tc.id})
		if err := p2.Return(payload); err != nil {
			t.Fatal(err)
		}
		if p2.Allowed != tc.allowed || p2.SelectedKind != tc.kind || p2.SelectedOptionID != tc.id {
			t.Fatalf("%s: allowed=%v kind=%s id=%s", tc.id, p2.Allowed, p2.SelectedKind, p2.SelectedOptionID)
		}
	}

	// Unknown kind on a custom option.
	p3 := &tacklr.ToolPermissionInterrupt{
		Options: []tacklr.PermissionOption{{OptionID: "weird", Name: "W", Kind: "nope"}},
	}
	if err := p3.Return([]byte(`{"optionId":"weird"}`)); err == nil {
		t.Fatal("unknown kind")
	}
}

// TestRegister_newAndDuplicate covers the registry: builtins are found, an
// unknown type is not, a permission's answer stays off the client payload,
// and a type registers once.
func TestRegister_newAndDuplicate(t *testing.T) {
	if _, ok := tacklr.NewInterrupt("user_selection_choice"); !ok {
		t.Fatal("builtin selection")
	}
	if _, ok := tacklr.NewInterrupt("tool_permission"); !ok {
		t.Fatal("builtin permission")
	}
	if _, ok := tacklr.NewInterrupt("missing_type"); ok {
		t.Fatal("unknown type")
	}

	perm := &tacklr.ToolPermissionInterrupt{Options: tacklr.DefaultPermissionOptions()}
	if err := perm.Return([]byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
	wire, err := perm.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "allowed") || strings.Contains(string(wire), "selectedOption") {
		t.Fatalf("serialize leaked resolution: %s", wire)
	}

	// Double-register panics.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("want panic on double register")
		}
	}()
	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return fakeInterrupt{name: "dup_once"} })
	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return fakeInterrupt{name: "dup_once"} })
}

type fakeInterrupt struct{ name string }

func (f fakeInterrupt) TypeName() string           { return f.name }
func (f fakeInterrupt) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (f fakeInterrupt) Return([]byte) error        { return nil }
func (f fakeInterrupt) Error() string              { return f.name }

func TestChildWaiting_typeNameAndError(t *testing.T) {
	empty := &tacklr.ChildWaiting{}
	if empty.TypeName() != tacklr.TypeChildWaiting {
		t.Fatalf("default type: %s", empty.TypeName())
	}
	if empty.Error() != "child session awaiting input" {
		t.Fatalf("default error: %s", empty.Error())
	}
	if err := empty.Return([]byte(`anything`)); err != nil {
		t.Fatal(err)
	}
	raw, err := empty.Serialize()
	if err != nil || len(raw) == 0 {
		t.Fatalf("serialize: %v %s", err, raw)
	}

	named := &tacklr.ChildWaiting{Kind: "spawn_wait", Message: "waiting on researcher"}
	if named.TypeName() != "spawn_wait" {
		t.Fatalf("kind: %s", named.TypeName())
	}
	if named.Error() != "waiting on researcher" {
		t.Fatalf("message: %s", named.Error())
	}
	got, ok := tacklr.NewInterrupt(tacklr.TypeChildWaiting)
	if !ok {
		t.Fatal("child_waiting not registered")
	}
	if got.TypeName() != tacklr.TypeChildWaiting {
		t.Fatalf("new: %s", got.TypeName())
	}
}

func TestAuthExpired_typeNameAndResume(t *testing.T) {
	empty := &tacklr.AuthExpired{}
	if empty.TypeName() != tacklr.TypeAuthExpired {
		t.Fatalf("type: %s", empty.TypeName())
	}
	if empty.Error() != "credentials expired" {
		t.Fatalf("error: %s", empty.Error())
	}
	if err := empty.Return([]byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	named := &tacklr.AuthExpired{Tool: "read"}
	if named.Error() != "read: credentials expired" {
		t.Fatalf("named: %s", named.Error())
	}
	got, ok := tacklr.NewInterrupt(tacklr.TypeAuthExpired)
	if !ok {
		t.Fatal("auth_expired not registered")
	}
	if got.TypeName() != tacklr.TypeAuthExpired {
		t.Fatalf("new: %s", got.TypeName())
	}
}
