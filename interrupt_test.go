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

	if err := usi.ValidatePayload([]byte(`not-json`)); err == nil || !errors.Is(err, tacklr.ErrInvalidPayload) {
		t.Fatalf("invalid json: %v", err)
	}
	if err := usi.ValidatePayload([]byte(`{}`)); err == nil || !errors.Is(err, tacklr.ErrInvalidPayload) || !strings.Contains(err.Error(), "selectionIdx") {
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

// TestRegister_New_Clone covers registry and deep clone outcomes.
func TestRegister_New_Clone(t *testing.T) {
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
	cpPerm, err := tacklr.CloneInterrupt(perm)
	if err != nil {
		t.Fatal(err)
	}
	clonedPerm, ok := cpPerm.(*tacklr.ToolPermissionInterrupt)
	if !ok || !clonedPerm.Allowed || clonedPerm.SelectedOptionID != "allow-once" || clonedPerm.SelectedKind != tacklr.PermissionAllowOnce {
		t.Fatalf("clone lost permission resolution: %+v", cpPerm)
	}
	wire, err := perm.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "allowed") || strings.Contains(string(wire), "selectedOption") {
		t.Fatalf("serialize leaked resolution: %s", wire)
	}

	src := &tacklr.UserSelectionInterrupt{
		Options: []tacklr.UserChoice{{Title: "A"}},
	}
	cp, err := tacklr.CloneInterrupt(src)
	if err != nil {
		t.Fatal(err)
	}
	cloned, ok := cp.(*tacklr.UserSelectionInterrupt)
	if !ok || len(cloned.Options) != 1 || cloned.Options[0].Title != "A" {
		t.Fatalf("%+v", cp)
	}
	// Mutate original; clone stays independent after re-serialize path.
	src.Options[0].Title = "mutated"
	if cloned.Options[0].Title != "A" {
		t.Fatal("clone should be independent")
	}

	got, err := tacklr.CloneInterrupt(nil)
	if got != nil || err != nil {
		t.Fatalf("nil clone: %v %v", got, err)
	}

	fake := fakeInterrupt{name: "not_in_registry"}
	if _, err := tacklr.CloneInterrupt(fake); err == nil {
		t.Fatal("clone unknown type")
	}

	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return marshalBoom{} })
	if _, err := tacklr.CloneInterrupt(marshalBoom{}); err == nil {
		t.Fatal("clone marshal error")
	}
	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return unmarshalBoom{} })
	if _, err := tacklr.CloneInterrupt(unmarshalBoom{}); err == nil {
		t.Fatal("clone unmarshal error")
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

type marshalBoom struct{}

func (marshalBoom) TypeName() string           { return "marshal_boom" }
func (marshalBoom) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (marshalBoom) Return([]byte) error        { return nil }
func (marshalBoom) Error() string              { return "marshal_boom" }
func (marshalBoom) MarshalJSON() ([]byte, error) {
	return nil, errors.New("marshal boom")
}

type unmarshalBoom struct{}

func (unmarshalBoom) TypeName() string           { return "unmarshal_boom" }
func (unmarshalBoom) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (unmarshalBoom) Return([]byte) error        { return nil }
func (unmarshalBoom) Error() string              { return "unmarshal_boom" }
func (unmarshalBoom) MarshalJSON() ([]byte, error) {
	return []byte(`"x"`), nil
}

func TestChildWaiting_typeNameAndError(t *testing.T) {
	empty := &tacklr.ChildWaiting{}
	if empty.TypeName() != tacklr.TypeChildWaiting {
		t.Fatalf("default type: %s", empty.TypeName())
	}
	if empty.Error() != "child session awaiting input" {
		t.Fatalf("default error: %s", empty.Error())
	}
	if err := empty.ValidatePayload(nil); err != nil {
		t.Fatal(err)
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

// TestInterrupt_asError confirms errors.As works for tool return paths.
func TestInterrupt_asError(t *testing.T) {
	var err error = &tacklr.UserSelectionInterrupt{Options: []tacklr.UserChoice{{Title: "A"}}}
	var target tacklr.Interrupt
	if !errors.As(err, &target) {
		t.Fatal("errors.As")
	}
	if !errors.Is(tacklr.ErrInterruptNotFound, tacklr.ErrInterruptNotFound) {
		t.Fatal("sentinel")
	}
}
