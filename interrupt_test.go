package tacklr_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
)

// TestUserSelection_returnsTheChosenOption: a user selection serializes its
// question and options, takes an in-range selectionIdx as the chosen
// option, and refuses an answer that is not JSON, has no selectionIdx, has
// one of the wrong type, or one out of range.
func TestUserSelection_returnsTheChosenOption(t *testing.T) {
	usi := &tacklr.UserSelectionInterrupt{Question: "which?", Options: []tacklr.UserChoice{{Title: "A", IsRecommended: true}, {Title: "B"}}}

	wire, err := usi.Serialize()
	invalid := []error{
		usi.Return([]byte(`not-json`)),
		usi.Return([]byte(`{}`)),
		usi.Return([]byte(`{"selectionIdx":"x"}`)),
		usi.Return([]byte(`{"selectionIdx":99}`)),
		usi.Return([]byte(`{"selectionIdx":-1}`)),
	}
	chosen := usi.Return([]byte(`{"selectionIdx":1}`))

	if want := `{"question":"which?","options":[{"title":"A","description":"","isRecommended":true},{"title":"B","description":"","isRecommended":false}],"ConfirmedChoice":null}`; err != nil || string(wire) != want {
		t.Fatalf("serialize = %s %v, want %s", wire, err, want)
	}
	for i, err := range invalid[:3] {
		if !errors.Is(err, tacklr.ErrInvalidPayload) {
			t.Errorf("answer %d = %v, want ErrInvalidPayload", i, err)
		}
	}
	if !strings.Contains(invalid[1].Error(), "selectionIdx") || invalid[3] == nil || invalid[4] == nil {
		t.Fatalf("answers = %v, want the missing field named and each out-of-range index refused", invalid)
	}
	if chosen != nil || usi.ConfirmedChoice.Title != "B" || !strings.Contains(usi.Error(), `"title":"B"`) {
		t.Fatalf("choose = %v giving %+v, error text %s; want B", chosen, usi.ConfirmedChoice, usi.Error())
	}
}

// TestToolPermission_resolvesEachKindOfAnswer: a permission prompt
// serializes only what the client shows, resolves each default option to
// its kind (allowed or not), and refuses an answer that is not JSON, has no
// or a wrongly typed optionId, names an unknown option, or an option of an
// unknown kind.
func TestToolPermission_resolvesEachKindOfAnswer(t *testing.T) {
	for _, tc := range []struct {
		id, kind string
		allowed  bool
	}{
		{"allow-once", tacklr.PermissionAllowOnce, true},
		{"allow-always", tacklr.PermissionAllowAlways, true},
		{"reject-once", tacklr.PermissionRejectOnce, false},
		{"reject-always", tacklr.PermissionRejectAlways, false},
	} {
		p := &tacklr.ToolPermissionInterrupt{ToolName: "gate", Options: tacklr.DefaultPermissionOptions()}
		payload, _ := json.Marshal(map[string]string{"optionId": tc.id})
		if err := p.Return(payload); err != nil || p.Allowed != tc.allowed || p.SelectedKind != tc.kind || p.SelectedOptionID != tc.id {
			t.Fatalf("%s = %v: allowed=%v kind=%s id=%s", tc.id, err, p.Allowed, p.SelectedKind, p.SelectedOptionID)
		}
		if wire, _ := p.Serialize(); !strings.HasPrefix(string(wire), `{"toolName":"gate","options":[{"optionId":"allow-once"`) || !strings.HasSuffix(string(wire), `"kind":"reject_always"}]}`) {
			t.Fatalf("serialize = %s, want the tool and its options only", wire)
		}
	}
	p := &tacklr.ToolPermissionInterrupt{Options: tacklr.DefaultPermissionOptions()}
	odd := &tacklr.ToolPermissionInterrupt{Options: []tacklr.PermissionOption{{OptionID: "weird", Name: "W", Kind: "nope"}}}

	invalid := []error{
		p.Return([]byte(`x`)),
		p.Return([]byte(`{}`)),
		p.Return([]byte(`{"optionId":1}`)),
		p.Return([]byte(`{"optionId":"nope"}`)),
	}
	unknownKind := odd.Return([]byte(`{"optionId":"weird"}`))

	for i, err := range invalid {
		if !errors.Is(err, tacklr.ErrInvalidPayload) {
			t.Errorf("answer %d = %v, want ErrInvalidPayload", i, err)
		}
	}
	if unknownKind == nil || !strings.Contains(unknownKind.Error(), `unknown permission kind "nope"`) || !strings.Contains(p.Error(), `"options"`) {
		t.Fatalf("an option of an unknown kind = %v, want it refused", unknownKind)
	}
}

// hostInterrupt is an interrupt type a host registers.
type hostInterrupt struct{ name string }

func (h hostInterrupt) TypeName() string           { return h.name }
func (h hostInterrupt) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (h hostInterrupt) Return([]byte) error        { return nil }
func (h hostInterrupt) Error() string              { return h.name }

// TestRegisterInterrupt_makesATypeReadableOnce: the built-in types and a
// host's registered type are found by name, an unknown type is not, and a
// type registers once.
func TestRegisterInterrupt_makesATypeReadableOnce(t *testing.T) {
	// Each run registers a type of its own: the registry is the process's.
	host := "host_" + uuid.NewString()
	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return hostInterrupt{name: host} })
	var found []string
	for _, name := range []string{"user_selection_choice", "tool_permission", tacklr.TypeAuthExpired, host} {
		if intr, ok := tacklr.NewInterrupt(name); ok {
			found = append(found, intr.TypeName())
		}
	}
	_, unknown := tacklr.NewInterrupt("missing_type")

	if strings.Join(found, ",") != "user_selection_choice,tool_permission,auth_expired,"+host || unknown {
		t.Fatalf("found %v (unknown found %v), want each registered type", found, unknown)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("a second registration of a type was accepted")
		}
	}()
	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return hostInterrupt{name: host} })
}

// TestAuthExpired_namesTheToolAndResumesOnAnyAnswer: an expired credential
// names its tool when it has one and resumes on any answer.
func TestAuthExpired_namesTheToolAndResumesOnAnyAnswer(t *testing.T) {
	empty, named := &tacklr.AuthExpired{}, &tacklr.AuthExpired{Tool: "read"}

	resumed := empty.Return([]byte(`{}`))
	wire, err := named.Serialize()

	if empty.TypeName() != tacklr.TypeAuthExpired || empty.Error() != "credentials expired" || named.Error() != "read: credentials expired" || resumed != nil || err != nil || string(wire) != `{"tool":"read"}` {
		t.Fatalf("auth expired = %q, %q, resume %v, wire %s", empty.Error(), named.Error(), resumed, wire)
	}
}
