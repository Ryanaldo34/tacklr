package tacklr_test

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
)

// TestUserSelection_serializesTheQuestionForTheUser: a user selection
// serializes its question and options, and no choice before an answer;
// as an error it reads as the same question.
func TestUserSelection_serializesTheQuestionForTheUser(t *testing.T) {
	t.Parallel()
	usi := &tacklr.UserSelectionInterrupt{Question: "which?", Options: []tacklr.UserChoice{{Title: "A", IsRecommended: true}, {Title: "B"}}}

	wire, err := usi.Serialize()

	var got map[string]any
	if err := errors.Join(err, json.Unmarshal(wire, &got)); err != nil {
		t.Fatal(err)
	}
	if want := []string{"options", "question"}; !slices.Equal(slices.Sorted(maps.Keys(got)), want) || got["question"] != "which?" {
		t.Fatalf("serialize = %s, want the question and its options only", wire)
	}
	if usi.Error() != string(wire) {
		t.Fatalf("error = %s, want %s", usi.Error(), wire)
	}
}

// TestUserSelection_returnsTheChosenOption: an in-range selectionIdx is
// the chosen option.
func TestUserSelection_returnsTheChosenOption(t *testing.T) {
	t.Parallel()
	usi := &tacklr.UserSelectionInterrupt{Options: []tacklr.UserChoice{{Title: "A"}, {Title: "B"}}}

	err := usi.Return([]byte(`{"selectionIdx":1}`))

	if err != nil || usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("choose = %v giving %+v, want B", err, usi.ConfirmedChoice)
	}
}

// TestUserSelection_refusesAnInvalidAnswer: an answer that is not JSON,
// has no selectionIdx, has one of the wrong type, or one out of range is
// refused.
func TestUserSelection_refusesAnInvalidAnswer(t *testing.T) {
	t.Parallel()
	for name, answer := range map[string]string{
		"not JSON":        `not-json`,
		"no selectionIdx": `{}`,
		"a wrong type":    `{"selectionIdx":"x"}`,
		"out of range":    `{"selectionIdx":2}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			usi := &tacklr.UserSelectionInterrupt{Options: []tacklr.UserChoice{{Title: "A"}, {Title: "B"}}}

			err := usi.Return([]byte(answer))

			if err == nil || usi.ConfirmedChoice != nil {
				t.Fatalf("answer %s = %v choosing %+v, want it refused", answer, err, usi.ConfirmedChoice)
			}
		})
	}
}

// TestToolPermission_resolvesEachKindOfAnswer: a permission prompt
// serializes only what the client shows, resolves each default option to
// its kind (allowed or not), and refuses an answer that is not JSON, has no
// or a wrongly typed optionId, names an unknown option, or an option of an
// unknown kind.
func TestToolPermission_resolvesEachKindOfAnswer(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	empty, named := &tacklr.AuthExpired{}, &tacklr.AuthExpired{Tool: "read"}

	resumed := empty.Return([]byte(`{}`))
	wire, err := named.Serialize()

	if empty.TypeName() != tacklr.TypeAuthExpired || empty.Error() != "credentials expired" || named.Error() != "read: credentials expired" || resumed != nil || err != nil || string(wire) != `{"tool":"read"}` {
		t.Fatalf("auth expired = %q, %q, resume %v, wire %s", empty.Error(), named.Error(), resumed, wire)
	}
}
