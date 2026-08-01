package session_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestCheckpointer_roundTrip(t *testing.T) {
	sm := session.NewSessionManager()
	sm.Plan().SetDocument("plan")
	sm.Plan().Set([]streaming.Todo{{Title: "A", Status: streaming.TodoStatusPending}})
	rt := session.NewRuntime(nil, nil, sm)
	rt.StateSet("k", "v")

	cp, err := session.NewCheckpointer().Capture(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "hi"}},
		sm,
		map[string]stores.PendingToolCall{},
		map[string]string{},
	)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	applied, err := session.NewCheckpointer().Apply(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Window) != 1 {
		t.Fatalf("window=%d", len(applied.Window))
	}
	if sm2.Plan().Document() != "plan" {
		t.Fatal("plan doc")
	}
	rt2 := session.NewRuntime(nil, nil, sm2)
	if v, ok := rt2.StateGet("k"); !ok || v != "v" {
		t.Fatalf("state %v %v", v, ok)
	}
}
