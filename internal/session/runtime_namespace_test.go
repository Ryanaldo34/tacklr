package session_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestSession_SearchNamespace_survivesCheckpoint(t *testing.T) {
	sm := session.NewSessionManager()
	ns := uuid.New()
	sm.SetSearchNamespace(ns)
	sm.Plan().SetDocument("plan")
	sm.Plan().Set([]streaming.Todo{{Title: "A", Status: streaming.TodoStatusPending}})

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
	if _, err := session.NewCheckpointer().Apply(*cp, sm2); err != nil {
		t.Fatal(err)
	}

	got, ok := sm2.SearchNamespace()
	if !ok || got != ns {
		t.Fatalf("restored session namespace %v %v, want %v", got, ok, ns)
	}

	rt := session.NewRuntime(nil, nil, sm2)
	rt.StateSet("_search_namespace", "blocked")
	got, ok = sm2.SearchNamespace()
	if !ok || got != ns {
		t.Fatalf("namespace after reserved StateSet: %v %v", got, ok)
	}
}

func TestSession_SearchNamespace_clearThenCheckpoint(t *testing.T) {
	sm := session.NewSessionManager()
	sm.SetSearchNamespace(uuid.New())
	sm.ClearSearchNamespace()

	cp, err := session.NewCheckpointer().Capture(nil, sm, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	sm2.SetSearchNamespace(uuid.New())
	if _, err := session.NewCheckpointer().Apply(*cp, sm2); err != nil {
		t.Fatal(err)
	}
	if _, ok := sm2.SearchNamespace(); ok {
		t.Fatal("cleared namespace must restore as unset")
	}
}
