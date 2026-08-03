package session_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/internal/session"
)

// Legacy _search_namespace is reserved and never re-exported as user state.
func TestSession_legacySearchNamespaceKey_strippedOnLoad(t *testing.T) {
	sm := session.NewSessionManager()
	sm.LoadUserAndPlanState(map[string]any{
		"_search_namespace": uuid.New().String(),
		"user_key":          "v",
	})
	cp, err := session.NewCheckpointer().Capture(nil, sm, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cp.State.RuntimeState["_search_namespace"]; ok {
		t.Fatal("legacy key must not re-export")
	}
	if cp.State.RuntimeState["user_key"] != "v" {
		t.Fatalf("%+v", cp.State.RuntimeState)
	}
}
