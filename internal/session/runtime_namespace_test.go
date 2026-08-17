package session_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/internal/session"
)

// Legacy _search_namespace is reserved and never re-exported as user state.
func TestSession_legacySearchNamespaceKey_strippedOnLoad(t *testing.T) {
	sm := session.NewSessionManager()
	id := uuid.New()
	sm.LoadUserAndPlanState(map[string]any{
		"_search_namespace": id.String(),
		"user_key":          "v",
	})
	got, ok := sm.Search.Namespace()
	if !ok || got != id {
		t.Fatalf("legacy key must restore Search namespace, got %v ok=%v", got, ok)
	}
	cp, err := session.NewCheckpointer().Capture(nil, sm, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cp.State.UserState["_search_namespace"]; ok {
		t.Fatal("legacy key must not re-export")
	}
	var userValue string
	if err := json.Unmarshal(cp.State.UserState["user_key"], &userValue); err != nil || userValue != "v" {
		t.Fatalf("%+v err=%v", cp.State.UserState, err)
	}
}
