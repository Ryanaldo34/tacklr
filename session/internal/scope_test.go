package adapter_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
)

// TestForSession_searchesInTheScopeTheHostGivesTheSession: an agent
// without a SessionScope searches where it is set to; with one, in the
// namespace and as the principals the scope gives the session; a scope
// that fails for the session is its error.
func TestForSession_searchesInTheScopeTheHostGivesTheSession(t *testing.T) {
	t.Parallel()
	// Arrange
	acme, globex := namespace(t, "acme"), namespace(t, "globex")
	scope := func(_ context.Context, id string) (brain.Scope, error) {
		if id != "sess-acme" {
			return brain.Scope{}, errors.New("no tenant for " + id)
		}
		return brain.Scope{Namespace: acme, Principals: []string{"u1"}}, nil
	}
	for name, tc := range map[string]struct {
		scope      func(context.Context, string) (brain.Scope, error)
		id         string
		namespace  brain.Namespace
		principals []string
		err        string
	}{
		"no scope":           {nil, "sess-acme", globex, []string{"admin"}, ""},
		"the session's":      {scope, "sess-acme", acme, []string{"u1"}, ""},
		"a scope that fails": {scope, "sess-other", brain.Namespace{}, nil, "session sess-other scope: no tenant for sess-other"},
	} {
		agent := tacklr.AgentOptions{SearchNamespace: globex, SearchPrincipals: []string{"admin"}, SessionScope: tc.scope}

		// Act
		got, err := adapter.ForSession(t.Context(), agent, session.ID(tc.id))

		// Assert
		if (err == nil) != (tc.err == "") || (err != nil && err.Error() != tc.err) || got.SearchNamespace.String() != tc.namespace.String() || !slices.Equal(got.SearchPrincipals, tc.principals) {
			t.Errorf("%s: %s %v, %v; want %s %v, %q", name, got.SearchNamespace, got.SearchPrincipals, err, tc.namespace, tc.principals, tc.err)
		}
	}
}

// TestForgetMessages_deletesTheSessionsMessagesInItsOwnScope: the
// session's messages are deleted in the namespace its scope gives it; an
// agent without a brain has none, and a scope that fails is the error.
func TestForgetMessages_deletesTheSessionsMessagesInItsOwnScope(t *testing.T) {
	t.Parallel()
	// Arrange
	acme := namespace(t, "acme")
	var forgot []string
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(func(_ context.Context, ev brain.AuditEvent) error {
		if ev.Op == brain.AuditSessionDelete {
			forgot = append(forgot, ev.Namespace.String()+" "+ev.SessionID)
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	agent := tacklr.AgentOptions{Brain: eng, SearchNamespace: namespace(t, "globex"), SessionScope: func(_ context.Context, id string) (brain.Scope, error) {
		if id != "s" {
			return brain.Scope{}, errors.New("no tenant")
		}
		return brain.Scope{Namespace: acme}, nil
	}}

	// Act
	scoped := adapter.ForgetMessages(t.Context(), agent, "s")
	failed := adapter.ForgetMessages(t.Context(), agent, "other")
	none := adapter.ForgetMessages(t.Context(), tacklr.AgentOptions{}, "s")

	// Assert
	if scoped != nil || failed == nil || none != nil || !slices.Equal(forgot, []string{acme.String() + " s"}) {
		t.Fatalf("forget %v, with a failed scope %v, without a brain %v, deletes %v; want one delete in the session's namespace", scoped, failed, none, forgot)
	}
}

func namespace(t *testing.T, org string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace("org", org)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}
