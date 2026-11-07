package adapter

import (
	"context"
	"fmt"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// ForSession is agent as session id runs it: when the agent has a
// SessionScope, its Namespace and Principals for id replace SearchNamespace
// and SearchPrincipals.
func ForSession(ctx context.Context, agent tacklr.AgentOptions, id session.ID) (tacklr.AgentOptions, error) {
	if agent.SessionScope == nil {
		return agent, nil
	}
	scope, err := agent.SessionScope(ctx, string(id))
	if err != nil {
		return tacklr.AgentOptions{}, fmt.Errorf("session %s scope: %w", id, err)
	}
	agent.SearchNamespace, agent.SearchPrincipals = scope.Namespace, scope.Principals
	return agent, nil
}

// ForgetMessages deletes the messages that session id saved in the agent's
// brain, in the session's own namespace. An agent without a brain saved
// none.
func ForgetMessages(ctx context.Context, agent tacklr.AgentOptions, id session.ID) error {
	if agent.Brain == nil {
		return nil
	}
	scoped, err := ForSession(ctx, agent, id)
	if err != nil {
		return err
	}
	return agent.Brain.DeleteSessionMessages(ctx, scoped.SearchNamespace, string(id))
}
