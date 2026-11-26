package worker

import (
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/sessiontest"
)

// TestLive_theClientMeetsTheSessionContract: the client runs sessions as
// every runtime does, on a worker of the live stack.
func TestLive_theClientMeetsTheSessionContract(t *testing.T) {
	t.Parallel()
	sessiontest.Suite{Deploy: func(t *testing.T, agent tacklr.AgentOptions, jobs map[string]session.JobHandler) session.Client {
		return newLiveStack(t, agent, func(c *Config) { c.Jobs = jobs }).Client
	}}.Run(t)
}
