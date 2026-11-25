package worker

import (
	"context"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/sandbox"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// constructTurn builds a Harness for the turn. With a sandbox client the
// turn gets run_command, whose supervisor opens the session's tree with the
// turn's bindings on the sandbox host.
func constructTurn(ctx context.Context, agent tacklr.AgentOptions, client *sandbox.Client, threadID string, bindings []mount.Binding, extraMCP []session.MCPServer, jobs []string) (*tacklr.Harness, error) {
	opts := tacklr.BindTurn(agent, threadID, jobs, bindings)
	if client != nil {
		opts.Tools = append(slices.Clone(opts.Tools), sandbox.RunCommand(client))
	}
	if len(extraMCP) > 0 {
		opts.MCPConfigs = append(append([]session.MCPServer(nil), agent.MCPConfigs...), extraMCP...)
	}
	return tacklr.NewHarness(ctx, opts)
}
