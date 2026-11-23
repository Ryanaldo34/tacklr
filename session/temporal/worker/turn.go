package worker

import (
	"context"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/sandbox"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/workspace"
)

// constructTurn opens the turn's workspace and builds a Harness with its
// tools, skills and overflow. With a sandbox client, a turn that has a
// workspace also gets run_command over it.
func constructTurn(ctx context.Context, agent tacklr.AgentOptions, ws workspace.Config, client *sandbox.Client, threadID string, bindings []mount.Binding, extraMCP []session.MCPServer, jobs []string) (*tacklr.Harness, error) {
	opts := tacklr.BindTurn(agent, threadID, jobs, bindings)
	if ws.Open != nil {
		ms, err := ws.Open(ctx, threadID, vfs.Request{Bindings: bindings})
		if err != nil {
			return nil, err
		}
		if opts, err = ws.Bind(opts, ms); err != nil {
			return nil, err
		}
		if client != nil {
			opts.Tools = append(slices.Clone(opts.Tools), sandbox.RunCommand(client, vfs.NewFiles(ms)))
		}
	}
	if len(extraMCP) > 0 {
		opts.MCPConfigs = append(append([]session.MCPServer(nil), agent.MCPConfigs...), extraMCP...)
	}
	return tacklr.NewHarness(ctx, opts)
}
