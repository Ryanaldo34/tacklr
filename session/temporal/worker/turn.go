package worker

import (
	"context"
	"slices"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/shell"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
	"github.com/ryanaldo34/tacklr/workspace"
)

// constructTurn opens the turn's workspace and builds a Harness with its
// tools, skills and overflow. A workspace mounted on the host also gets
// run_command there. On a failure after the open the workspace is closed.
func constructTurn(ctx context.Context, agent tacklr.AgentOptions, ws workspace.Config, fuseRoot, threadID string, bindings []mount.Binding, extraMCP []session.MCPServer, jobs []string) (*tacklr.Harness, *fuse.Server, error) {
	ms, srv, err := openWorkspace(ctx, threadID, ws, bindings, fuseRoot)
	if err != nil {
		return nil, nil, err
	}
	opts := tacklr.BindTurn(agent, threadID, jobs)
	if ms != nil {
		if opts, err = ws.Bind(opts, ms); err != nil {
			closeWorkspace(srv)
			return nil, nil, err
		}
	}
	if srv != nil {
		opts.Tools = append(slices.Clone(opts.Tools), shell.RunCommand(srv.Dir()))
	}
	if len(extraMCP) > 0 {
		opts.MCPConfigs = append(append([]session.MCPServer(nil), agent.MCPConfigs...), extraMCP...)
	}
	h, err := tacklr.NewHarness(ctx, opts)
	if err != nil {
		closeWorkspace(srv)
		return nil, nil, err
	}
	return h, srv, nil
}

// abandonTurn closes a harness and unmounts its workspace: a harness that
// closes, or a construct that failed.
func abandonTurn(h *tacklr.Harness, srv *fuse.Server) {
	if h != nil {
		h.Close()
	}
	closeWorkspace(srv)
}
