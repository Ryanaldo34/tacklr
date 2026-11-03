package worker

import (
	"context"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/vfs"
)

// constructTurn opens VFS trees and builds a Harness. On NewHarness
// failure the trees are closed.
func constructTurn(ctx context.Context, agent tacklr.AgentOptions, threadID string, bindings []mount.Binding, proj vfs.Projection, extraMCP []session.MCPServer, jobs []string) (*tacklr.Harness, *vfs.MountSession, error) {
	ms, err := openTurnVFS(ctx, threadID, agent, bindings, proj)
	if err != nil {
		return nil, nil, err
	}
	opts := tacklr.BindTurn(agent, threadID, ms, jobs)
	if len(extraMCP) > 0 {
		opts.MCPConfigs = append(append([]session.MCPServer(nil), agent.MCPConfigs...), extraMCP...)
	}
	h, err := tacklr.NewHarness(ctx, opts)
	if err != nil {
		closeTurnVFS(ms)
		return nil, nil, err
	}
	return h, ms, nil
}

// abandonTurn closes a harness and its VFS: a harness that closes, or a
// construct that failed.
func abandonTurn(h *tacklr.Harness, workspace *vfs.MountSession) {
	if h != nil {
		h.Close()
	}
	closeTurnVFS(workspace)
}
