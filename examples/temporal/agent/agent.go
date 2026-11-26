// Package agent is the agent the example's workers run: its tools, its
// specialist and its workspace. Only the worker links it; the host needs
// none of it.
package agent

import (
	"context"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Options is the agent every session runs on model: a tool of its own
// (lookup), a researcher specialist, and the MCP servers a session's
// creator names. The window is the model's unless the caller sets one.
func Options(model tacklr.Model) tacklr.AgentOptions {
	lookup := tacklr.NewTool(tacklr.ToolConfig{
		Name:        "lookup",
		Description: "Looks up the status of a release.",
		Handler: func(_ context.Context, args struct {
			Release string `json:"release" desc:"Release name, for example v2"`
		}) (string, error) {
			return args.Release + ": shipped", nil
		},
	})
	return tacklr.AgentOptions{
		Name:         "releases",
		SystemPrompt: "You help with releases. Keep notes in /workspace/work.",
		Model:        model,
		Tools:        []*tacklr.Tool{lookup},
		Specialists: []*tacklr.Specialist{{
			Name:         "researcher",
			Description:  "Researches a question and reports back.",
			Instructions: "Research the question. Answer briefly.",
			Model:        model,
		}},
		MCP: mcp.Client{},
	}
}

// Tree is each session's /workspace: work is a directory of the session's
// own under dir (a specialist gets its own too). The supervisor opens it on
// the sandbox host for the agent's run_command.
func Tree(dir string) vfs.OpenVFS {
	return vfs.Tree(vfs.At("work", vfs.SessionLocal(dir)))
}
