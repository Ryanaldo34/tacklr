// Package agent is the agent the example's workers run: its tools, its
// specialist and its workspace. Only the worker links it; the host needs
// none of it.
package agent

import (
	"context"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/workspace"
)

// Options is the agent every session runs on model: a tool of its own
// (lookup), a researcher specialist, and the MCP servers an ACP client names
// for its session. The window is the model's unless the caller sets one.
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

// Workspace is each session's /workspace: work is a directory of the
// session's own under dir (a specialist gets its own too), which the agent
// reads and writes. A write asks the user first. Results cut from the window
// are kept in it, so the agent can read them back.
func Workspace(dir string) workspace.Config {
	return workspace.Config{
		Open:         vfs.Tree(vfs.At("work", vfs.SessionLocal(dir))),
		OverflowPath: "/workspace/work/.overflow",
	}
}
