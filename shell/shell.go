// Package shell is the run_command tool: a shell jailed to one session's
// workspace as the host shows it on disk (package vfs/fuse). The jail uses
// Linux user, mount and pid namespaces.
//
// The Temporal worker adds the tool to a turn when it mounts the workspace
// (worker.Config.FuseRoot); a host with its own mount adds RunCommand(dir) to
// AgentOptions.Tools.
package shell

import (
	"context"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
)

const runCommandTimeout = 60 * time.Second

type runCommandArgs struct {
	Command string `json:"command" desc:"Host shell command. Runs as /bin/sh -c. cwd is the VFS root. Use relative paths (workspace/work/foo). The shell is jailed to this mount."`
}

// RunCommand is the run_command tool: /bin/sh -c in dir, the host directory
// that shows the session's tree, jailed so it sees nothing else. It asks for
// execute permission unless the host lists it in AgentOptions.AllowTools. It
// panics when the host cannot build the jail (no unprivileged user
// namespaces), so a missing jail fails at setup, not on the model's first
// call.
func RunCommand(dir string) *tacklr.Tool {
	requireJail()
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "run_command",
		DisplayName: "Run {command}",
		Description: `Run a host shell command as /bin/sh -c with cwd at this session's workspace root. Call for directory listings, grep, and other shell work. The shell cannot see other session mounts, system directories are read-only, and each command starts with only PATH, HOME=/tmp and TMPDIR=/tmp (HOME is not kept between commands).

May pause for execute permission. Returns stdout/stderr. A non-zero process exit is still a successful tool result (exit=N in the output). Fails if no command is given.`,
		Category: message.ToolCategoryExecute,
		Access:   tacklr.ToolExecuteAccess,
		Timeout:  runCommandTimeout,
		OnCall:   []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context, args runCommandArgs, cc *tacklr.CallContext) (string, error) {
			command := strings.TrimSpace(args.Command)
			if command == "" {
				return "", tacklr.Correctionf(tacklr.ErrInvalid, "run_command: command is required. Pass a shell command string, for example ls work")
			}
			cc.EmitUpdate("Running " + command)
			return run(ctx, dir, command)
		},
	})
}
