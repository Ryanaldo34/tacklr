package tacklr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/internal/command"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

const runCommandTimeout = 60 * time.Second

type runCommandArgs struct {
	Command string `json:"command" desc:"Host shell command. Runs as /bin/sh -c. cwd is the VFS root. Use relative paths (work/foo). Absolute /work is the host /work until a later jail."`
}

func newRunCommand(ms *vfs.MountSession, permissionRequired bool) *Tool {
	cfg := ToolConfig{
		Name:        "run_command",
		DisplayName: "Run {command}",
		Description: `Run a host shell command as /bin/sh -c. cwd is the VFS root (FUSE mount). Use relative paths (work/foo, ./work/foo). Absolute /work is the host /work until a later jail. Non-zero exit is a successful tool result (exit=N).`,
		Category:    streaming.ToolCategoryExecute,
		Access:      ToolExecuteAccess,
		Timeout:     runCommandTimeout,
		Handler: func(ctx context.Context, args runCommandArgs, rt HarnessRuntime) (string, error) {
			dir := ms.HostDir()
			if dir == "" {
				return "", vfs.ErrFuseNotMounted
			}
			cmdStr := strings.TrimSpace(args.Command)
			if cmdStr == "" {
				return "", fmt.Errorf("run_command: command is required: %w", ErrInvalid)
			}
			rt.EmitUpdate("Running " + cmdStr)
			return command.Run(ctx, dir, cmdStr)
		},
	}
	if permissionRequired {
		cfg.OnCall = []OnCallFunc{ToolPermissionOnCall}
	}
	return NewTool(cfg)
}
