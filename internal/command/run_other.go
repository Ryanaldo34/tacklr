//go:build !linux

package command

import (
	"context"
	"os"
	"os/exec"
	"syscall"
)

const jailRequired = false

func RequireJail() {}

func jailCommand(ctx context.Context, dir, command string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `cd "$1" && eval "$2"`, "run_command", dir, command)
	cmd.Dir = os.TempDir()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}
