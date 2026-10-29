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

// shellEnv is the whole environment of the shell. There is no jail here, so
// the worker's PATH and HOME are kept for its tools; nothing else is passed.
func shellEnv() []string {
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
}

func jailCommand(ctx context.Context, dir, command string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `cd "$1" && eval "$2"`, "run_command", dir, command)
	cmd.Dir = os.TempDir()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}
