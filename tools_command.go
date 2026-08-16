package tacklr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	runCommandTimeout   = 60 * time.Second
	runCommandOutputCap = 1 << 20 // 1 MiB combined stdout+stderr
)

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
				return "", fmt.Errorf("run_command: command is required")
			}
			rt.EmitUpdate("Running " + cmdStr)

			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", cmdStr)
			cmd.Dir = dir
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Cancel = func() error {
				if cmd.Process == nil {
					return nil
				}
				return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}

			budget := outputBudget{remaining: runCommandOutputCap}
			stdout := budget.writer()
			stderr := budget.writer()
			cmd.Stdout = stdout
			cmd.Stderr = stderr

			err := cmd.Run()
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			exit := 0
			if err != nil {
				var ee *exec.ExitError
				if errors.As(err, &ee) {
					exit = ee.ExitCode()
				} else {
					return "", fmt.Errorf("run_command: %w", err)
				}
			}
			return formatRunCommandResult(exit, budget.truncated, stdout, stderr), nil
		},
	}
	if permissionRequired {
		cfg.OnCall = OnCalls(ToolPermissionOnCall)
	}
	return NewTool(cfg)
}

func formatRunCommandResult(exit int, truncated bool, stdout, stderr *budgetWriter) string {
	var b strings.Builder
	extra := 0
	if truncated {
		extra = len("output truncated\n")
	}
	b.Grow(64 + extra + stdout.buf.Len() + stderr.buf.Len())
	fmt.Fprintf(&b, "exit=%d truncated=%v\n", exit, truncated)
	if truncated {
		b.WriteString("output truncated\n")
	}
	b.WriteString("--- stdout ---\n")
	out := stdout.buf.Bytes()
	b.Write(out)
	if n := len(out); n > 0 && out[n-1] != '\n' {
		b.WriteByte('\n')
	}
	b.WriteString("--- stderr ---\n")
	b.Write(stderr.buf.Bytes())
	return b.String()
}

// outputBudget is a shared 1 MiB stdout+stderr cap for one exec.Cmd.
type outputBudget struct {
	mu        sync.Mutex
	remaining int
	truncated bool
}

type budgetWriter struct {
	b   *outputBudget
	buf bytes.Buffer
}

func (b *outputBudget) writer() *budgetWriter {
	return &budgetWriter{b: b}
}

func (w *budgetWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	if w.b.remaining <= 0 {
		w.b.truncated = true
		return len(p), nil
	}
	take := len(p)
	if take > w.b.remaining {
		take = w.b.remaining
		w.b.truncated = true
	}
	w.buf.Write(p[:take])
	w.b.remaining -= take
	return len(p), nil
}
