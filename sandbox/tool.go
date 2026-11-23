package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	runCommandTimeout = 60 * time.Second
	outputCap         = 1 << 20
)

type runCommandArgs struct {
	Command string `json:"command" desc:"Bash script. Runs as bash -c with cwd /workspace. Use relative paths (work/foo)."`
}

// RunCommand is the run_command tool: the model's bash script runs in a
// sandboxed worker that client starts, over workspace (the turn's tree,
// vfs.NewFiles). Each call starts a worker and ends it, so nothing but the
// workspace lasts from one call to the next. It asks for execute permission
// unless the host lists it in AgentOptions.AllowTools.
func RunCommand(client *Client, workspace vfs.Files) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "run_command",
		DisplayName: "Run {command}",
		Description: `Run a bash script in a sandbox, with cwd at this session's workspace root (/workspace). Call for directory listings, grep, sed, find, sort and other shell work on the workspace files.

The sandbox has bash, coreutils, grep, sed and findutils, and no network. Documents (Word, Google Docs) read as their text and cannot be written. Nothing but the workspace files lasts between calls.

May pause for execute permission. Returns stdout/stderr. A non-zero exit is still a successful tool result (exit=N in the output). Fails if no command is given.`,
		Category: message.ToolCategoryExecute,
		Access:   tacklr.ToolExecuteAccess,
		Timeout:  runCommandTimeout,
		OnCall:   []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context, args runCommandArgs, cc *tacklr.CallContext) (string, error) {
			command := strings.TrimSpace(args.Command)
			if command == "" {
				return "", tacklr.Correctionf(tacklr.ErrInvalid, "run_command: command is required. Pass a bash script, for example ls work")
			}
			cc.EmitUpdate("Running " + command)
			worker, err := client.Start(ctx)
			if err != nil {
				return "", err
			}
			defer worker.Close()
			budget := outputBudget{remaining: outputCap}
			stdout, stderr := budget.writer(), budget.writer()
			code, err := worker.Run(ctx, command, workspace, stdout, stderr)
			if err != nil {
				return "", err
			}
			return formatResult(code, budget.truncated, stdout, stderr), nil
		},
	})
}

func formatResult(exit int, truncated bool, stdout, stderr *budgetWriter) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "exit=%d truncated=%v\n", exit, truncated)
	if truncated {
		builder.WriteString("output truncated\n")
	}
	builder.WriteString("--- stdout ---\n")
	out := stdout.buf.Bytes()
	builder.Write(out)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		builder.WriteByte('\n')
	}
	builder.WriteString("--- stderr ---\n")
	builder.Write(stderr.buf.Bytes())
	return builder.String()
}

// outputBudget is the bytes of output one call keeps, shared by its stdout
// and stderr.
type outputBudget struct {
	mu        sync.Mutex
	remaining int
	truncated bool
}

type budgetWriter struct {
	budget *outputBudget
	buf    bytes.Buffer
}

func (b *outputBudget) writer() *budgetWriter {
	return &budgetWriter{budget: b}
}

func (w *budgetWriter) Write(value []byte) (int, error) {
	w.budget.mu.Lock()
	defer w.budget.mu.Unlock()
	take := min(len(value), w.budget.remaining)
	if take < len(value) {
		w.budget.truncated = true
	}
	_, _ = w.buf.Write(value[:take])
	w.budget.remaining -= take
	return len(value), nil
}
