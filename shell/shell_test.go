package shell

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
)

// TestRunCommand_runsTheModelsCommandInTheJail: a harness runs the
// run_command tool that the host allows without a prompt. The model's
// command runs in the jail on the host's directory, and an empty command
// is the model's to correct.
func TestRunCommand_runsTheModelsCommandInTheJail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte("jailed note\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts := tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192, Tools: []*tacklr.Tool{RunCommand(dir)}, AllowTools: []string{"run_command"}}
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(opts, "shell", nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	out, stop := tacklr.PipeEvents(func(session.Event) {})
	t.Cleanup(stop)
	run := func(args string) (string, error) {
		c, err := h.RunCall(t.Context(), tacklr.NewCall(message.ToolCall{ID: "c1", CallID: "c1", Name: "run_command", Arguments: args}, 0), out)
		return c.Body.Text(), err
	}

	cat, catErr := run(`{"command":"cat note.txt"}`)
	empty, emptyErr := run(`{"command":" "}`)

	if err := errors.Join(catErr, emptyErr); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cat, "exit=0 truncated=false\n") || !strings.Contains(cat, "jailed note") {
		t.Fatalf("cat = %q, want the host directory's file", cat)
	}
	if !strings.Contains(empty, "command is required") {
		t.Fatalf("empty command = %q, want the model told to pass one", empty)
	}
}

// TestRun_panicsWhenTheJailCannotStart: a jail that cannot bind the
// host's directory panics, so the command never runs outside the jail.
func TestRun_panicsWhenTheJailCannotStart(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "missing")
	defer func() {
		if r := recover(); !strings.Contains(fmt.Sprint(r), "the session jail could not be built") {
			t.Fatalf("panic = %v, want the jail's setup failure", r)
		}
	}()

	_, _ = run(t.Context(), missing, "true")
}

// TestRun_jailsTheShellToItsMountedSession: on a FUSE-mounted session tree
// the shell reads the tree's text and a file it writes lands in the tree. It
// cannot see a sibling directory, cannot write the system directories, and
// has only the shell environment, not the worker's. A command's exit status,
// even the jail's own setup status 125 with its setup marker, is a result.
func TestRun_jailsTheShellToItsMountedSession(t *testing.T) {
	// Arrange
	ctx := t.Context()
	t.Setenv("TACKLR_WORKER_SECRET", "worker-only")
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir())))(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	const body = "dirty body unique phrase xyzzy-tacklr\n"
	if err := ms.Route(ctx, "/workspace/work/note.md").WriteFile(ctx, []byte(body)); err != nil {
		t.Fatal(err)
	}
	sibling := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(sibling, []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, err := fuse.Mount(ms, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// Act
	cat, catErr := run(ctx, srv.Dir(), "cat workspace/work/note.md && ls /")
	_, writeErr := run(ctx, srv.Dir(), "mkdir -p workspace/work/fromsh && printf 'from-sh\n' > workspace/work/fromsh/x.txt")
	written, readErr := ms.Route(ctx, "/workspace/work/fromsh/x.txt").ReadFile(ctx)
	hidden, hiddenErr := run(ctx, srv.Dir(), "cat "+sibling+"; for d in /bin /lib /usr; do touch $d/tacklr-probe 2>&1; done")
	env, envErr := run(ctx, srv.Dir(), "env")
	// The command prints the jail's setup marker too: only the jail's
	// own setup can report a setup failure.
	exit, exitErr := run(ctx, srv.Dir(), "echo "+jailFailMarker+" >&2; exit 125")

	// Assert
	if err := errors.Join(catErr, writeErr, readErr, hiddenErr, envErr, exitErr); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cat, "exit=0 truncated=false\n") || !strings.Contains(cat, body) || !strings.Contains(cat, "session\n") {
		t.Fatalf("cat and ls / = %q, want the tree's text and the jail's /session root", cat)
	}
	if string(written) != "from-sh\n" {
		t.Fatalf("shell write = %q, want it in the tree", written)
	}
	if !strings.Contains(hidden, "No such file") || strings.Count(hidden, "Read-only file system") != 3 {
		t.Fatalf("sibling read and system writes = %q, want the sibling missing and 3 read-only refusals", hidden)
	}
	if got, want := shellVars(env), shellEnv(); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("shell env = %q, want %q", got, want)
	}
	if !strings.HasPrefix(exit, "exit=125 ") {
		t.Fatalf("exit 125 with the setup marker = %q, want it as the command's result", exit)
	}
}

// shellVars is the sorted environment an env command printed, without the
// shell's own working-directory variables.
func shellVars(out string) []string {
	_, stdout, _ := strings.Cut(out, "--- stdout ---\n")
	stdout, _, _ = strings.Cut(stdout, "--- stderr ---")
	var got []string
	for line := range strings.Lines(stdout) {
		line = strings.TrimSpace(line)
		if name, _, _ := strings.Cut(line, "="); line != "" && name != "PWD" && name != "OLDPWD" {
			got = append(got, line)
		}
	}
	slices.Sort(got)
	return got
}

// TestRun_truncatesOversizedOutput: output past the cap is cut, and the
// result says so.
func TestRun_truncatesOversizedOutput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "big.txt"), bytes.Repeat([]byte("a"), outputCap+64), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := run(t.Context(), dir, "cat big.txt")

	if err != nil || !strings.HasPrefix(out, "exit=0 truncated=true\noutput truncated\n") || len(out) > outputCap+128 {
		t.Fatalf("result = %q %v, want the cut output marked truncated", out[:min(len(out), 200)], err)
	}
}

// TestOutputBudget_keepsWhatFitsOfAWrite: a write larger than the budget
// left keeps what fits and marks the output truncated; an empty write
// keeps nothing and is not a truncation. Each write reports all its bytes
// as written, so the command never sees a short write.
func TestOutputBudget_keepsWhatFitsOfAWrite(t *testing.T) {
	t.Parallel()
	b := outputBudget{remaining: 4}
	w := b.writer()

	empty, emptyErr := w.Write(nil)
	emptyTruncated := b.truncated
	n, err := w.Write([]byte("hello"))

	if empty != 0 || emptyErr != nil || emptyTruncated {
		t.Fatalf("empty write = %d %v, truncated %v, want nothing kept and no truncation", empty, emptyErr, emptyTruncated)
	}
	if n != 5 || err != nil || w.buf.String() != "hell" || !b.truncated {
		t.Fatalf("write = %d %v, kept %q, truncated %v, want 5 written, %q kept and truncated", n, err, w.buf.String(), b.truncated, "hell")
	}
}

// TestRun_deadlineKillsTheCommand: a command still running at the deadline
// is killed, and run returns the deadline's error.
func TestRun_deadlineKillsTheCommand(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err := run(ctx, t.TempDir(), "sleep 10")

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}
