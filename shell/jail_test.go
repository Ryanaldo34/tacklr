package shell

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRun_truncatesOversizedOutput(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("a"), outputCap+64)
	if err := os.WriteFile(filepath.Join(dir, "big.txt"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := run(context.Background(), dir, "cat big.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "truncated=true") || !strings.Contains(out, "output truncated") {
		t.Fatalf("want truncated result, got %q", out[:min(len(out), 200)])
	}
}

func TestOutputBudget_emptyAndExhaustedWrites(t *testing.T) {
	b := outputBudget{remaining: 4}
	w := b.writer()
	n, err := w.Write(nil)
	if err != nil || n != 0 || b.truncated {
		t.Fatalf("empty write: n=%d err=%v truncated=%v", n, err, b.truncated)
	}
	n, err = w.Write([]byte("hello"))
	if err != nil || n != 5 || !b.truncated || b.remaining != 0 {
		t.Fatalf("partial: n=%d err=%v remaining=%d truncated=%v", n, err, b.remaining, b.truncated)
	}
	n, err = w.Write([]byte("more"))
	if err != nil || n != 4 || !b.truncated {
		t.Fatalf("exhausted: n=%d err=%v truncated=%v", n, err, b.truncated)
	}
}

func TestRun_sessionJailHidesSiblingDir(t *testing.T) {
	a := t.TempDir()
	b := t.TempDir()
	if err := os.WriteFile(filepath.Join(a, "secret-a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b, "secret-b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := run(context.Background(), a, "cat secret-a.txt && ls /")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "alpha") {
		t.Fatalf("want session file, got %q", out)
	}
	if !strings.Contains(out, "session") {
		t.Fatalf("want jail root, got %q", out)
	}
	out, err = run(context.Background(), a, "cat "+filepath.Join(b, "secret-b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "No such file") {
		t.Fatalf("want sibling path missing in jail, got %q", out)
	}
}

func TestRun_agentExit125IsCommandResult(t *testing.T) {
	out, err := run(context.Background(), t.TempDir(), "exit 125")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "exit=125") {
		t.Fatalf("want command exit 125, got %q", out)
	}
}

func TestRun_jailShellEnvironmentIsOnlyTheShellEnv(t *testing.T) {
	// Arrange
	t.Setenv("TACKLR_WORKER_SECRET", "worker-only")

	// Act
	out, err := run(context.Background(), t.TempDir(), "env")
	if err != nil {
		t.Fatal(err)
	}

	// Assert: every variable is from shellEnv, apart from the shell's own
	// working-directory bookkeeping.
	stdout, _, _ := strings.Cut(strings.SplitN(out, "--- stdout ---\n", 2)[1], "--- stderr ---")
	var got []string
	for line := range strings.Lines(stdout) {
		line = strings.TrimSpace(line)
		if name, _, _ := strings.Cut(line, "="); line != "" && name != "PWD" && name != "OLDPWD" {
			got = append(got, line)
		}
	}
	want := shellEnv()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("shell env = %q, want %q", got, want)
	}
}

func TestRun_jailSystemDirsAreReadOnlyAndSessionIsWritable(t *testing.T) {
	// Arrange
	dir := t.TempDir()

	// Act
	out, err := run(context.Background(), dir,
		`for d in /bin /lib /usr; do touch $d/tacklr-probe 2>&1; done; echo hi > /session/ok`)
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	if n := strings.Count(out, "Read-only file system"); n != 3 {
		t.Fatalf("want 3 read-only refusals, got %d: %q", n, out)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "ok")); err != nil || string(b) != "hi\n" {
		t.Fatalf("session write: %q %v", b, err)
	}
}
