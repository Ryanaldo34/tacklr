//go:build linux

package command

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func skipWithoutJail(t *testing.T) {
	if !userNSAvailable() {
		t.Skip("user namespaces not available")
	}
}

func TestRun_sessionJailHidesSiblingDir(t *testing.T) {
	skipWithoutJail(t)
	a := t.TempDir()
	b := t.TempDir()
	if err := os.WriteFile(filepath.Join(a, "secret-a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b, "secret-b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := Run(context.Background(), a, "cat secret-a.txt && ls /")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "alpha") {
		t.Fatalf("want session file, got %q", out)
	}
	if !strings.Contains(out, "session") {
		t.Fatalf("want jail root, got %q", out)
	}
	out, err = Run(context.Background(), a, "cat "+filepath.Join(b, "secret-b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "No such file") {
		t.Fatalf("want sibling path missing in jail, got %q", out)
	}
}

func TestRun_agentExit125IsCommandResult(t *testing.T) {
	skipWithoutJail(t)
	out, err := Run(context.Background(), t.TempDir(), "exit 125")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "exit=125") {
		t.Fatalf("want command exit 125, got %q", out)
	}
}

func TestRun_jailShellEnvironmentIsOnlyTheShellEnv(t *testing.T) {
	// Arrange
	skipWithoutJail(t)
	t.Setenv("TACKLR_WORKER_SECRET", "worker-only")

	// Act
	out, err := Run(context.Background(), t.TempDir(), "env")
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
	skipWithoutJail(t)
	dir := t.TempDir()

	// Act
	out, err := Run(context.Background(), dir,
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
