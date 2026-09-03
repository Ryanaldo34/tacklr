//go:build linux

package command

import (
	"context"
	"os"
	"path/filepath"
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
