package shell

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
)

// TestRun_seesAndWritesTheMountedTree: on a FUSE-mounted session tree the
// shell reads the tree's current text, a failing command is a result, and a
// file the shell writes lands in the tree.
func TestRun_seesAndWritesTheMountedTree(t *testing.T) {
	// Arrange
	ctx := t.Context()
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir())))(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	const body = "dirty body unique phrase xyzzy-tacklr\n"
	if err := ms.Route(ctx, "/workspace/work/note.md").WriteFile(ctx, []byte(body)); err != nil {
		t.Fatal(err)
	}
	srv, err := fuse.Mount(ms, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// Act
	cat, catErr := run(ctx, srv.Dir(), "cat workspace/work/note.md")
	failed, failedErr := run(ctx, srv.Dir(), "false")
	_, writeErr := run(ctx, srv.Dir(), "mkdir -p workspace/work/fromsh && printf 'from-sh\n' > workspace/work/fromsh/x.txt")
	written, readErr := ms.Route(ctx, "/workspace/work/fromsh/x.txt").ReadFile(ctx)

	// Assert
	if catErr != nil || !strings.Contains(cat, "exit=0") || !strings.Contains(cat, body) {
		t.Fatalf("cat = %q err=%v, want the tree's text", cat, catErr)
	}
	if failedErr != nil || !strings.Contains(failed, "exit=1") {
		t.Fatalf("false = %q err=%v, want exit=1 as a result", failed, failedErr)
	}
	if writeErr != nil || readErr != nil || string(written) != "from-sh\n" {
		t.Fatalf("shell write = %q err=%v/%v, want it in the tree", written, writeErr, readErr)
	}
}

func TestRun_deadlineKillsTheCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err := run(ctx, t.TempDir(), "sleep 10")

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}
