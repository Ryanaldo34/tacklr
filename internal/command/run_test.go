package command

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_truncatesOversizedOutput(t *testing.T) {
	skipWithoutJail(t)
	dir := t.TempDir()
	big := bytes.Repeat([]byte("a"), outputCap+64)
	if err := os.WriteFile(filepath.Join(dir, "big.txt"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := Run(context.Background(), dir, "cat big.txt")
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
