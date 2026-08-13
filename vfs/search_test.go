package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestSearchText_dirtyIRAndSkipBinary(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	ms := newSearchMountAt(t, base)

	if err := ms.WriteFile(ctx, "/work/note.md", []byte("old secret\n")); err != nil {
		t.Fatal(err)
	}
	doc, err := ms.ReadText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	doc.SetText("# Title\n\nnew phrase lives here\n")
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}

	got, err := ms.SearchText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "new phrase") || strings.Contains(got, "old secret") {
		t.Fatalf("SearchText dirty: %q", got)
	}
	disk, err := os.ReadFile(filepath.Join(base, "note.md"))
	if err != nil || string(disk) != "old secret\n" {
		t.Fatalf("backend still old: %q err=%v", disk, err)
	}

	if err := ms.WriteFile(ctx, "/work/pic.bin", []byte{0x00, 0x01, 0xff, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.SearchText(ctx, "/work/pic.bin"); !errors.Is(err, vfs.ErrNoCodec) && !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("binary SearchText err=%v", err)
	}
	if _, err := ms.SearchText(ctx, "/work"); err == nil {
		t.Fatal("SearchText on directory")
	}
}

func newSearchMountAt(t *testing.T, base string) *vfs.MountSession {
	t.Helper()
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	return ms
}
