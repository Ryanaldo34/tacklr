package vfs_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestReadText_dirtyIRNotYetOnDisk(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

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

	got, err := ms.ReadText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Text(), "new phrase") || strings.Contains(got.Text(), "old secret") {
		t.Fatalf("dirty ReadText: %q", got.Text())
	}
	disk, err := os.ReadFile(filepath.Join(base, "note.md"))
	if err != nil || string(disk) != "old secret\n" {
		t.Fatalf("backend still old: %q err=%v", disk, err)
	}

	if err := ms.WriteFile(ctx, "/work/pic.bin", []byte{0x00, 0x01, 0xff, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/work/pic.bin"); !errors.Is(err, vfs.ErrNoCodec) && !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("binary ReadText err=%v", err)
	}
	if _, err := ms.ReadText(ctx, "/work"); err == nil {
		t.Fatal("ReadText on directory")
	}

	f, err := ms.Open(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ra, ok := f.(io.ReaderAt)
	if !ok {
		t.Fatal("dirty Open should be io.ReaderAt")
	}
	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 2)
	if err != nil || n != 5 || string(buf) != "Title" {
		t.Fatalf("dirty ReadAt: n=%d %q err=%v", n, buf, err)
	}
}
