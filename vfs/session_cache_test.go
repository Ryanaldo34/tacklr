package vfs_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestContentCache_evictsCleanUnderPressure: soft caps drop clean entries while
// dirty entries remain session-visible.
func TestContentCache_evictsCleanUnderPressure(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("cache-evict", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

	// Create and open >32 files so clean entries exceed maxCacheEntries.
	const n = 40
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("/work/f%02d.txt", i)
		if err := ms.WriteFile(ctx, p, []byte(fmt.Sprintf("body-%d\n", i))); err != nil {
			t.Fatal(err)
		}
		if _, err := ms.ReadText(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	// Dirty one path — must still be readable after further pressure.
	doc, err := ms.ReadText(ctx, "/work/f00.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.SetLine(1, "dirty-keep-me"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	// Touch more clean files to force eviction of other clean entries.
	for i := 0; i < 10; i++ {
		p := fmt.Sprintf("/work/g%02d.txt", i)
		if err := ms.WriteFile(ctx, p, []byte("g\n")); err != nil {
			t.Fatal(err)
		}
		if _, err := ms.ReadText(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	// Dirty body still session-visible
	got, err := ms.ReadText(ctx, "/work/f00.txt")
	if err != nil || !strings.Contains(got.Text(), "dirty-keep-me") {
		t.Fatalf("dirty surviving eviction: %q err=%v", got.Text(), err)
	}
	// Sync flushes dirty
	if err := ms.SyncAll(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadText(ctx, "/work/f00.txt")
	if err != nil || !strings.Contains(got.Text(), "dirty-keep-me") {
		t.Fatalf("after SyncAll: %q err=%v", got.Text(), err)
	}

	// AfterPersist + GetAfterPersist on local
	var saw string
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		saw = path
		return nil
	})
	if ms.GetAfterPersist() == nil {
		t.Fatal("GetAfterPersist")
	}
	if err := ms.WriteFile(ctx, "/work/hook.txt", []byte("h\n")); err != nil {
		t.Fatal(err)
	}
	if saw != "/work/hook.txt" {
		t.Fatalf("AfterPersist %q", saw)
	}
	ms.SetAfterPersist(nil)
	if ms.GetAfterPersist() != nil {
		t.Fatal("clear AfterPersist")
	}

	// Materialize clears cache
	if err := ms.Materialize(ctx, []vfs.MountSpec{{Point: "/work", Profile: "scratch"}}); err != nil {
		t.Fatal(err)
	}
	// Unmount prefix
	if err := ms.Unmount("/work"); err != nil {
		t.Fatal(err)
	}
}

// TestReadLines_streamLargeWindow: files above IR materialize cap stream lines.
func TestReadLines_streamLargeWindow(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("lines-stream", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

	// Build a multi-line file and read a middle window (hits cache or stream).
	var b strings.Builder
	for i := 1; i <= 20; i++ {
		fmt.Fprintf(&b, "line-%d\n", i)
	}
	if err := ms.WriteFile(ctx, "/work/lines.txt", []byte(b.String())); err != nil {
		t.Fatal(err)
	}
	// First open caches IR
	w, err := ms.ReadLines(ctx, "/work/lines.txt", 5, 8)
	if err != nil {
		t.Fatal(err)
	}
	if w.Returned != 3 || w.Lines[0] != "line-5" || w.EOF {
		t.Fatalf("window mid: %+v", w)
	}
	if w.Rev.Hash == "" {
		t.Fatal("expected rev from IR cache")
	}
	// Window past EOF
	w, err = ms.ReadLines(ctx, "/work/lines.txt", 18, 100)
	if err != nil || !w.EOF || w.Returned < 1 {
		t.Fatalf("tail window: %+v err=%v", w, err)
	}
	// Bad range
	if _, err := ms.ReadLines(ctx, "/work/lines.txt", 0, 1); err == nil {
		t.Fatal("start 0")
	}
	if _, err := ms.ReadLines(ctx, "/work/lines.txt", 50, 51); err == nil {
		t.Fatal("start past EOF")
	}
	// MaxLinesPerWindow clamp
	w, err = ms.ReadLines(ctx, "/work/lines.txt", 1, 10000)
	if err != nil || w.Returned > 500 {
		t.Fatalf("clamp: returned=%d err=%v", w.Returned, err)
	}
}
