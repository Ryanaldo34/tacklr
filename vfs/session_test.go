package vfs_test

import (
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMountSession_lifecycle(t *testing.T) {
	ctx := t.Context()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("sess-1", reg)

	if err := ms.Materialize(ctx, []vfs.MountSpec{
		{Point: "/a", Profile: "scratch"},
		{Point: "/b", Profile: "scratch", ReadOnly: true},
	}); err != nil {
		t.Fatal(err)
	}
	if len(ms.Infos()) != 2 {
		t.Fatalf("Infos = %+v", ms.Infos())
	}
	if err := ms.Unmount("/a"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/c", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	specs := ms.Specs()
	if len(specs) != 2 {
		t.Fatalf("Specs = %+v", specs)
	}
	info, rel, err := ms.Lookup("/c/x")
	if err != nil || info.Point != "/c" || rel != "x" {
		t.Fatalf("Lookup = %+v rel=%q err=%v", info, rel, err)
	}

	// Restart shape
	ms2 := vfs.NewMountSession("sess-1", reg)
	if err := ms2.Materialize(ctx, specs); err != nil {
		t.Fatal(err)
	}
	if len(ms2.Specs()) != 2 {
		t.Fatalf("restored = %+v", ms2.Specs())
	}
}

func TestMountSession_errors(t *testing.T) {
	var nilMS *vfs.MountSession
	if err := nilMS.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: "p"}); err == nil {
		t.Fatal("nil Mount")
	}
	if err := nilMS.Unmount("/x"); err == nil {
		t.Fatal("nil Unmount")
	}
	if nilMS.Specs() != nil || nilMS.Infos() != nil {
		t.Fatal("nil lists")
	}
	ms := vfs.NewMountSession("s", nil)
	if err := ms.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: "p"}); err == nil {
		t.Fatal("nil registry Mount")
	}
	reg0 := vfs.NewBackendRegistry()
	msBad := vfs.NewMountSession("s", reg0)
	if err := msBad.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: "nope"}); !errors.Is(err, vfs.ErrUnknownProfile) {
		t.Fatalf("unknown profile Mount: %v", err)
	}
	if err := ms.Materialize(t.Context(), []vfs.MountSpec{{Point: "/x", Profile: "p"}}); err == nil {
		t.Fatal("nil registry Materialize")
	}
	if err := ms.Unmount("/missing"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("unmount empty: %v", err)
	}
	if err := nilMS.Materialize(t.Context(), nil); err == nil {
		t.Fatal("nil Materialize")
	}
	if _, _, err := nilMS.Lookup("/x"); err == nil {
		t.Fatal("nil Lookup")
	}
	// empty materialize clears
	reg := vfs.NewBackendRegistry()
	_ = reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()})
	ms2 := vfs.NewMountSession("s", reg)
	_ = ms2.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: "scratch"})
	if err := ms2.Materialize(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if len(ms2.Specs()) != 0 {
		t.Fatalf("empty materialize: %+v", ms2.Specs())
	}
	// materialize failure leaves tree
	_ = ms2.Mount(t.Context(), vfs.MountSpec{Point: "/keep", Profile: "scratch"})
	if err := ms2.Materialize(t.Context(), []vfs.MountSpec{{Point: "/z", Profile: "missing"}}); err == nil {
		t.Fatal("want materialize error")
	}
	if len(ms2.Specs()) != 1 || ms2.Specs()[0].Point != "/keep" {
		t.Fatalf("tree should be unchanged: %+v", ms2.Specs())
	}
}
