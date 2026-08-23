package vfs

import (
	"errors"
	"strings"
	"testing"
)

func TestFuseProjection_availableAndAttachFailure(t *testing.T) {
	if (FuseProjection{}).Available() != FuseAvailable() {
		t.Fatal("Available must match FuseAvailable")
	}
	if !(DirectProjection{}).Available() {
		t.Fatal("DirectProjection.Available")
	}
	if err := (DirectProjection{}).Attach(nil, "s"); err != nil {
		t.Fatal(err)
	}

	reg := NewBackendRegistry()
	if err := reg.Register(LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := MustNewMountSession(t.Name(), reg)
	if err := ms.Mount(t.Context(), MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	err := FuseProjection{}.Attach(ms, `sess/with\slash`)
	if FuseAvailable() {
		if err != nil {
			t.Fatalf("attach with fuse: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("want attach error without /dev/fuse")
	}
	if !errors.Is(err, ErrFuseNotMounted) && !strings.Contains(err.Error(), "fuse") {
		t.Fatalf("attach err = %v", err)
	}
	for _, id := range []string{"", ".", ".."} {
		_ = FuseProjection{}.Attach(ms, id)
	}
}
