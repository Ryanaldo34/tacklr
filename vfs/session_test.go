package vfs

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

func TestMount_rejectsBadProviderAndDuplicate(t *testing.T) {
	ctx := t.Context()
	p, err := Local(t.TempDir())(ctx, "s", mount.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := NewMountSession("s")
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.mount(ctx, MountSpec{Point: "/x"}, p); !errors.Is(err, ErrInvalidProvider) {
		t.Fatalf("empty profile: %v", err)
	}
	if err := ms.mount(ctx, MountSpec{Point: mount.WorkspacePoint, Profile: "workspace"}, p); err != nil {
		t.Fatal(err)
	}
	if err := ms.mount(ctx, MountSpec{Point: mount.WorkspacePoint, Profile: "workspace"}, p); !errors.Is(err, ErrAlreadyMounted) {
		t.Fatalf("dup mount: %v", err)
	}
	if err := ms.mount(ctx, MountSpec{Point: "/z", Profile: "z"}, nil); err == nil {
		t.Fatal("nil provider")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	ms2, err := NewMountSession("s2")
	if err != nil {
		t.Fatal(err)
	}
	if err := ms2.mount(cctx, MountSpec{Point: mount.WorkspacePoint, Profile: "workspace"}, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("mount cancel: %v", err)
	}
}

// WithCodecs fails the open when the tree fails, and when a codec is for a
// type the session already has.
func TestWithCodecs_failsTheOpen(t *testing.T) {
	ctx := t.Context()

	_, badTree := Tree(At("", nil)).WithCodecs()(ctx, "s", Request{})
	_, dup := Tree().WithCodecs(TextCodec{})(ctx, "s", Request{})

	if badTree == nil {
		t.Fatal("a tree that fails to open must fail WithCodecs")
	}
	if !errors.Is(dup, ErrAlreadyRegistered) {
		t.Fatalf("second text codec = %v, want ErrAlreadyRegistered", dup)
	}
}
