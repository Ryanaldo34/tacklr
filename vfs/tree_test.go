package vfs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestTree_aMemberNeedsANameAndAnOpenAndAnEmptyOneIsSkipped: a member with
// no name or no Open fails the tree, two members with one name are
// ambiguous, and a canceled context fails the tree, also
// for an Open that returns a provider it built before; a member whose Open
// returns no provider is left out of the tree.
func TestTree_aMemberNeedsANameAndAnOpenAndAnEmptyOneIsSkipped(t *testing.T) {
	skip := vfs.Open(func(context.Context, string, mount.Binding) (vfs.Provider, error) { return nil, nil })
	built, err := vfs.NewLocalProvider(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	prebuilt := vfs.Open(func(context.Context, string, mount.Binding) (vfs.Provider, error) { return built, nil })
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	_, noName := vfs.Tree(vfs.At("", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	_, noOpen := vfs.Tree(vfs.At("work", nil))(t.Context(), t.Name(), vfs.Request{})
	_, twice := vfs.Tree(vfs.At("legal", vfs.Local(t.TempDir())), vfs.At("legal", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	_, canceledErr := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir())))(canceled, t.Name(), vfs.Request{})
	_, canceledPrebuilt := vfs.Tree(vfs.At("work", prebuilt))(canceled, t.Name(), vfs.Request{})
	ms, err := vfs.Tree(vfs.At("gone", skip), vfs.At("work", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	ents, readErr := ms.Route(t.Context(), mount.WorkspacePoint).ReadDir(t.Context())

	if noName == nil || noOpen == nil {
		t.Fatalf("no name = %v, no open = %v; want both refused", noName, noOpen)
	}
	if !errors.Is(twice, vfs.ErrAmbiguous) {
		t.Fatalf("one name twice = %v, want ErrAmbiguous", twice)
	}
	if !errors.Is(canceledErr, context.Canceled) || !errors.Is(canceledPrebuilt, context.Canceled) {
		t.Fatalf("canceled open = %v, with a prebuilt provider = %v", canceledErr, canceledPrebuilt)
	}
	if readErr != nil || len(ents) != 1 || ents[0].Name != "work" {
		t.Fatalf("ReadDir = %+v err=%v, want only the member that opened", ents, readErr)
	}
}

// WithCodecs fails the open when the tree fails, and when a codec is for a
// type the session already has.
func TestWithCodecs_failsTheOpen(t *testing.T) {
	ctx := t.Context()

	_, badTree := vfs.Tree(vfs.At("", nil)).WithCodecs()(ctx, "s", vfs.Request{})
	_, dup := vfs.Tree().WithCodecs(vfs.TextCodec{})(ctx, "s", vfs.Request{})

	if badTree == nil {
		t.Fatal("a tree that fails to open must fail WithCodecs")
	}
	if !errors.Is(dup, vfs.ErrAlreadyRegistered) {
		t.Fatalf("second text codec = %v, want ErrAlreadyRegistered", dup)
	}
}
