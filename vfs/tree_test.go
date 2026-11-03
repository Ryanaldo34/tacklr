package vfs_test

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestTree_aMemberNeedsANameAndAnOpenAndAnEmptyOneIsSkipped: a member with
// no name or no Open fails the tree; a member whose Open returns no provider
// is left out of it.
func TestTree_aMemberNeedsANameAndAnOpenAndAnEmptyOneIsSkipped(t *testing.T) {
	skip := vfs.Open(func(context.Context, string, mount.Binding) (vfs.Provider, error) { return nil, nil })

	_, noName := vfs.Tree(vfs.At("", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	_, noOpen := vfs.Tree(vfs.At("work", nil))(t.Context(), t.Name(), vfs.Request{})
	ms, err := vfs.Tree(vfs.At("gone", skip), vfs.At("work", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	ents, readErr := ms.Route(t.Context(), mount.WorkspacePoint).ReadDir(t.Context())

	if noName == nil || noOpen == nil {
		t.Fatalf("no name = %v, no open = %v; want both refused", noName, noOpen)
	}
	if readErr != nil || len(ents) != 1 || ents[0].Name != "work" {
		t.Fatalf("ReadDir = %+v err=%v, want only the member that opened", ents, readErr)
	}
}
