package vfs_test

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestTree_requiresNameAndOpen(t *testing.T) {
	if _, err := vfs.Tree(vfs.At("", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{}); err == nil {
		t.Fatal("empty At name")
	}
	if _, err := vfs.Tree(vfs.At("work", nil))(t.Context(), t.Name(), vfs.Request{}); err == nil {
		t.Fatal("nil open")
	}
}

func TestTree_skipsNilProvider(t *testing.T) {
	skip := vfs.Open(func(context.Context, string, mount.Binding) (vfs.Provider, error) {
		return nil, nil
	})
	ms, err := vfs.Tree(vfs.At("gone", skip), vfs.At("work", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	if ents, err := ms.Route(t.Context(), mount.WorkspacePoint).
		ReadDir(t.Context()); err != nil || len(ents) != 1 || ents[0].Name != "work" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
}
