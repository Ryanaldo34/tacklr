package vfs_test

import (
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestWorkspace_listsAndReadsEachDriveFolderAtItsName: two Drive folders
// mount under /workspace by their names, their files read there and not
// at the old top-level path, and a shortcut to a folder walks by the
// shortcut's name.
func TestWorkspace_listsAndReadsEachDriveFolderAtItsName(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	open := testdrive.Open(t, testdrive.Tree(), nil)
	ms, err := vfs.Tree(
		vfs.At("contracts", open),
		vfs.At("notes", open),
	)(ctx, "sess-ws", vfs.Request{Bindings: []mount.Binding{
		{Provider: mount.ProviderGoogleDrive, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"}, Auth: mount.Credential{Token: "tok"}},
		{Provider: mount.ProviderGoogleDrive, Params: map[string]string{mount.ParamName: "notes", mount.ParamFolderID: "root-b"}, Auth: mount.Credential{Token: "tok"}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	ents, listErr := ms.Route(ctx, mount.WorkspacePoint).ReadDir(ctx)
	nda, ndaErr := ms.Route(ctx, "/workspace/contracts/nda.pdf").ReadFile(ctx)
	_, oldErr := ms.Route(ctx, "/contracts/nda.pdf").ReadFile(ctx)
	note, noteErr := ms.Route(ctx, "/workspace/notes/alias/note.md").ReadFile(ctx)

	if listErr != nil || len(ents) != 2 || ents[0].Name != "contracts" || ents[1].Name != "notes" || !ents[0].IsDir {
		t.Fatalf("ReadDir /workspace = %+v %v", ents, listErr)
	}
	if ndaErr != nil || string(nda) != "%PDF" || !errors.Is(oldErr, vfs.ErrNotMounted) {
		t.Fatalf("nda.pdf = %q %v, at the old path = %v", nda, ndaErr, oldErr)
	}
	if noteErr != nil || string(note) != "# hi\n\n" {
		t.Fatalf("read through the shortcut = %q %v", note, noteErr)
	}
}

// TestWorkspace_aMemberThatIsNotMountedIsNotFoundAndTheRootListsTheWorkspace:
// every call on a path of a member that is not mounted fails, as not found
// or, for a new directory, as not supported, / lists the workspace, and
// /workspace is a directory.
func TestWorkspace_aMemberThatIsNotMountedIsNotFoundAndTheRootListsTheWorkspace(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms, err := vfs.Tree(vfs.At("legal", vfs.Local(t.TempDir())))(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	root, rootErr := ms.Route(ctx, "/").ReadDir(ctx)
	workspace, workspaceErr := ms.Route(ctx, "/workspace").Stat(ctx)
	failed := map[string]error{}
	for name, op := range routeOps {
		failed[name] = op(ctx, ms.Route(ctx, "/workspace/missing/x"))
	}

	if rootErr != nil || len(root) != 1 || root[0].Name != "workspace" || !root[0].IsDir {
		t.Fatalf("list / = %+v %v, want [workspace]", root, rootErr)
	}
	if workspaceErr != nil || !workspace.IsDir {
		t.Fatalf("stat /workspace = %+v %v, want a directory", workspace, workspaceErr)
	}
	for name, err := range failed {
		want := vfs.ErrNotExist
		if name == "mkdir" {
			want = vfs.ErrNotSupported
		}
		if !errors.Is(err, want) {
			t.Errorf("%s = %v, want %v", name, err, want)
		}
	}
}
