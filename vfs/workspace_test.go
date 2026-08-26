package vfs_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestWorkspace_namedUnionListsAndReadsAliases(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("sess-ws", vfs.Binding{
		Provider: vfs.ProviderGoogleDrive, Point: "/contracts",
		Auth: vfs.Credential{Token: "tok"}, Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("sess-ws", vfs.Binding{
		Provider: vfs.ProviderGoogleDrive, Point: "/notes",
		Auth: vfs.Credential{Token: "tok"}, Params: map[string]string{vfs.ParamFolderID: "root-b"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("sess-ws", reg)
	if err != nil {
		t.Fatal(err)
	}
	members := []vfs.MountSpec{
		vfs.BindingMember(vfs.Binding{Provider: "gdrive", Point: "/contracts", Params: map[string]string{vfs.ParamFolderID: "root-a"}}),
		vfs.BindingMember(vfs.Binding{Provider: "gdrive", Point: "/notes", Params: map[string]string{vfs.ParamFolderID: "root-b"}}),
	}
	if err := ms.Mount(ctx, vfs.Workspace(members...)); err != nil {
		t.Fatal(err)
	}

	ents, err := ms.ReadDir(ctx, vfs.WorkspacePoint)
	if err != nil || len(ents) != 2 || ents[0].Name != "contracts" || ents[1].Name != "notes" || !ents[0].IsDir {
		t.Fatalf("ReadDir /workspace = %+v err=%v", ents, err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/contracts/nda.pdf")
	if err != nil || string(got) != "%PDF" {
		t.Fatalf("ReadFile = %q err=%v", got, err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/nda.pdf"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("old /contracts path: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/nope"); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("mkdir alias: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("remove alias: %v", err)
	}
}

func TestWorkspace_duplicateAliasIsAmbiguous(t *testing.T) {
	ctx := t.Context()
	a, b := t.TempDir(), t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "a", Base: a}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "b", Base: b}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession(t.Name(), reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	dup := vfs.MountSpec{Profile: "a", Params: map[string]string{vfs.ParamName: "legal"}}
	other := vfs.MountSpec{Profile: "b", Params: map[string]string{vfs.ParamName: "legal"}}
	if err := ms.Mount(ctx, vfs.Workspace(dup, other)); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("dup alias = %v", err)
	}
	nested := vfs.MountSpec{Profile: "a", Params: map[string]string{vfs.ParamName: "x"}, Members: []vfs.MountSpec{{Profile: "b"}}}
	if err := ms.Mount(ctx, vfs.Workspace(nested)); err == nil {
		t.Fatal("nested workspace members")
	}
	if err := ms.Mount(ctx, vfs.Workspace(vfs.MountSpec{Params: map[string]string{vfs.ParamName: "z"}})); err == nil {
		t.Fatal("member profile required")
	}
	if err := ms.Mount(ctx, vfs.Workspace(vfs.MountSpec{Profile: "a"})); err == nil {
		t.Fatal("member name required")
	}
}

func TestWorkspace_writableMemberAndReadOnlyMember(t *testing.T) {
	ctx := t.Context()
	host := t.TempDir()
	if err := os.WriteFile(filepath.Join(host, "a.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: host}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession(t.Name(), reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Mount(ctx, vfs.Workspace(
		vfs.MountSpec{Profile: "scratch", Params: map[string]string{vfs.ParamName: "legal"}},
		vfs.MountSpec{Profile: "scratch", ReadOnly: true, Params: map[string]string{vfs.ParamName: "ro"}},
	)); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/legal/b.txt", []byte("two")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/legal/b.txt")
	if err != nil || string(got) != "two" {
		t.Fatalf("write legal = %q err=%v", got, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/ro/a.txt", []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write = %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/ro/sub"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro mkdir = %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/ro/a.txt"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro remove = %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace"); err != nil {
		t.Fatalf("mkdir workspace root: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/root.txt", []byte("x")); !errors.Is(err, vfs.ErrNotExist) && !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("write at workspace file = %v", err)
	}
	if _, err := ms.Open(ctx, "/workspace"); err == nil {
		t.Fatal("open workspace root as file")
	}
	if _, err := ms.Open(ctx, "/workspace/missing/x"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("open missing = %v", err)
	}
	if _, err := ms.OpenDocument(ctx, "/workspace", nil); err == nil {
		t.Fatal("opendoc workspace root")
	}
	if _, err := ms.OpenDocument(ctx, "/workspace/missing/x", nil); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("opendoc missing = %v", err)
	}
	if _, err := ms.OpenDocument(ctx, "/workspace/legal/a.txt", nil); err != nil && !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("opendoc local = %v", err)
	}
}
