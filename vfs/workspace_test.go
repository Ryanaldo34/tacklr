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
	ms, err := vfs.Tree(
		vfs.At("contracts", vfs.Drive(api)),
		vfs.At("notes", vfs.Drive(api)),
	)(ctx, "sess-ws", vfs.Request{Bindings: []vfs.Binding{
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}, Auth: vfs.Credential{Token: "tok"}},
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "notes", vfs.ParamFolderID: "root-b"}, Auth: vfs.Credential{Token: "tok"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

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
	a, b := t.TempDir(), t.TempDir()
	_, err := vfs.Tree(vfs.At("legal", vfs.Local(a)), vfs.At("legal", vfs.Local(b)))(t.Context(), t.Name(), vfs.Request{})
	if !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("dup alias = %v", err)
	}
}

func TestWorkspace_writableMemberAndReadOnlyMember(t *testing.T) {
	ctx := t.Context()
	host := t.TempDir()
	if err := os.WriteFile(filepath.Join(host, "a.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(
		vfs.At("legal", vfs.Local(host)),
		vfs.At("ro", vfs.Local(host)).ReadOnly(),
	)(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
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
