package vfs_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestWorkspace_namedUnionListsAndReadsAliases(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	open := testdrive.Open(t, api, nil)
	ms, err := vfs.Tree(
		vfs.At("contracts", open),
		vfs.At("notes", open),
	)(ctx, "sess-ws", vfs.Request{Bindings: []vfs.Binding{
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}, Auth: vfs.Credential{Token: "tok"}},
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "notes", vfs.ParamFolderID: "root-b"}, Auth: vfs.Credential{Token: "tok"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	if ents, err := ms.Route(ctx, vfs.WorkspacePoint).
		ReadDir(ctx); err != nil || len(ents) != 2 || ents[0].Name != "contracts" || ents[1].Name != "notes" || !ents[0].IsDir {
		t.Fatalf("ReadDir /workspace = %+v err=%v", ents, err)
	}

	if got, err := ms.Route(ctx, "/workspace/contracts/nda.pdf").
		ReadFile(ctx); err != nil || string(got) != "%PDF" {
		t.Fatalf("ReadFile = %q err=%v", got, err)
	}

	if _, err := ms.Route(ctx, "/contracts/nda.pdf").
		ReadFile(ctx); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("old /contracts path: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/nope").
		MkdirAll(ctx); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("mkdir alias: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/contracts").
		Remove(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
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

	if err := ms.Route(ctx, "/workspace/legal/b.txt").
		WriteFile(ctx, []byte("two")); err != nil {
		t.Fatal(err)
	}

	if got, err := ms.Route(ctx, "/workspace/legal/b.txt").
		ReadFile(ctx); err != nil || string(got) != "two" {
		t.Fatalf("write legal = %q err=%v", got, err)
	}

	if err := ms.Route(ctx, "/workspace/ro/a.txt").
		WriteFile(ctx, []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write = %v", err)
	}

	if err := ms.Route(ctx, "/workspace/ro/sub").
		MkdirAll(ctx); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro mkdir = %v", err)
	}

	if err := ms.Route(ctx, "/workspace/ro/a.txt").
		Remove(ctx); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro remove = %v", err)
	}

	if err := ms.Route(ctx, "/workspace").
		MkdirAll(ctx); err != nil {
		t.Fatalf("mkdir workspace root: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/root.txt").
		WriteFile(ctx, []byte("x")); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("write at workspace file = %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace").
		Open(ctx); err == nil {
		t.Fatal("open workspace root as file")
	}

	if _, err := ms.Route(ctx, "/workspace/missing/x").
		Open(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("open missing = %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace").
		OpenDocument(ctx, nil); err == nil {
		t.Fatal("opendoc workspace root")
	}

	if _, err := ms.Route(ctx, "/workspace/missing/x").
		OpenDocument(ctx, nil); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("opendoc missing = %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/a.txt").
		OpenDocument(ctx, nil); err != nil && !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("opendoc local = %v", err)
	}
}
