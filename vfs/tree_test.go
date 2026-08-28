package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestTree_hostMembersUnderWorkspace(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("work", builtins.Local(dir)))(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	specs := ms.Specs()
	if len(specs) != 1 || specs[0].Point != vfs.WorkspacePoint {
		t.Fatalf("Specs = %+v", specs)
	}
	got, err := ms.ReadFile(ctx, "/workspace/work/a.txt")
	if err != nil || string(got) != "hi" {
		t.Fatalf("ReadFile = %q err=%v", got, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/b.txt", []byte("new")); err != nil {
		t.Fatal(err)
	}
}

func TestTree_duplicateAtIsAmbiguous(t *testing.T) {
	dir := t.TempDir()
	_, err := vfs.Tree(vfs.At("work", builtins.Local(dir)), vfs.At("work", builtins.Local(dir)))(t.Context(), t.Name(), vfs.Request{})
	if !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("dup At = %v", err)
	}
}

func TestTree_requiresNameAndOpen(t *testing.T) {
	if _, err := vfs.Tree(vfs.At("", builtins.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{}); err == nil {
		t.Fatal("empty At name")
	}
	if _, err := vfs.Tree(vfs.At("work", nil))(t.Context(), t.Name(), vfs.Request{}); err == nil {
		t.Fatal("nil open")
	}
}

func TestTree_skipsNilProvider(t *testing.T) {
	skip := vfs.Open(func(context.Context, string, vfs.Binding) (vfs.Provider, error) {
		return nil, nil
	})
	ms, err := vfs.Tree(vfs.At("gone", skip), vfs.At("work", builtins.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	ents, err := ms.ReadDir(t.Context(), vfs.WorkspacePoint)
	if err != nil || len(ents) != 1 || ents[0].Name != "work" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
}

func TestTree_readOnlyHostMember(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("ro", builtins.Local(dir)).ReadOnly())(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.WriteFile(ctx, "/workspace/ro/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("write ro: %v", err)
	}
}

func TestTree_driveFakeInjected(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	ms, err := vfs.Tree(
		vfs.At("contracts", builtins.Drive(api)),
		vfs.At("notes", builtins.Drive(api)),
	)(ctx, t.Name(), vfs.Request{Bindings: []vfs.Binding{
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}, Auth: vfs.Credential{Token: "tok"}},
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "notes", vfs.ParamFolderID: "root-b"}, Auth: vfs.Credential{Token: "tok"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	got, err := ms.ReadFile(ctx, "/workspace/contracts/nda.pdf")
	if err != nil || string(got) != "%PDF" {
		t.Fatalf("contracts: %q err=%v", got, err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/notes/readme.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("notes: %q err=%v", got, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/contracts/nda.pdf", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("default bind is read-only: %v", err)
	}
}

func TestTree_driveWritableBind(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	ms, err := vfs.Tree(vfs.At("contracts", builtins.Drive(api)))(ctx, t.Name(), vfs.Request{Bindings: []vfs.Binding{{
		Provider: vfs.ProviderGoogleDrive, Writable: true,
		Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"},
		Auth:   vfs.Credential{Token: "tok"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.WriteFile(ctx, "/workspace/contracts/new.txt", []byte("ok")); err != nil {
		t.Fatal(err)
	}
}

func TestTree_unionSkills(t *testing.T) {
	ctx := t.Context()
	a, b := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(a, "one.md"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b, "two.md"), []byte("2"), 0o644); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("skills", vfs.Union(builtins.Local(a), builtins.Local(b))))(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	ents, err := ms.ReadDir(ctx, "/workspace/skills")
	if err != nil || len(ents) != 2 {
		t.Fatalf("skills dir = %+v err=%v", ents, err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/skills/one.md")
	if err != nil || string(got) != "1" {
		t.Fatalf("one: %q err=%v", got, err)
	}
}

func TestTree_indexedPolicyOnMember(t *testing.T) {
	ms, err := vfs.Tree(
		vfs.At("work", builtins.Local(t.TempDir())),
		vfs.At("auto", builtins.Local(t.TempDir())).Indexed("prefix"),
		vfs.At("off", builtins.Local(t.TempDir())).Indexed("none"),
	)(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	work, err := ms.SpecAt("/workspace/work/a.txt")
	if err != nil || work.IndexPolicy != "" {
		t.Fatalf("work policy: %+v err=%v", work, err)
	}
	auto, err := ms.SpecAt("/workspace/auto/a.txt")
	if err != nil || auto.IndexPolicy != "prefix" {
		t.Fatalf("auto policy: %+v err=%v", auto, err)
	}
	off, err := ms.SpecAt("/workspace/off/a.txt")
	if err != nil || off.IndexPolicy != "none" {
		t.Fatalf("off policy: %+v err=%v", off, err)
	}
}

func TestTree_memberProfileAndBindParams(t *testing.T) {
	ms, err := vfs.Tree(vfs.At("discovery", builtins.Memory()).Profile("brain"))(t.Context(), t.Name(), vfs.Request{
		Bindings: []vfs.Binding{{
			Params: map[string]string{vfs.ParamName: "discovery", "mode": "roots", "kind": "Discovery"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	spec, err := ms.SpecAt("/workspace/discovery/x.md")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Profile != "brain" || spec.Params["mode"] != "roots" || spec.Params["kind"] != "Discovery" {
		t.Fatalf("member spec = %+v", spec)
	}
	if spec.Point != "/workspace/discovery" {
		t.Fatalf("member point = %q", spec.Point)
	}
}

func TestBindingByName_matchesAliasOrProvider(t *testing.T) {
	binds := []vfs.Binding{
		{Provider: "gdrive", Params: map[string]string{vfs.ParamName: "docs"}, Auth: vfs.Credential{Token: "t"}},
		{Provider: "msgraph", Params: map[string]string{vfs.ParamName: "legal"}, Auth: vfs.Credential{Token: "u"}},
	}
	if _, ok := vfs.BindingByName(binds, "docs"); !ok {
		t.Fatal("alias docs")
	}
	if _, ok := vfs.BindingByName(binds, "msgraph"); !ok {
		t.Fatal("provider msgraph")
	}
	if _, ok := vfs.BindingByName(binds, "missing"); ok {
		t.Fatal("missing")
	}
}
