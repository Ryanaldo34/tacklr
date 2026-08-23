package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

type downProjection struct{}

func (downProjection) Available() bool                        { return false }
func (downProjection) Attach(*vfs.MountSession, string) error { return nil }

func TestOpenTurnVFS_nilWhenNoRegistryOrProjection(t *testing.T) {
	ctx := t.Context()
	ms, err := OpenTurnVFS(t.Context(), "s", AgentSpec{}, nil, vfs.DirectProjection{})
	if err != nil || ms != nil {
		t.Fatalf("no registry: %v %v", ms, err)
	}
	reg := vfs.NewBackendRegistry()
	ms, err = OpenTurnVFS(ctx, "s", AgentSpec{FSRegistry: reg, FSBootstrap: []vfs.MountSpec{{Point: "/work", Profile: "local"}}}, nil, downProjection{})
	if err != nil || ms != nil {
		t.Fatalf("projection down: %v %v", ms, err)
	}
	CloseTurnVFS(nil, "s", "test")
	ClearSessionVFS(nil, "s")
	ClearSessionVFS(NewCatalog("x"), "")
}

func TestOpenTurnVFS_workspaceFromBindings(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
	binds := []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "tok"},
	}}
	ms, err := OpenTurnVFS(ctx, "sess", AgentSpec{FSRegistry: reg}, binds, vfs.DirectProjection{})
	if err != nil || ms == nil {
		t.Fatalf("open: %v %v", ms, err)
	}
	t.Cleanup(func() { CloseTurnVFS(ms, "sess", "test") })
	b, err := ms.ReadFile(ctx, "/workspace/docs/hello.txt")
	if err != nil || string(b) != "hi" {
		t.Fatalf("read: %q %v", b, err)
	}
}

func TestOpenTurnVFS_rejectsMultiSegmentPoint(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
	_, err := OpenTurnVFS(ctx, "sess", AgentSpec{
		FSRegistry:  reg,
		FSBootstrap: []vfs.MountSpec{{Point: "/work/nested", Profile: "local"}},
	}, nil, vfs.DirectProjection{})
	if err == nil {
		t.Fatal("want multi-segment point error")
	}
}

type failAttach struct{ err error }

func (failAttach) Available() bool                          { return true }
func (f failAttach) Attach(*vfs.MountSession, string) error { return f.err }

func TestOpenTurnVFS_materializeAndAttachErrors(t *testing.T) {
	ctx := t.Context()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	_, err := OpenTurnVFS(ctx, "s", AgentSpec{
		FSRegistry:  reg,
		FSBootstrap: []vfs.MountSpec{{Point: "/work", Profile: "missing"}},
	}, nil, vfs.DirectProjection{})
	if err == nil {
		t.Fatal("want materialize error for unknown profile")
	}

	_, err = OpenTurnVFS(ctx, "s", AgentSpec{
		FSRegistry:  reg,
		FSBootstrap: []vfs.MountSpec{{Point: "/work", Profile: "local"}},
	}, nil, failAttach{err: os.ErrPermission})
	if err == nil {
		t.Fatal("want attach error")
	}
}

func TestOpenTurnVFS_bindSessionError(t *testing.T) {
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	_, err := OpenTurnVFS(t.Context(), "s", AgentSpec{FSRegistry: reg}, []vfs.Binding{{
		Provider: "",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "tok"},
	}}, vfs.DirectProjection{})
	if err == nil {
		t.Fatal("want bind error")
	}
}

func TestClearSessionVFS_clearsFactoryAuth(t *testing.T) {
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s1", vfs.Binding{
		Provider: "gdrive",
		Params:   map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld"},
		Auth:     vfs.Credential{Token: "secret"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth}); err != nil {
		t.Fatal(err)
	}
	cat := NewCatalog("default")
	cat.Register("default", AgentSpec{
		Options:    tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
		FSRegistry: reg,
	})
	ClearSessionVFS(cat, "s1")
	if auth.HasBindings("s1") {
		t.Fatal("want factory tokens cleared")
	}
}

func TestBindingsForTurn_skipsEmptyAliasAndEmptyToken(t *testing.T) {
	recipes := []MountRecipe{
		{Provider: "gdrive", Alias: ""},
		{Provider: "gdrive", Alias: "docs", Params: nil},
	}
	got := BindingsForTurn(recipes, AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "   "},
	}}})
	if len(got) != 0 {
		t.Fatalf("want none, got %+v", got)
	}
	got = BindingsForTurn(recipes, AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "tok"},
	}}})
	if len(got) != 1 || got[0].Params[vfs.ParamName] != "docs" {
		t.Fatalf("token-only provider: %+v", got)
	}
}
