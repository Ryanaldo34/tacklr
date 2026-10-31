package adapter

import (
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestApplyAuthDropByAliasThenProvider(t *testing.T) {
	start := []session.MountRecipe{
		{Provider: "gdrive", Alias: "docs", Params: map[string]string{vfs.ParamName: "docs"}},
		{Provider: "local", Alias: "scratch", Params: map[string]string{vfs.ParamName: "scratch"}},
	}
	got := session.ApplyAuth(start, session.AuthContext{Drop: []string{"docs"}})
	if len(got) != 1 || got[0].Alias != "scratch" {
		t.Fatalf("after alias drop: %+v", got)
	}
	got = session.ApplyAuth(got, session.AuthContext{Drop: []string{"local"}})
	if len(got) != 0 {
		t.Fatalf("after provider drop: %+v", got)
	}
}

func TestBindingsForTurnUsesCachedRecipePlusProviderToken(t *testing.T) {
	recipes := []session.MountRecipe{{
		Provider:  "gdrive",
		Alias:     "docs",
		Params:    map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld-1"},
		SourceIDs: []string{vfs.ParamFolderID + ":fld-1"},
		Writable:  true,
	}}
	auth := session.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "tok-2"},
	}}}
	binds := session.BindingsForTurn(recipes, auth)
	if len(binds) != 1 {
		t.Fatalf("len=%d", len(binds))
	}
	if binds[0].Auth.Token != "tok-2" {
		t.Fatalf("token=%q", binds[0].Auth.Token)
	}
	if binds[0].Params[vfs.ParamFolderID] != "fld-1" {
		t.Fatalf("want cached folderId, got %v", binds[0].Params)
	}
	if !binds[0].Writable {
		t.Fatal("want writable from recipe")
	}
	if got := session.BindingsForTurn(recipes, session.AuthContext{}); len(got) != 0 {
		t.Fatalf("want no bindings without token, got %+v", got)
	}
}

func TestApplyAuthKeepsPriorSourceIDsOnRebind(t *testing.T) {
	start := []session.MountRecipe{{
		Provider:  "gdrive",
		Alias:     "docs",
		Params:    map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld-1"},
		SourceIDs: []string{vfs.ParamFolderID + ":fld-1", "file:abc"},
	}}
	got := session.ApplyAuth(start, session.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Params:   map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld-1"},
		Auth:     vfs.Credential{Token: "x"},
	}}})
	if !slices.Contains(got[0].SourceIDs, "file:abc") {
		t.Fatalf("lost discovered source id: %v", got[0].SourceIDs)
	}
}
