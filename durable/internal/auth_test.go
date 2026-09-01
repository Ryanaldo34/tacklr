package adapter

import (
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestApplyAuthUpsertsRecipesAndRecordsSourceIDs(t *testing.T) {
	auth := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: vfs.ProviderGoogleDrive,
		Params: map[string]string{
			vfs.ParamName:     "docs",
			vfs.ParamFolderID: "fld-1",
		},
		Auth:     vfs.Credential{Token: "secret"},
		Writable: true,
	}}}
	got := ApplyAuth(nil, auth)
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].Provider != vfs.ProviderGoogleDrive || got[0].Alias != "docs" || !got[0].Writable {
		t.Fatalf("recipe=%+v", got[0])
	}
	if got[0].Params[vfs.ParamFolderID] != "fld-1" {
		t.Fatalf("params=%v", got[0].Params)
	}
	if !slices.Equal(got[0].SourceIDs, []string{vfs.ParamFolderID + ":fld-1"}) {
		t.Fatalf("sourceIds=%v", got[0].SourceIDs)
	}
	if got[0].Params[vfs.ParamName] != "docs" {
		t.Fatalf("alias param missing")
	}
}

func TestApplyAuthDropByAliasThenProvider(t *testing.T) {
	start := []durable.MountRecipe{
		{Provider: "gdrive", Alias: "docs", Params: map[string]string{vfs.ParamName: "docs"}},
		{Provider: "local", Alias: "scratch", Params: map[string]string{vfs.ParamName: "scratch"}},
	}
	got := ApplyAuth(start, durable.AuthContext{Drop: []string{"docs"}})
	if len(got) != 1 || got[0].Alias != "scratch" {
		t.Fatalf("after alias drop: %+v", got)
	}
	got = ApplyAuth(got, durable.AuthContext{Drop: []string{"local"}})
	if len(got) != 0 {
		t.Fatalf("after provider drop: %+v", got)
	}
}

func TestBindingsForTurnUsesCachedRecipePlusProviderToken(t *testing.T) {
	recipes := []durable.MountRecipe{{
		Provider:  "gdrive",
		Alias:     "docs",
		Params:    map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld-1"},
		SourceIDs: []string{vfs.ParamFolderID + ":fld-1"},
		Writable:  true,
	}}
	auth := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "tok-2"},
	}}}
	binds := BindingsForTurn(recipes, auth)
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
}

func TestApplyAuthKeepsPriorSourceIDsOnRebind(t *testing.T) {
	start := []durable.MountRecipe{{
		Provider:  "gdrive",
		Alias:     "docs",
		Params:    map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld-1"},
		SourceIDs: []string{vfs.ParamFolderID + ":fld-1", "file:abc"},
	}}
	got := ApplyAuth(start, durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Params:   map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld-1"},
		Auth:     vfs.Credential{Token: "x"},
	}}})
	if !slices.Contains(got[0].SourceIDs, "file:abc") {
		t.Fatalf("lost discovered source id: %v", got[0].SourceIDs)
	}
}
