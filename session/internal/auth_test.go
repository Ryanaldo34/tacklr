package adapter

import (
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/session"
)

// TestApplyAuth_dropsRebindsAndGivesTheTurnTheTokenOfEachRecipe: an auth
// context drops a recipe by its provider and rebinds another by its alias.
// The rebound recipe keeps the source ids it found before and holds no
// token. A turn then gets one binding for that recipe, with the recipe's
// params and the first real token of its provider.
func TestApplyAuth_dropsRebindsAndGivesTheTurnTheTokenOfEachRecipe(t *testing.T) {
	t.Parallel()
	// Arrange
	start := []session.MountRecipe{
		{Provider: "gdrive", Alias: "docs", Params: map[string]string{mount.ParamName: "docs", mount.ParamFolderID: "fld-1"},
			SourceIDs: []string{mount.ParamFolderID + ":fld-1", "file:abc"}},
		{Provider: "local", Alias: "scratch", Params: map[string]string{mount.ParamName: "scratch"}},
	}
	rebind := session.AuthContext{Drop: []string{"local"}, Bindings: []mount.Binding{{
		Provider: "gdrive",
		Params:   map[string]string{mount.ParamName: "docs", mount.ParamFolderID: "fld-1"},
		Auth:     mount.Credential{Token: "x"},
		Writable: true,
	}}}
	turn := session.AuthContext{Bindings: []mount.Binding{
		{Provider: "gdrive", Auth: mount.Credential{Token: " "}}, // a blank token is not a credential
		{Provider: "gdrive", Auth: mount.Credential{Token: "tok-2"}},
	}}

	// Act
	recipes := ApplyAuth(start, rebind)
	binds := BindingsForTurn(recipes, turn)

	// Assert
	if len(recipes) != 1 || recipes[0].Alias != "docs" || !slices.Equal(recipes[0].SourceIDs, []string{mount.ParamFolderID + ":fld-1", "file:abc"}) {
		t.Fatalf("recipes = %+v, want docs alone with its source ids kept", recipes)
	}
	if len(binds) != 1 || binds[0].Auth.Token != "tok-2" || binds[0].Params[mount.ParamFolderID] != "fld-1" || !binds[0].Writable {
		t.Fatalf("bindings = %+v, want docs with tok-2, its folder and writable", binds)
	}
}
