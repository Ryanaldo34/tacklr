package adapter

import (
	"maps"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/vfs"
)

var sourceParamKeys = []string{
	vfs.ParamFolderID,
	vfs.ParamItemID,
	vfs.ParamDriveID,
	vfs.ParamSiteID,
}

// ApplyAuth updates cached recipes from a work-item AuthContext.
// Drop is applied first. Bindings with an alias
// upsert that recipe. Tokens are not stored on the returned recipes.
func ApplyAuth(recipes []durable.MountRecipe, auth durable.AuthContext) []durable.MountRecipe {
	out := cloneRecipes(recipes)
	if len(auth.Drop) > 0 {
		filtered := out[:0]
		for _, r := range out {
			if dropRecipe(r, auth.Drop) {
				continue
			}
			filtered = append(filtered, r)
		}
		out = filtered
	}
	for _, b := range auth.Bindings {
		alias := bindingAlias(b)
		if alias == "" {
			continue
		}
		rec := recipeFromBinding(b, alias)
		replaced := false
		for i, existing := range out {
			if existing.Alias == alias {
				rec.SourceIDs = mergeSourceIDs(existing.SourceIDs, rec.SourceIDs)
				out[i] = rec
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, rec)
		}
	}
	return out
}

// BindingsForTurn builds the secret-bearing mounts for one activity/turn.
// Each cached recipe is included when a token for its provider is on auth.
func BindingsForTurn(recipes []durable.MountRecipe, auth durable.AuthContext) []vfs.Binding {
	byProvider, byAlias := tokenIndex(auth)
	out := make([]vfs.Binding, 0, len(recipes))
	for _, r := range recipes {
		cred, ok := byAlias[r.Alias]
		if !ok {
			cred, ok = byProvider[r.Provider]
		}
		if !ok {
			continue
		}
		params := cloneParams(r.Params)
		params[vfs.ParamName] = r.Alias
		out = append(out, vfs.Binding{
			Provider: r.Provider,
			Point:    vfs.WorkspacePoint,
			Auth:     cred,
			Params:   params,
			Writable: r.Writable,
		})
	}
	return out
}

func recipeFromBinding(b vfs.Binding, alias string) durable.MountRecipe {
	params := cloneParams(b.Params)
	params[vfs.ParamName] = alias
	return durable.MountRecipe{
		Provider:  b.Provider,
		Alias:     alias,
		Params:    params,
		SourceIDs: sourceIDs(params),
		Writable:  b.Writable,
	}
}

func sourceIDs(params map[string]string) []string {
	var ids []string
	for _, k := range sourceParamKeys {
		v := strings.TrimSpace(params[k])
		if v == "" {
			continue
		}
		ids = append(ids, k+":"+v)
	}
	return ids
}

func mergeSourceIDs(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, id := range slices.Concat(a, b) {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func bindingAlias(b vfs.Binding) string {
	if name := strings.TrimSpace(b.Params[vfs.ParamName]); name != "" {
		return name
	}
	point := strings.TrimSpace(b.Point)
	if point == "" || point == vfs.WorkspacePoint {
		return ""
	}
	return strings.TrimPrefix(point, "/")
}

func dropRecipe(r durable.MountRecipe, drop []string) bool {
	for _, d := range drop {
		d = strings.TrimSpace(d)
		if r.Alias == d || r.Provider == d {
			return true
		}
	}
	return false
}

func tokenIndex(auth durable.AuthContext) (byProvider, byAlias map[string]vfs.Credential) {
	byProvider = make(map[string]vfs.Credential)
	byAlias = make(map[string]vfs.Credential)
	for _, b := range auth.Bindings {
		if strings.TrimSpace(b.Auth.Token) == "" {
			continue
		}
		if b.Provider != "" {
			byProvider[b.Provider] = b.Auth
		}
		if alias := bindingAlias(b); alias != "" {
			byAlias[alias] = b.Auth
		}
	}
	return
}

func cloneParams(p map[string]string) map[string]string {
	out := maps.Clone(p)
	if out == nil {
		out = map[string]string{}
	}
	return out
}

func cloneRecipes(in []durable.MountRecipe) []durable.MountRecipe {
	if len(in) == 0 {
		return nil
	}
	out := make([]durable.MountRecipe, len(in))
	for i, r := range in {
		out[i] = r
		out[i].Params = maps.Clone(r.Params)
		out[i].SourceIDs = slices.Clone(r.SourceIDs)
	}
	return out
}
