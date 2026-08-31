package durable

import (
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr/vfs"
)

// WithoutSecrets returns a copy with Credential Token and ExpiresAt cleared.
// Binding metadata (provider, alias, params, writable) is kept. The input is
// not modified.
func (a AuthContext) WithoutSecrets() AuthContext {
	out := cloneAuth(a)
	for i := range out.Bindings {
		out.Bindings[i].Auth = vfs.Credential{}
	}
	return out
}

func cloneAuth(a AuthContext) AuthContext {
	out := AuthContext{Drop: slices.Clone(a.Drop)}
	if len(a.Bindings) == 0 {
		return out
	}
	out.Bindings = make([]vfs.Binding, len(a.Bindings))
	for i, b := range a.Bindings {
		b.Params = maps.Clone(b.Params)
		b.Live = nil
		out.Bindings[i] = b
	}
	return out
}
