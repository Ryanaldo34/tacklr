package server

import (
	"sync"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// CredentialBag is the VFS credential state for one wire session.
// A protocol parses its own payload into vfs.Binding values, then calls
// Bind, Refresh, and Unbind. Take copies that state onto AuthContext for
// the next Prompt or Resume and clears the drop list.
type CredentialBag struct {
	mu       sync.Mutex
	bindings []mount.Binding
	drop     []string
}

// Bind validates b and stores it. A later bind with the same provider and
// alias replaces the previous one.
func (b *CredentialBag) Bind(binding mount.Binding) error {
	if err := vfs.ValidateBinding(binding); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	alias := binding.Alias()
	for i, existing := range b.bindings {
		if existing.Alias() == alias && existing.Provider == binding.Provider {
			b.bindings[i] = binding
			return nil
		}
	}
	b.bindings = append(b.bindings, binding)
	return nil
}

// Refresh replaces the token on every binding for provider.
// It reports whether any binding matched.
func (b *CredentialBag) Refresh(provider string, cred mount.Credential) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	found := false
	for i, existing := range b.bindings {
		if existing.Provider != provider {
			continue
		}
		b.bindings[i].Auth = cred
		found = true
	}
	return found
}

// Unbind removes bindings whose alias or provider matches and records the
// alias on the next Take drop list.
func (b *CredentialBag) Unbind(point, provider string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.bindings[:0]
	for _, existing := range b.bindings {
		alias := existing.Alias()
		drop := false
		if point != "" && (alias == point || existing.Point == point) {
			drop = true
		}
		if provider != "" && existing.Provider == provider {
			drop = true
		}
		if drop {
			b.drop = append(b.drop, alias)
			continue
		}
		kept = append(kept, existing)
	}
	b.bindings = kept
}

// Take returns the bindings for the next work item and clears drops.
func (b *CredentialBag) Take() session.AuthContext {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := session.AuthContext{
		Bindings: append([]mount.Binding(nil), b.bindings...),
		Drop:     append([]string(nil), b.drop...),
	}
	b.drop = nil
	return out
}
