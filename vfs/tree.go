package vfs

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// Request is this turn's user-owned bind list (tokens + params). Host-owned
// At members ignore it. User-owned backends (Drive, Graph) match Binding
// params["name"] to the At name.
type Request struct {
	Bindings []mount.Binding
}

// OpenVFS builds a turn-scoped MountSession. Nil OpenVFS means no VFS.
type OpenVFS func(ctx context.Context, sessionID string, req Request) (*MountSession, error)

// WithCodecs opens the tree with codecs added to the text codec and the
// codecs of the mounted backends' own types, for example vfs/adapters' Word
// and Excel:
//
//	vfs.Tree(members...).WithCodecs(adapters.Codecs()...)
//
// Only the codecs a host names are linked into its binary.
func (o OpenVFS) WithCodecs(codecs ...Codec) OpenVFS {
	return func(ctx context.Context, sessionID string, req Request) (*MountSession, error) {
		ms, err := o(ctx, sessionID, req)
		if err != nil {
			return nil, err
		}
		if ms.codecs, err = ms.codecs.with(codecs...); err != nil {
			return nil, err
		}
		return ms, nil
	}
}

// Open opens a Provider. Close over clients in the function. Binding carries
// this turn's user token and params (folderId, subpath, …). A nil Provider
// and nil error means skip (no token for a user-scoped backend).
type Open func(ctx context.Context, sessionID string, b mount.Binding) (Provider, error)

// Member is one /workspace/<name> backend.
type Member struct {
	name       string
	open       Open
	notIndexed bool
	readOnly   bool
}

// At names a backend under /workspace. name is the first path segment
// (/workspace/scratch/…).
func At(name string, open Open) Member {
	return Member{name: strings.TrimSpace(name), open: open}
}

// ReadOnly marks a host-owned member read-only. User-owned members use
// Binding.Writable instead.
func (m Member) ReadOnly() Member {
	m.readOnly = true
	return m
}

// NotIndexed makes index_file refuse this member's files.
func (m Member) NotIndexed() Member {
	m.notIndexed = true
	return m
}

// Tree mounts every member under /workspace — the only top-level mount.
// Hosts omit user-cloud members they did not construct from this turn's bind.
func Tree(members ...Member) OpenVFS {
	return func(ctx context.Context, sessionID string, req Request) (*MountSession, error) {
		ms, err := NewMountSession(sessionID)
		if err != nil {
			return nil, err
		}
		seen := make(map[string]struct{}, len(members))
		for _, m := range members {
			if m.name == "" || m.open == nil {
				return nil, fmt.Errorf("vfs: At name and open are required")
			}
			if err := validMemberName(m.name); err != nil {
				return nil, err
			}
			if _, ok := seen[m.name]; ok {
				return nil, ErrAmbiguous
			}
			seen[m.name] = struct{}{}
			b := bindNamed(req.Bindings, m.name)
			if strings.TrimSpace(b.Point) == "" {
				b.Point = mount.WorkspacePoint + "/" + m.name
			}
			p, err := m.open(ctx, sessionID, b)
			if err != nil {
				return nil, err
			}
			if p == nil {
				continue
			}
			if cs, ok := p.(codecSource); ok {
				ms.codecs = ms.codecs.adding(cs.codecs())
			}
			writable := memberWritable(m, b)
			params := maps.Clone(b.Params)
			if params == nil {
				params = map[string]string{}
			}
			params[mount.ParamName] = m.name
			spec := MountSpec{
				Point:      mount.WorkspacePoint + "/" + m.name,
				Params:     params,
				ReadOnly:   !writable,
				Profile:    m.name,
				NotIndexed: m.notIndexed,
			}
			if err := ms.mount(ctx, spec, p); err != nil {
				return nil, err
			}
		}
		return ms, nil
	}
}

func memberWritable(m Member, b mount.Binding) bool {
	if strings.TrimSpace(b.Auth.Token) != "" {
		return b.Writable
	}
	return !m.readOnly
}

// BindingByName returns the bind whose alias or provider matches name.
func BindingByName(binds []mount.Binding, name string) (mount.Binding, bool) {
	for _, b := range binds {
		if b.Alias() == name || strings.TrimSpace(b.Provider) == name {
			return b, true
		}
	}
	return mount.Binding{}, false
}

func bindNamed(binds []mount.Binding, name string) mount.Binding {
	if b, ok := BindingByName(binds, name); ok {
		return b
	}
	return mount.Binding{Params: map[string]string{mount.ParamName: name}}
}
