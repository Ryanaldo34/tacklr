package vfs

import (
	"context"
	"fmt"
	"maps"
	"strings"
)

// Request is this turn's user-owned bind list (tokens + params). Host-owned
// At members ignore it. User-owned backends (Drive, Graph) match Binding
// params["name"] to the At name.
type Request struct {
	Bindings []Binding
}

// OpenVFS builds a turn-scoped MountSession. Nil OpenVFS means no VFS.
type OpenVFS func(ctx context.Context, sessionID string, req Request) (*MountSession, error)

// Open opens a Provider. Close over clients in the function. Binding carries
// this turn's user token and params (folderId, subpath, …). A nil Provider
// and nil error means skip (no token for a user-scoped backend).
type Open func(ctx context.Context, sessionID string, b Binding) (Provider, error)

// Member is one /workspace/<name> backend.
type Member struct {
	name        string
	open        Open
	indexPolicy string
	readOnly    bool
	profile     string
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

// Indexed sets MountSpec.IndexPolicy on this member (none|selective|prefix|watch).
func (m Member) Indexed(policy string) Member {
	m.indexPolicy = strings.TrimSpace(policy)
	return m
}

// Profile sets MountSpec.Profile. Empty keeps the At name. Tree still
// sets Profile "brain" when name is "engram".
func (m Member) Profile(profile string) Member {
	m.profile = strings.TrimSpace(profile)
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
		ws := workspaceProvider{}
		specs := make([]MountSpec, 0, len(members))
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
				b.Point = WorkspacePoint + "/" + m.name
			}
			p, err := m.open(ctx, sessionID, b)
			if err != nil {
				return nil, err
			}
			if p == nil {
				continue
			}
			writable := memberWritable(m, b)
			ws.members = append(ws.members, workspaceMember{
				name: m.name, writable: writable, inner: p,
			})
			params := maps.Clone(b.Params)
			if params == nil {
				params = map[string]string{}
			}
			params[ParamName] = m.name
			profile := m.name
			if m.profile != "" {
				profile = m.profile
			}
			if m.name == "engram" {
				profile = "brain"
			}
			spec := MountSpec{
				Params:      params,
				ReadOnly:    !writable,
				Profile:     profile,
				IndexPolicy: memberIndexPolicy(m),
			}
			specs = append(specs, spec)
		}
		root := MountSpec{
			Point:   WorkspacePoint,
			Profile: workspaceProfile,
			Members: specs,
		}
		if err := ms.Attach(ctx, root, ws); err != nil {
			return nil, err
		}
		return ms, nil
	}
}

func memberWritable(m Member, b Binding) bool {
	if strings.TrimSpace(b.Auth.Token) != "" {
		return b.Writable
	}
	return !m.readOnly
}

func memberIndexPolicy(m Member) string {
	if m.indexPolicy != "" {
		return m.indexPolicy
	}
	switch m.name {
	case "engram", "skills":
		return "none"
	case "memory":
		return "watch"
	default:
		return ""
	}
}

// BindingByName returns the bind whose alias or provider matches name.
func BindingByName(binds []Binding, name string) (Binding, bool) {
	for _, b := range binds {
		if bindingAlias(b) == name || strings.TrimSpace(b.Provider) == name {
			return b, true
		}
	}
	return Binding{}, false
}

func bindNamed(binds []Binding, name string) Binding {
	if b, ok := BindingByName(binds, name); ok {
		return b
	}
	return Binding{Params: map[string]string{ParamName: name}}
}
