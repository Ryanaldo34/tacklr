package vfs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
)

var (
	_ Provider        = workspaceProvider{}
	_ documentBackend = workspaceProvider{}
	_ filePutter      = workspaceProvider{}
)

// workspaceProvider is a named writable union. First path segment is the
// member alias; it is not a flat merge of child names.
type workspaceProvider struct {
	members []workspaceMember
}

type workspaceMember struct {
	name     string
	writable bool
	inner    Provider
}

func (r *BackendRegistry) openWorkspace(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spec.Profile == "" {
		return nil, ErrInvalidProvider
	}
	seen := make(map[string]struct{}, len(spec.Members))
	for i, member := range spec.Members {
		if len(member.Members) > 0 {
			return nil, fmt.Errorf("vfs: nested union mounts are not supported")
		}
		if strings.TrimSpace(member.Point) != "" {
			return nil, fmt.Errorf("vfs: union member point must be empty")
		}
		if member.Profile == "" {
			return nil, fmt.Errorf("vfs: union member[%d] profile required", i)
		}
		name := strings.TrimSpace(member.Params[ParamName])
		if name == "" {
			return nil, fmt.Errorf("vfs: workspace member[%d] name required", i)
		}
		if err := validAlias(name); err != nil {
			return nil, err
		}
		if _, ok := seen[name]; ok {
			return nil, ErrAmbiguous
		}
		seen[name] = struct{}{}
	}
	members := make([]workspaceMember, 0, len(spec.Members))
	for _, member := range spec.Members {
		p, err := r.open(ctx, sessionID, member)
		if err != nil {
			return nil, err
		}
		members = append(members, workspaceMember{
			name:     strings.TrimSpace(member.Params[ParamName]),
			writable: !member.ReadOnly,
			inner:    p,
		})
	}
	return workspaceProvider{members: members}, nil
}

func (p workspaceProvider) Validate(ctx context.Context) error {
	for _, m := range p.members {
		if err := m.inner.Validate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p workspaceProvider) Stat(ctx context.Context, name string) (FileInfo, error) {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return FileInfo{}, err
	}
	if alias == "" {
		return FileInfo{Name: ".", Mode: fs.ModeDir | 0o755, IsDir: true}, nil
	}
	m, err := p.lookup(alias)
	if err != nil {
		return FileInfo{}, err
	}
	if rest == "" {
		st, err := m.inner.Stat(ctx, "")
		if err != nil {
			return FileInfo{}, err
		}
		st.Name = alias
		st.IsDir = true
		st.Mode = fs.ModeDir | 0o755
		st.MediaType = ""
		return st, nil
	}
	return m.inner.Stat(ctx, rest)
}

func (p workspaceProvider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return nil, err
	}
	if alias == "" {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}
	m, err := p.lookup(alias)
	if err != nil {
		return nil, err
	}
	if openWrites(flag) {
		if !m.writable {
			return nil, ErrReadOnly
		}
		if rest == "" {
			return nil, ErrNotSupported
		}
	}
	return m.inner.OpenFile(ctx, rest, flag, perm)
}

func (p workspaceProvider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return nil, err
	}
	if alias == "" {
		out := make([]DirEntry, 0, len(p.members))
		for _, m := range p.members {
			out = append(out, DirEntry{Name: m.name, IsDir: true, Type: fs.ModeDir})
		}
		slices.SortFunc(out, func(a, b DirEntry) int { return cmp.Compare(a.Name, b.Name) })
		return out, nil
	}
	m, err := p.lookup(alias)
	if err != nil {
		return nil, err
	}
	return m.inner.ReadDir(ctx, rest)
}

func (p workspaceProvider) Remove(ctx context.Context, name string) error {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return err
	}
	if alias == "" {
		return ErrNotSupported
	}
	m, err := p.lookup(alias)
	if err != nil {
		return err
	}
	if rest == "" {
		return ErrInvalidPath
	}
	if !m.writable {
		return ErrReadOnly
	}
	return m.inner.Remove(ctx, rest)
}

func (p workspaceProvider) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return err
	}
	if alias == "" {
		return nil
	}
	m, err := p.lookup(alias)
	if err != nil {
		if errors.Is(err, ErrNotExist) {
			return ErrNotSupported
		}
		return err
	}
	if !m.writable {
		return ErrReadOnly
	}
	return m.inner.MkdirAll(ctx, rest, perm)
}

func (p workspaceProvider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return nil, err
	}
	if alias == "" {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}
	m, err := p.lookup(alias)
	if err != nil {
		return nil, err
	}
	db, ok := m.inner.(documentBackend)
	if !ok {
		return nil, ErrNotSupported
	}
	return db.OpenDocument(ctx, rest, reg)
}

func (p workspaceProvider) WriteDocument(ctx context.Context, name string, doc Document) error {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return err
	}
	if alias == "" {
		return ErrNotSupported
	}
	m, err := p.lookup(alias)
	if err != nil {
		return err
	}
	if rest == "" {
		return ErrNotSupported
	}
	if !m.writable {
		return ErrReadOnly
	}
	db, ok := m.inner.(documentBackend)
	if !ok {
		return ErrNotSupported
	}
	return db.WriteDocument(ctx, rest, doc)
}

func (p workspaceProvider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	alias, rest, err := splitAlias(name)
	if err != nil {
		return err
	}
	if alias == "" || rest == "" {
		return ErrNotSupported
	}
	m, err := p.lookup(alias)
	if err != nil {
		return err
	}
	if !m.writable {
		return ErrReadOnly
	}
	putter, ok := m.inner.(filePutter)
	if !ok {
		return ErrNotSupported
	}
	return putter.PutFile(ctx, rest, r, size)
}

func (p workspaceProvider) lookup(alias string) (workspaceMember, error) {
	i := slices.IndexFunc(p.members, func(m workspaceMember) bool { return m.name == alias })
	if i < 0 {
		return workspaceMember{}, ErrNotExist
	}
	return p.members[i], nil
}

func splitAlias(name string) (alias, rest string, err error) {
	rel, err := cleanRel(name)
	if err != nil {
		return "", "", err
	}
	if rel == "" {
		return "", "", nil
	}
	alias, rest, _ = strings.Cut(rel, "/")
	return alias, rest, nil
}

func isWorkspaceSpec(spec MountSpec) bool {
	return len(spec.Members) > 0 && (spec.Point == WorkspacePoint || spec.Profile == workspaceProfile)
}
