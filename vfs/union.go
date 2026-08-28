package vfs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
)

// unionProvider is a read-only merge of member providers at one alias.
// First-level names must be unique across members.
type unionProvider struct {
	members []Provider
}

// Union merges backends into one read-only Open (skill packs under
// At("skills", Union(...))).
func Union(opens ...Open) Open {
	return func(ctx context.Context, sessionID string, b Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(opens) == 0 {
			return nil, fmt.Errorf("vfs: Union requires members")
		}
		members := make([]Provider, 0, len(opens))
		for _, open := range opens {
			if open == nil {
				return nil, fmt.Errorf("vfs: Union member open is required")
			}
			p, err := open(ctx, sessionID, b)
			if err != nil {
				return nil, err
			}
			if p == nil {
				continue
			}
			members = append(members, p)
		}
		return unionProvider{members: members}, nil
	}
}

func (p unionProvider) Validate(ctx context.Context) error {
	for _, m := range p.members {
		if err := m.Validate(ctx); err != nil {
			return err
		}
	}
	_, err := p.mergeRoot(ctx)
	return err
}

func (p unionProvider) Stat(ctx context.Context, name string) (FileInfo, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return FileInfo{}, err
	}
	if rel == "" {
		return FileInfo{Name: ".", Mode: fs.ModeDir | 0o755, IsDir: true}, nil
	}
	m, err := p.owner(ctx, rel)
	if err != nil {
		return FileInfo{}, err
	}
	return m.Stat(ctx, rel)
}

func (p unionProvider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	if openWrites(flag) {
		return nil, ErrReadOnly
	}
	rel, err := cleanRel(name)
	if err != nil {
		return nil, err
	}
	if rel == "" {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}
	m, err := p.owner(ctx, rel)
	if err != nil {
		return nil, err
	}
	return m.OpenFile(ctx, rel, flag, perm)
}

func (p unionProvider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return nil, err
	}
	if rel == "" {
		return p.mergeRoot(ctx)
	}
	m, err := p.owner(ctx, rel)
	if err != nil {
		return nil, err
	}
	return m.ReadDir(ctx, rel)
}

func (p unionProvider) Remove(context.Context, string) error {
	return ErrReadOnly
}

func (p unionProvider) MkdirAll(context.Context, string, fs.FileMode) error {
	return ErrReadOnly
}

func (p unionProvider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return nil, err
	}
	if rel == "" {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}
	m, err := p.owner(ctx, rel)
	if err != nil {
		return nil, err
	}
	db, ok := m.(documentBackend)
	if !ok {
		return nil, ErrNotSupported
	}
	return db.OpenDocument(ctx, rel, reg)
}

func (p unionProvider) WriteDocument(context.Context, string, Document) error {
	return ErrReadOnly
}

func (p unionProvider) mergeRoot(ctx context.Context) ([]DirEntry, error) {
	seen := make(map[string]DirEntry)
	for _, m := range p.members {
		ents, err := m.ReadDir(ctx, "")
		if err != nil {
			return nil, err
		}
		for _, e := range ents {
			if _, ok := seen[e.Name]; ok {
				return nil, ErrAmbiguous
			}
			seen[e.Name] = e
		}
	}
	out := make([]DirEntry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b DirEntry) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

func (p unionProvider) owner(ctx context.Context, rel string) (Provider, error) {
	first, _, _ := strings.Cut(rel, "/")
	var hit Provider
	for _, m := range p.members {
		_, err := m.Stat(ctx, first)
		if err == nil {
			if hit != nil {
				return nil, ErrAmbiguous
			}
			hit = m
			continue
		}
		if !errors.Is(err, ErrNotExist) {
			return nil, err
		}
	}
	if hit == nil {
		return nil, ErrNotExist
	}
	return hit, nil
}

func openWrites(flag int) bool {
	return flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0
}
