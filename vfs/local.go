package vfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LocalProvider mounts a host directory as the source root of a virtual path.
type LocalProvider struct {
	Root string
}

// Validate implements Provider.
func (p LocalProvider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(p.Root) {
		return fmt.Errorf("vfs: local root must be absolute")
	}
	root := filepath.Clean(p.Root)
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("vfs: local root %q: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("vfs: local root is not a directory")
	}
	return nil
}

// LocalFactory opens LocalProviders under a fixed Base directory.
// Params: "subpath" (optional, jailed), "session_scoped=true" (optional).
type LocalFactory struct {
	ID   string
	Base string
}

// Profile implements ProviderFactory.
func (f LocalFactory) Profile() string { return f.ID }

// Open implements ProviderFactory.
func (f LocalFactory) Open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.ID == "" || !filepath.IsAbs(f.Base) {
		return nil, fmt.Errorf("vfs: local factory needs id and absolute base")
	}
	base := filepath.Clean(f.Base)
	root := base
	if sub := spec.Params["subpath"]; sub != "" {
		jailed, err := jailSubpath(base, sub)
		if err != nil {
			return nil, err
		}
		root = jailed
	}
	if spec.Params["session_scoped"] == "true" && sessionID != "" {
		if strings.ContainsAny(sessionID, `/\`) || sessionID == ".." || sessionID == "." {
			return nil, fmt.Errorf("vfs: unsafe session id for path")
		}
		root = filepath.Join(root, sessionID)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("vfs: mkdir %q: %w", root, err)
	}
	return LocalProvider{Root: root}, nil
}

func jailSubpath(base, sub string) (string, error) {
	if filepath.IsAbs(sub) {
		return "", fmt.Errorf("vfs: subpath must be relative")
	}
	cleaned := filepath.Clean(sub)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("vfs: subpath escapes base")
	}
	return filepath.Join(base, cleaned), nil
}
