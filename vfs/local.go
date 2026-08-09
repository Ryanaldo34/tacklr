package vfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// LocalProvider mounts a host directory as the source root of a virtual path.
//
// Root must be an absolute host path to an existing directory. File ops (later)
// must jail all relative paths under Root; Validate alone does not sandbox the agent.
type LocalProvider struct {
	// Root is the host directory that becomes this mount's source root.
	Root string
}

// Validate implements Provider. Checks are offline aside from a local Stat.
func (p LocalProvider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(p.Root) {
		return fmt.Errorf("vfs: local root must be absolute: %q", p.Root)
	}
	root := filepath.Clean(p.Root)
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("vfs: local root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("vfs: local root is not a directory: %q", root)
	}
	return nil
}
