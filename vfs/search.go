package vfs

import (
	"context"
	"fmt"
)

// SearchText returns session-visible plaintext for one file (dirty IR before Sync).
// FUSE / host rg should read this, not native .docx/PDF bytes.
//
// Textual documents only (comma-ok). Directories, binaries, and types with no
// plaintext return an error (ErrNotTextual or ErrNoCodec).
func (m *MountSession) SearchText(ctx context.Context, virtualPath string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return "", err
	}
	if td, ok := m.cachedText(ctx, cleaned); ok {
		return td.Text(), nil
	}
	st, err := m.Stat(ctx, cleaned)
	if err != nil {
		return "", err
	}
	if st.IsDir {
		return "", fmt.Errorf("vfs: %s is a directory", cleaned)
	}
	doc, err := m.OpenDocument(ctx, cleaned, nil)
	if err != nil {
		return "", err
	}
	t, ok := doc.(Textual)
	if !ok {
		return "", ErrNotTextual
	}
	return t.Text(), nil
}
