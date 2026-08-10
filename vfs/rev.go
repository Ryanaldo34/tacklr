package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// ContentRev identifies session-visible file content for optimistic concurrency.
// Hash is hex SHA-256 of the UTF-8 body (same policy as vfsindex content_hash).
// vfs does not enforce edits; harness tools compare expected Hash before write.
type ContentRev struct {
	Path string
	Hash string
}

// ContentHash returns hex SHA-256 of body.
func ContentHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// ContentRev hashes the session-visible body (dirty IR cache, else ReadFile).
func (m *MountSession) ContentRev(ctx context.Context, virtualPath string) (ContentRev, error) {
	if doc, _, _, _, ok := m.cache.get(virtualPath); ok {
		return ContentRev{Path: virtualPath, Hash: ContentHash(doc.Text())}, nil
	}
	raw, err := m.ReadFile(ctx, virtualPath)
	if err != nil {
		return ContentRev{}, err
	}
	return ContentRev{Path: virtualPath, Hash: ContentHash(string(raw))}, nil
}
