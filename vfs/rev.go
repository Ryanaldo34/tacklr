package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"unsafe"
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
	return hashSHA256(unsafeStringBytes(body))
}

func unsafeStringBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func hashSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ContentToken is the single rev helper. Used by ContentRev, lineWindowFromDoc,
// readStructured, loadMatching, and stage. RichDocument uses the IR fingerprint
// so HTML reproject does not change the token.
func ContentToken(t Textual) string {
	if rd, ok := t.(*RichDocument); ok {
		return rd.ContentFingerprint()
	}
	return ContentHash(t.Text())
}

// ContentRev hashes the session-visible body: ReadText when textual (same
// bytes FUSE and the read tool show), otherwise ReadFile.
func (m *MountSession) ContentRev(ctx context.Context, virtualPath string) (ContentRev, error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return ContentRev{}, err
	}
	t, err := m.ReadText(ctx, cleaned)
	if err == nil {
		return ContentRev{Path: cleaned, Hash: ContentToken(t)}, nil
	}
	if !errors.Is(err, ErrNoCodec) && !errors.Is(err, ErrNotTextual) && !errors.Is(err, ErrNotSupported) {
		return ContentRev{}, err
	}
	raw, err := m.ReadFile(ctx, cleaned)
	if err != nil {
		return ContentRev{}, err
	}
	return ContentRev{Path: cleaned, Hash: hashSHA256(raw)}, nil
}
