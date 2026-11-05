package session

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// Sealer encrypts the secrets a SecretStorage writes. Seal binds the
// ciphertext to associated data (the session id), and Open fails unless it
// gets the same associated data back, so one session's secrets cannot be
// read as another's. A host can implement it over its KMS (envelope
// encryption), which is why it gets the caller's context; AESGCM is one
// built from a key.
type Sealer interface {
	Seal(ctx context.Context, plaintext, associated []byte) ([]byte, error)
	Open(ctx context.Context, ciphertext, associated []byte) ([]byte, error)
}

// AESGCM is a Sealer: AES-256-GCM with a random nonce per seal, written
// before the ciphertext.
type AESGCM struct{ aead cipher.AEAD }

// NewAESGCM returns an AESGCM Sealer for a 32-byte key.
func NewAESGCM(key []byte) (*AESGCM, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("session: AES-GCM key is %d bytes, want 32", len(key))
	}
	block, _ := aes.NewCipher(key)  // a 32-byte key is a valid AES-256 key
	aead, _ := cipher.NewGCM(block) // AES has the 16-byte block GCM needs
	return &AESGCM{aead: aead}, nil
}

// Seal implements Sealer.
func (a *AESGCM) Seal(_ context.Context, plaintext, associated []byte) ([]byte, error) {
	nonce := make([]byte, a.aead.NonceSize(), a.aead.NonceSize()+len(plaintext)+a.aead.Overhead())
	_, _ = rand.Read(nonce) // crypto/rand.Read never fails (Go 1.24+)
	return a.aead.Seal(nonce, nonce, plaintext, associated), nil
}

// Open implements Sealer.
func (a *AESGCM) Open(_ context.Context, ciphertext, associated []byte) ([]byte, error) {
	n := a.aead.NonceSize()
	if len(ciphertext) < n {
		return nil, errors.New("session: sealed secrets are too short")
	}
	return a.aead.Open(nil, ciphertext[:n], ciphertext[n:], associated)
}

var _ Sealer = (*AESGCM)(nil)
