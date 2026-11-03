package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/session"
)

// Sealer encrypts what Secrets writes. Seal binds the ciphertext to
// associated data (the session id), and Open fails unless it gets the same
// associated data back, so one session's row cannot be read as another's.
// A host can implement it over its KMS (envelope encryption), which is why
// it gets the caller's context; AESGCM is one built from a key.
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
		return nil, fmt.Errorf("postgres: AES-GCM key is %d bytes, want 32", len(key))
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
		return nil, errors.New("postgres: sealed secrets are too short")
	}
	return a.aead.Open(nil, ciphertext[:n], ciphertext[n:], associated)
}

// Secrets is session.SecretStorage on Postgres for the session-scoped
// tokens a host passes on CreateSession, Prompt and Resume (an end user's
// Drive or Graph OAuth tokens, per-session object-store credentials). Each
// session's bag is sealed before it is written, so the database never holds
// a plaintext token. Service-to-service credentials do not belong here: the
// host passes those to the providers it builds.
type Secrets struct {
	db     *DB
	sealer Sealer
}

// Secrets returns the session SecretStorage, sealing every bag with sealer.
func (db *DB) Secrets(sealer Sealer) *Secrets { return &Secrets{db: db, sealer: sealer} }

// Put implements session.SecretStorage: a bag with bindings replaces the
// session's; one without (drop-only) changes nothing.
func (s *Secrets) Put(ctx context.Context, sessionID session.ID, secrets session.Secrets) error {
	if sessionID == "" {
		return fmt.Errorf("secrets: session id is required")
	}
	if len(secrets.Auth.Bindings) == 0 {
		return nil
	}
	plain, _ := json.Marshal(secrets) // strings, times and maps of strings only
	sealed, err := s.sealer.Seal(ctx, plain, []byte(sessionID))
	if err != nil {
		return fmt.Errorf("secrets %q: seal: %w", sessionID, err)
	}
	_, err = s.db.pool.Exec(ctx, s.db.sql(`
		INSERT INTO {s}.session_secrets (session_id, sealed) VALUES ($1, $2)
		ON CONFLICT (session_id) DO UPDATE SET sealed = excluded.sealed, updated_at = now()`), sessionID, sealed)
	if err != nil {
		return fmt.Errorf("secrets %q: %w", sessionID, err)
	}
	return nil
}

// Get implements session.SecretStorage.
func (s *Secrets) Get(ctx context.Context, sessionID session.ID) (session.Secrets, error) {
	var sealed []byte
	err := s.db.pool.QueryRow(ctx, s.db.sql(`SELECT sealed FROM {s}.session_secrets WHERE session_id = $1`), sessionID).Scan(&sealed)
	if errors.Is(err, pgx.ErrNoRows) {
		return session.Secrets{}, nil
	}
	if err != nil {
		return session.Secrets{}, fmt.Errorf("secrets %q: %w", sessionID, err)
	}
	plain, err := s.sealer.Open(ctx, sealed, []byte(sessionID))
	if err != nil {
		return session.Secrets{}, fmt.Errorf("secrets %q: open: %w", sessionID, err)
	}
	var out session.Secrets
	if err := json.Unmarshal(plain, &out); err != nil {
		return session.Secrets{}, fmt.Errorf("secrets %q: %w", sessionID, err)
	}
	return out, nil
}

// Delete implements session.SecretStorage.
func (s *Secrets) Delete(ctx context.Context, sessionID session.ID) error {
	if _, err := s.db.pool.Exec(ctx, s.db.sql(`DELETE FROM {s}.session_secrets WHERE session_id = $1`), sessionID); err != nil {
		return fmt.Errorf("secrets %q: %w", sessionID, err)
	}
	return nil
}

var _ session.SecretStorage = (*Secrets)(nil)
