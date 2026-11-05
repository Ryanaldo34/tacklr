package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/session"
)

// Secrets is session.SecretStorage on Postgres for the session-scoped
// tokens a host passes on CreateSession, Prompt and Resume (an end user's
// Drive or Graph OAuth tokens, per-session object-store credentials). Each
// session's bag is sealed before it is written, so the database never holds
// a plaintext token. Service-to-service credentials do not belong here: the
// host passes those to the providers it builds.
type Secrets struct {
	db     *DB
	sealer session.Sealer
}

// Secrets returns the session SecretStorage, sealing every bag with sealer.
func (db *DB) Secrets(sealer session.Sealer) *Secrets { return &Secrets{db: db, sealer: sealer} }

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
