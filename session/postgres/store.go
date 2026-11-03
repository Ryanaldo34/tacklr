package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/session"
)

// Store is session.Store on Postgres: one row per session checkpoint, its
// revision a counter that a Save compares and bumps in one statement.
type Store struct{ db *DB }

// Store returns the session Store.
func (db *DB) Store() *Store { return &Store{db: db} }

// Save implements session.Store. It writes only when the row is at
// expected ("" when there is none yet), so of two saves from one revision
// exactly one wins and the other gets session.ErrStaleCheckpoint.
func (s *Store) Save(ctx context.Context, sessionID session.ID, cp []byte, expected session.Revision) (session.Revision, error) {
	if sessionID == "" {
		return "", fmt.Errorf("snapshot: session id is required")
	}
	query, args := s.db.sql(`
		INSERT INTO {s}.session_checkpoints (session_id, revision, checkpoint) VALUES ($1, 1, $2)
		ON CONFLICT (session_id) DO NOTHING
		RETURNING revision`), []any{sessionID, cp}
	if expected != "" {
		// A revision this store did not issue matches no row.
		prev, _ := strconv.ParseInt(string(expected), 10, 64)
		query, args = s.db.sql(`
			UPDATE {s}.session_checkpoints SET revision = revision + 1, checkpoint = $2, updated_at = now()
			WHERE session_id = $1 AND revision = $3
			RETURNING revision`), []any{sessionID, cp, prev}
	}
	var rev int64
	if err := s.db.pool.QueryRow(ctx, query, args...).Scan(&rev); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", session.ErrStaleCheckpoint
		}
		return "", fmt.Errorf("save snapshot %q: %w", sessionID, err)
	}
	return revision(rev), nil
}

// Load implements session.Store.
func (s *Store) Load(ctx context.Context, sessionID session.ID) ([]byte, session.Revision, error) {
	var cp []byte
	var rev int64
	err := s.db.pool.QueryRow(ctx, s.db.sql(`SELECT checkpoint, revision FROM {s}.session_checkpoints WHERE session_id = $1`), sessionID).Scan(&cp, &rev)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", fmt.Errorf("load snapshot %q: %w", sessionID, session.ErrNotFound)
	}
	if err != nil {
		return nil, "", fmt.Errorf("load snapshot %q: %w", sessionID, err)
	}
	return cp, revision(rev), nil
}

// Revision implements session.Store.
func (s *Store) Revision(ctx context.Context, sessionID session.ID) (session.Revision, error) {
	var rev int64
	err := s.db.pool.QueryRow(ctx, s.db.sql(`SELECT revision FROM {s}.session_checkpoints WHERE session_id = $1`), sessionID).Scan(&rev)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("snapshot revision %q: %w", sessionID, err)
	}
	return revision(rev), nil
}

// PutOutcome implements session.Store: a Put of a key replaces it.
func (s *Store) PutOutcome(ctx context.Context, sessionID session.ID, callKey string, o session.Outcome) error {
	_, err := s.db.pool.Exec(ctx, s.db.sql(`
		INSERT INTO {s}.session_outcomes (session_id, call_key, result, record) VALUES ($1, $2, $3, $4)
		ON CONFLICT (session_id, call_key) DO UPDATE SET result = excluded.result, record = excluded.record`),
		sessionID, callKey, o.Result, o.Record)
	if err != nil {
		return fmt.Errorf("put outcome %q %q: %w", sessionID, callKey, err)
	}
	return nil
}

// Outcomes implements session.Store in one query for every key.
func (s *Store) Outcomes(ctx context.Context, sessionID session.ID, callKeys []string) (map[string]session.Outcome, error) {
	out := make(map[string]session.Outcome, len(callKeys))
	if len(callKeys) == 0 {
		return out, nil
	}
	// A failed query is Rows in an error state, which ForEachRow reports.
	rows, _ := s.db.pool.Query(ctx, s.db.sql(`
		SELECT call_key, result, record FROM {s}.session_outcomes WHERE session_id = $1 AND call_key = ANY($2)`),
		sessionID, callKeys)
	var key string
	var o session.Outcome
	_, err := pgx.ForEachRow(rows, []any{&key, &o.Result, &o.Record}, func() error {
		out[key] = o
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("outcomes %q: %w", sessionID, err)
	}
	return out, nil
}

// Delete implements session.Store: the checkpoint and every outcome go in
// one statement.
func (s *Store) Delete(ctx context.Context, sessionID session.ID) error {
	_, err := s.db.pool.Exec(ctx, s.db.sql(`
		WITH cp AS (DELETE FROM {s}.session_checkpoints WHERE session_id = $1)
		DELETE FROM {s}.session_outcomes WHERE session_id = $1`), sessionID)
	if err != nil {
		return fmt.Errorf("delete snapshot %q: %w", sessionID, err)
	}
	return nil
}

func revision(n int64) session.Revision { return session.Revision(strconv.FormatInt(n, 10)) }

var _ session.Store = (*Store)(nil)
