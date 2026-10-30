package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresWireStore implements ProtocolWireStore against Postgres.
// Call Setup once per database. The table is separate from harness session
// checkpoints. Tracing
// is the pool's own (ConnConfig.Tracer).
type PostgresWireStore struct {
	pool     *pgxpool.Pool
	protocol string // stored in protocol column; default "acp"
}

// NewPostgresWireStore stores envelopes through pool.
// protocolKey labels rows (e.g. "acp"); empty defaults to "acp".
func NewPostgresWireStore(pool *pgxpool.Pool, protocolKey string) *PostgresWireStore {
	if protocolKey == "" {
		protocolKey = "acp"
	}
	return &PostgresWireStore{pool: pool, protocol: protocolKey}
}

// Setup creates public.protocol_wire_session (idempotent).
func (s *PostgresWireStore) Setup(ctx context.Context) error {
	const q = `
		CREATE TABLE IF NOT EXISTS public.protocol_wire_session (
			session_id   text PRIMARY KEY,
			protocol     text NOT NULL DEFAULT 'acp',
			payload      jsonb NOT NULL,
			updated_at   timestamptz NOT NULL DEFAULT now()
		)`
	_, err := s.pool.Exec(ctx, q)
	if err != nil {
		return fmt.Errorf("wire setup: %w", err)
	}
	return nil
}

func (s *PostgresWireStore) Put(ctx context.Context, sessionID string, payload []byte) error {
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	const q = `
		INSERT INTO public.protocol_wire_session (session_id, protocol, payload, updated_at)
		VALUES ($1, $2, $3::jsonb, now())
		ON CONFLICT (session_id) DO UPDATE
		SET protocol = EXCLUDED.protocol,
		    payload = EXCLUDED.payload,
		    updated_at = now()`
	_, err := s.pool.Exec(ctx, q, sessionID, s.protocol, payload)
	if err != nil {
		return fmt.Errorf("wire put %q: %w", sessionID, err)
	}
	return nil
}

func (s *PostgresWireStore) Get(ctx context.Context, sessionID string) ([]byte, error) {
	const q = `SELECT payload FROM public.protocol_wire_session WHERE session_id = $1`
	var payload []byte
	err := s.pool.QueryRow(ctx, q, sessionID).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("wire session %q: %w", sessionID, ErrSessionNotFound)
		}
		return nil, fmt.Errorf("wire get %q: %w", sessionID, err)
	}
	return payload, nil
}

func (s *PostgresWireStore) Delete(ctx context.Context, sessionID string) error {
	const q = `DELETE FROM public.protocol_wire_session WHERE session_id = $1`
	_, err := s.pool.Exec(ctx, q, sessionID)
	if err != nil {
		return fmt.Errorf("wire delete %q: %w", sessionID, err)
	}
	return nil
}
