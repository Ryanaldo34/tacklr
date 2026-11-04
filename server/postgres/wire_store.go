// Package postgres keeps protocol wire envelopes in Postgres: the
// server.ProtocolWireStore a protocol reads on session/load. The host owns
// the pool. The table is separate from the harness session checkpoints
// (package session/postgres), and the pool can be shared with them.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/server"
)

// WireStore implements server.ProtocolWireStore against Postgres.
// Call Setup once per database. Tracing is the pool's own
// (ConnConfig.Tracer).
type WireStore struct {
	pool     *pgxpool.Pool
	protocol string // stored in protocol column; default "acp"
}

var _ server.ProtocolWireStore = (*WireStore)(nil)

// NewWireStore stores envelopes through pool.
// protocolKey labels rows (e.g. "acp"); empty defaults to "acp".
func NewWireStore(pool *pgxpool.Pool, protocolKey string) *WireStore {
	if protocolKey == "" {
		protocolKey = "acp"
	}
	return &WireStore{pool: pool, protocol: protocolKey}
}

// Setup creates public.protocol_wire_session (idempotent).
func (s *WireStore) Setup(ctx context.Context) error {
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

func (s *WireStore) Put(ctx context.Context, sessionID string, payload []byte) error {
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

func (s *WireStore) Get(ctx context.Context, sessionID string) ([]byte, error) {
	const q = `SELECT payload FROM public.protocol_wire_session WHERE session_id = $1`
	var payload []byte
	err := s.pool.QueryRow(ctx, q, sessionID).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("wire session %q: %w", sessionID, server.ErrSessionNotFound)
		}
		return nil, fmt.Errorf("wire get %q: %w", sessionID, err)
	}
	return payload, nil
}

func (s *WireStore) Delete(ctx context.Context, sessionID string) error {
	const q = `DELETE FROM public.protocol_wire_session WHERE session_id = $1`
	_, err := s.pool.Exec(ctx, q, sessionID)
	if err != nil {
		return fmt.Errorf("wire delete %q: %w", sessionID, err)
	}
	return nil
}
