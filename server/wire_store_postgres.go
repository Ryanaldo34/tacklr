package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
)

// PostgresWireStore implements ProtocolWireStore against Postgres.
// Call Setup once per database. Shares a *pgx.Conn with a brain store when
// desired; the table is separate from harness session checkpoints.
type PostgresWireStore struct {
	conn     *pgx.Conn
	protocol string // stored in protocol column; default "acp"
	tr       *otelpgx.Tracer
}

// NewPostgresWireStore wraps an existing pgx connection.
// protocolKey labels rows (e.g. "acp"); empty defaults to "acp".
func NewPostgresWireStore(conn *pgx.Conn, protocolKey string) *PostgresWireStore {
	if protocolKey == "" {
		protocolKey = "acp"
	}
	return &PostgresWireStore{
		conn:     conn,
		protocol: protocolKey,
		tr: otelpgx.NewTracer(
			otelpgx.WithTrimSQLInSpanName(),
			otelpgx.WithDisableAcquireTracer(),
		),
	}
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
	ctx = s.tr.TraceQueryStart(ctx, s.conn, pgx.TraceQueryStartData{SQL: q})
	_, err := s.conn.Exec(ctx, q)
	s.tr.TraceQueryEnd(ctx, s.conn, pgx.TraceQueryEndData{Err: err})
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
	ctx = s.tr.TraceQueryStart(ctx, s.conn, pgx.TraceQueryStartData{SQL: q, Args: []any{sessionID, s.protocol, payload}})
	_, err := s.conn.Exec(ctx, q, sessionID, s.protocol, payload)
	s.tr.TraceQueryEnd(ctx, s.conn, pgx.TraceQueryEndData{Err: err})
	if err != nil {
		return fmt.Errorf("wire put %q: %w", sessionID, err)
	}
	return nil
}

func (s *PostgresWireStore) Get(ctx context.Context, sessionID string) ([]byte, error) {
	const q = `SELECT payload FROM public.protocol_wire_session WHERE session_id = $1`
	var payload []byte
	ctx = s.tr.TraceQueryStart(ctx, s.conn, pgx.TraceQueryStartData{SQL: q, Args: []any{sessionID}})
	err := s.conn.QueryRow(ctx, q, sessionID).Scan(&payload)
	s.tr.TraceQueryEnd(ctx, s.conn, pgx.TraceQueryEndData{Err: err})
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
	ctx = s.tr.TraceQueryStart(ctx, s.conn, pgx.TraceQueryStartData{SQL: q, Args: []any{sessionID}})
	_, err := s.conn.Exec(ctx, q, sessionID)
	s.tr.TraceQueryEnd(ctx, s.conn, pgx.TraceQueryEndData{Err: err})
	if err != nil {
		return fmt.Errorf("wire delete %q: %w", sessionID, err)
	}
	return nil
}
