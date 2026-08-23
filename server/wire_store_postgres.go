package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// PostgresWireStore implements ProtocolWireStore against Postgres.
// Table: public.protocol_wire_session (see stores/testdata/session_schema.sql).
// Shares a *pgx.Conn with stores.PostgresStore when desired; schema is separate
// from harness session checkpoints.
type PostgresWireStore struct {
	conn     *pgx.Conn
	protocol string // stored in protocol column; default "acp"
}

// NewPostgresWireStore wraps an existing pgx connection.
// protocolKey labels rows (e.g. "acp"); empty defaults to "acp".
func NewPostgresWireStore(conn *pgx.Conn, protocolKey string) *PostgresWireStore {
	if conn == nil {
		panic("server: postgres wire store requires conn")
	}
	if protocolKey == "" {
		protocolKey = "acp"
	}
	return &PostgresWireStore{conn: conn, protocol: protocolKey}
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
	_, err := s.conn.Exec(ctx, q, sessionID, s.protocol, payload)
	if err != nil {
		return fmt.Errorf("wire put %q: %w", sessionID, err)
	}
	return nil
}

func (s *PostgresWireStore) Get(ctx context.Context, sessionID string) ([]byte, error) {
	const q = `SELECT payload FROM public.protocol_wire_session WHERE session_id = $1`
	var payload []byte
	err := s.conn.QueryRow(ctx, q, sessionID).Scan(&payload)
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
	_, err := s.conn.Exec(ctx, q, sessionID)
	if err != nil {
		return fmt.Errorf("wire delete %q: %w", sessionID, err)
	}
	return nil
}
