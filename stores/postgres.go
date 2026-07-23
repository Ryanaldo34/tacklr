package stores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// PostgresStore implements BaseStore against a Postgres connection via pgx.
type PostgresStore struct {
	conn *pgx.Conn
}

// NewPostgresStore wraps an existing pgx connection.
func NewPostgresStore(conn *pgx.Conn) *PostgresStore {
	return &PostgresStore{conn: conn}
}

func (s *PostgresStore) SaveSession(ctx context.Context, sessionID string, checkpoint SessionCheckpoint) error {
	contextWindow, err := json.Marshal(checkpoint.ContextWindow)
	if err != nil {
		return fmt.Errorf("marshal context window: %w", err)
	}
	state, err := json.Marshal(checkpoint.State)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	query := `INSERT INTO public.session (session_id, context_window, state)
		VALUES ($1, $2, $3)
		ON CONFLICT (session_id) DO UPDATE SET context_window = $2, state = $3`
	_, err = s.conn.Exec(ctx, query, sessionID, contextWindow, state)
	if err != nil {
		return fmt.Errorf("save session %q: %w", sessionID, err)
	}
	return nil
}

func (s *PostgresStore) LoadSession(ctx context.Context, sessionID string) (SessionCheckpoint, error) {
	query := `SELECT context_window, state FROM public.session WHERE session_id = $1`
	var contextWindowRaw, stateRaw []byte
	err := s.conn.QueryRow(ctx, query, sessionID).Scan(&contextWindowRaw, &stateRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SessionCheckpoint{}, fmt.Errorf("load session %q: %w", sessionID, ErrSessionNotFound)
		}
		return SessionCheckpoint{}, fmt.Errorf("load session %q: %w", sessionID, err)
	}

	var checkpoint SessionCheckpoint
	if err := json.Unmarshal(contextWindowRaw, &checkpoint.ContextWindow); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("unmarshal context window: %w", err)
	}
	if err := json.Unmarshal(stateRaw, &checkpoint.State); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("unmarshal state: %w", err)
	}
	return checkpoint, nil
}
