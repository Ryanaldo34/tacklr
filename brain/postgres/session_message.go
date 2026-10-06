package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/brain"
)

// SaveSessionMessages implements brain.ObjectWriter.
func (s *Store) SaveSessionMessages(ctx context.Context, host brain.Namespace, sessionID, generationKey string, messages []brain.SessionMessage) (int, error) {
	sessionID = strings.TrimSpace(sessionID)
	generationKey = strings.TrimSpace(generationKey)
	if sessionID == "" {
		return 0, fmt.Errorf("%w: session id is required", brain.ErrInvalid)
	}
	if generationKey == "" {
		return 0, fmt.Errorf("%w: generation key is required", brain.ErrInvalid)
	}
	if len(messages) == 0 {
		return 0, fmt.Errorf("%w: window is empty", brain.ErrInvalid)
	}
	ns := brain.NamespaceForSession(host, sessionID)
	rawNS, err := ns.Value()
	if err != nil {
		return 0, fmt.Errorf("postgres: session namespace: %w", err)
	}
	var existing int
	err = s.db.QueryRow(ctx, `
		SELECT generation FROM session_messages
		WHERE session_id = $1 AND generation_key = $2
		LIMIT 1`, sessionID, generationKey).Scan(&existing)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("postgres: session generation: %w", err)
	}
	var gen int
	if err := s.db.QueryRow(ctx, `
		SELECT COALESCE(MAX(generation), 0) + 1 FROM session_messages WHERE session_id = $1`,
		sessionID).Scan(&gen); err != nil {
		return 0, fmt.Errorf("postgres: session generation next: %w", err)
	}
	for i, msg := range messages {
		body := msg.Body
		if len(body) == 0 {
			body = []byte("{}")
		}
		var emb any
		if len(msg.Embedding) > 0 {
			emb = formatVectorLiteral(msg.Embedding)
		}
		if _, err := s.db.Exec(ctx, `
			INSERT INTO session_messages (
				id, session_id, namespace, generation, generation_key, position, role, body, search_text, embedding
			) VALUES (
				$1, $2, $3::jsonb, $4, $5, $6, $7, $8::jsonb, $9,
				CASE WHEN $10::text IS NULL THEN NULL ELSE $10::vector END
			)`,
			uuid.New(), sessionID, rawNS, gen, generationKey, i, msg.Role, body, msg.SearchText, emb,
		); err != nil {
			_, _ = s.db.Exec(ctx, `DELETE FROM session_messages WHERE session_id = $1 AND generation_key = $2`, sessionID, generationKey)
			return 0, fmt.Errorf("postgres: session insert: %w", err)
		}
	}
	return gen, nil
}

func (s *Store) sessionLexical(ctx context.Context, scope brain.Scope, query string, k int) ([]brain.ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" || strings.TrimSpace(scope.SessionID) == "" {
		return nil, nil
	}
	raw, err := brain.NamespaceForSession(scope.Namespace, scope.SessionID).Value()
	if err != nil {
		return nil, err
	}
	q := `
		SELECT id, role, search_text, id, position, updated_at,
		       search_text <@> to_bm25query($1, 'idx_session_messages_bm25') AS score
		FROM session_messages
		WHERE namespace = $2::jsonb
		ORDER BY search_text <@> to_bm25query($1, 'idx_session_messages_bm25')
		LIMIT $3`
	return s.queryScored(ctx, q, []any{query, raw, k}, true)
}

func (s *Store) sessionVector(ctx context.Context, scope brain.Scope, embedding []float32, k int) ([]brain.ScoredID, error) {
	if k <= 0 || len(embedding) == 0 || strings.TrimSpace(scope.SessionID) == "" {
		return nil, nil
	}
	raw, err := brain.NamespaceForSession(scope.Namespace, scope.SessionID).Value()
	if err != nil {
		return nil, err
	}
	q := `
		SELECT id, role, search_text, id, position, updated_at,
		       1 - (embedding <=> $1::vector) AS score
		FROM session_messages
		WHERE namespace = $2::jsonb AND embedding IS NOT NULL
		ORDER BY embedding <=> $1::vector
		LIMIT $3`
	return s.queryScored(ctx, q, []any{formatVectorLiteral(embedding), raw, k}, false)
}

func (s *Store) sessionTrigram(ctx context.Context, scope brain.Scope, query string, k int) ([]brain.ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" || strings.TrimSpace(scope.SessionID) == "" {
		return nil, nil
	}
	raw, err := brain.NamespaceForSession(scope.Namespace, scope.SessionID).Value()
	if err != nil {
		return nil, err
	}
	q := `
		SELECT id, role, search_text, id, position, updated_at,
		       GREATEST(similarity(role, $1), similarity(search_text, $1)) AS score
		FROM session_messages
		WHERE namespace = $2::jsonb
		  AND (
		    similarity(role, $1) > 0.3
		    OR similarity(search_text, $1) > 0.3
		  )
		ORDER BY score DESC
		LIMIT $3`
	return s.queryScored(ctx, q, []any{query, raw, k}, false)
}

func (s *Store) readSessionMessage(ctx context.Context, sessionID string, id uuid.UUID) (brain.Object, error) {
	if id == uuid.Nil {
		return brain.Object{}, fmt.Errorf("%w: object id is required", brain.ErrInvalid)
	}
	objs, err := s.readSessionMessages(ctx, sessionID, []uuid.UUID{id})
	if err != nil {
		return brain.Object{}, err
	}
	if len(objs) == 0 {
		return brain.Object{}, fmt.Errorf("%w: %s", brain.ErrNotFound, id)
	}
	return objs[0], nil
}

func (s *Store) readSessionMessages(ctx context.Context, sessionID string, ids []uuid.UUID) ([]brain.Object, error) {
	if len(ids) == 0 || strings.TrimSpace(sessionID) == "" {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, role, body, position, generation, namespace, created_at, updated_at
		FROM session_messages
		WHERE session_id = $1 AND id = ANY($2)`, sessionID, ids)
	if err != nil {
		return nil, fmt.Errorf("postgres: session read: %w", err)
	}
	defer rows.Close()
	var out []brain.Object
	for rows.Next() {
		var (
			id       uuid.UUID
			role     string
			body     []byte
			position int
			gen      int
			ns       brain.Namespace
			created  time.Time
			updated  time.Time
		)
		if err := rows.Scan(&id, &role, &body, &position, &gen, &ns, &created, &updated); err != nil {
			return nil, fmt.Errorf("postgres: session read: %w", err)
		}
		pos := position
		out = append(out, brain.Object{
			ID:      id,
			Kind:    brain.KindSessionMessage,
			Title:   role,
			Content: string(body),
			Properties: map[string]any{
				"role":       role,
				"generation": gen,
				"index":      position,
			},
			Position:  &pos,
			Namespace: ns,
			CreatedAt: created,
			UpdatedAt: updated,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: session read: %w", err)
	}
	return out, nil
}

func mergeRanked(hits, extra []brain.ScoredID, err error) ([]brain.ScoredID, error) {
	if err != nil || len(extra) == 0 {
		return hits, err
	}
	if len(hits) == 0 {
		return extra, nil
	}
	return brain.FuseRanks([][]brain.ScoredID{hits, extra}, 0), nil
}

// DeleteSessionMessages implements brain.ObjectWriter.
func (s *Store) DeleteSessionMessages(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM session_messages WHERE session_id = $1`, sessionID); err != nil {
		return fmt.Errorf("postgres: session delete: %w", err)
	}
	return nil
}
