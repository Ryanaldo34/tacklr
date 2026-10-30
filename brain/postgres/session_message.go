package postgres

import (
	"cmp"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/brain"
)

// SaveSessionMessages implements brain.SessionWriter in one transaction.
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
	if err := host.Validate(); err != nil {
		return 0, err
	}
	ns := brain.NamespaceForSession(host, sessionID)
	var gen int
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		// The whole save is one round trip. One saver per session at a time,
		// so two workers cannot both take the next generation. Each row takes
		// the generation its key already has, or the next one; a retry of a
		// saved window inserts nothing.
		b := &pgx.Batch{}
		b.Queue(`SELECT pg_advisory_xact_lock(hashtext('tacklr.session:' || $1))`, sessionID)
		for i, msg := range messages {
			var model *string
			if len(msg.Embedding) > 0 {
				model = &msg.EmbeddingModel
			}
			b.Queue(`
				INSERT INTO session_messages (
					id, session_id, namespace, generation, generation_key, position, role, body, search_text, embedding, embedding_model
				) VALUES (
					$1, $2, $3::jsonb,
					coalesce(
						(SELECT generation FROM session_messages WHERE session_id = $2 AND namespace = $3::jsonb AND generation_key = $4 LIMIT 1),
						(SELECT coalesce(max(generation), 0) + 1 FROM session_messages WHERE session_id = $2)),
					$4, $5, $6, $7::jsonb, $8, $9::real[]::vector, $10)
				ON CONFLICT (session_id, namespace, generation_key, position) DO NOTHING`,
				uuid.New(), sessionID, ns, generationKey, i, msg.Role, cmp.Or(string(msg.Body), "{}"), msg.SearchText, msg.Embedding, model)
		}
		b.Queue(`SELECT generation FROM session_messages WHERE session_id = $1 AND namespace = $2::jsonb AND generation_key = $3 LIMIT 1`,
			sessionID, ns, generationKey)
		br := tx.SendBatch(ctx, b)
		errs := make([]error, 0, len(messages)+3)
		for range len(messages) + 1 {
			_, err := br.Exec()
			errs = append(errs, err)
		}
		errs = append(errs, br.QueryRow().Scan(&gen), br.Close())
		if err := cmp.Or(errs...); err != nil {
			return fmt.Errorf("postgres: session save: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return gen, nil
}

func (s *Store) sessionLexical(ctx context.Context, scope brain.Scope, query string, k int) ([]brain.ScoredID, error) {
	args := []any{query, strings.TrimSpace(scope.SessionID), brain.NamespaceForSession(scope.Namespace, scope.SessionID), k}
	return s.searchFTS(ctx, sessionLexicalSQL(true), sessionLexicalSQL(false), args, k)
}

func sessionLexicalSQL(matchAll bool) string {
	return fmt.Sprintf(`
		SELECT id, role, search_text, id, position, updated_at,
		       ts_rank_cd(search_tsv, tacklr_web_terms($1, %t), %d) AS score
		FROM session_messages
		WHERE session_id = $2 AND namespace = $3::jsonb
		  AND tacklr_web_terms($1, %t) IS NOT NULL
		  AND search_tsv @@ tacklr_web_terms($1, %t)
		ORDER BY score DESC
		LIMIT $4`, matchAll, ftsRankNorm, matchAll, matchAll)
}

func (s *Store) sessionVector(ctx context.Context, scope brain.Scope, model string, embedding []float32, k int) ([]brain.ScoredID, error) {
	q := `
		SELECT id, role, search_text, id, position, updated_at,
		       1 - (embedding <=> $1::real[]::vector) AS score
		FROM session_messages
		WHERE session_id = $2 AND namespace = $3::jsonb AND embedding IS NOT NULL AND embedding_model = $5
		ORDER BY embedding <=> $1::real[]::vector
		LIMIT $4`
	return s.queryScored(ctx, q, []any{embedding, strings.TrimSpace(scope.SessionID), brain.NamespaceForSession(scope.Namespace, scope.SessionID), k, model})
}

func (s *Store) sessionTrigram(ctx context.Context, scope brain.Scope, query string, k int) ([]brain.ScoredID, error) {
	q := `
		SELECT id, role, search_text, id, position, updated_at,
		       GREATEST(similarity(role, $1), word_similarity($1, search_text)) AS score
		FROM session_messages
		WHERE session_id = $2 AND namespace = $3::jsonb
		  AND (
		    strpos(lower(role), lower($1)) > 0
		    OR strpos(lower(search_text), lower($1)) > 0
		    OR similarity(role, $1) > 0.3
		    OR word_similarity($1, search_text) > 0.3
		  )
		ORDER BY score DESC
		LIMIT $4`
	return s.queryScored(ctx, q, []any{query, strings.TrimSpace(scope.SessionID), brain.NamespaceForSession(scope.Namespace, scope.SessionID), k})
}

// readSessionMessages reads ids from the scope's own session rows.
func (s *Store) readSessionMessages(ctx context.Context, scope brain.Scope, ids []uuid.UUID) ([]brain.Object, error) {
	sid := strings.TrimSpace(scope.SessionID)
	rows, _ := s.db.Query(ctx, `
		SELECT id, role, body, position, generation, namespace, created_at, updated_at
		FROM session_messages
		WHERE session_id = $1 AND namespace = $2::jsonb AND id = ANY($3)`,
		sid, brain.NamespaceForSession(scope.Namespace, sid), ids)
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (brain.Object, error) {
		var role, body string
		var position, gen int
		o := brain.Object{Kind: brain.KindSessionMessage}
		err := row.Scan(&o.ID, &role, &body, &position, &gen, &o.Namespace, &o.CreatedAt, &o.UpdatedAt)
		o.Title, o.Content, o.Position = role, body, &position
		o.Properties = map[string]any{"role": role, "generation": gen, "index": position}
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: session read: %w", err)
	}
	return out, nil
}

// mergeRanked fuses object hits with the session's own hits.
func mergeRanked(hits, extra []brain.ScoredID) []brain.ScoredID {
	if len(extra) == 0 {
		return hits
	}
	if len(hits) == 0 {
		return extra
	}
	return brain.FuseRanks([][]brain.ScoredID{hits, extra}, 0)
}

// DeleteSessionMessages implements brain.SessionWriter.
func (s *Store) DeleteSessionMessages(ctx context.Context, host brain.Namespace, sessionID string) error {
	if err := host.Validate(); err != nil {
		return err
	}
	sessionID = strings.TrimSpace(sessionID)
	if _, err := s.db.Exec(ctx, `DELETE FROM session_messages WHERE session_id = $1 AND namespace = $2::jsonb`,
		sessionID, brain.NamespaceForSession(host, sessionID)); err != nil {
		return fmt.Errorf("postgres: session delete: %w", err)
	}
	return nil
}

// PurgeSessionMessages implements brain.SessionWriter.
func (s *Store) PurgeSessionMessages(ctx context.Context, host brain.Namespace, before time.Time) (int, error) {
	if err := host.Validate(); err != nil {
		return 0, err
	}
	tag, err := s.db.Exec(ctx, `DELETE FROM session_messages WHERE created_at < $1 AND namespace @> $2::jsonb`, before, host)
	if err != nil {
		return 0, fmt.Errorf("postgres: session purge: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
