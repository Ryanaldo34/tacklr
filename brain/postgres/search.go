package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/brain"
)

// Search implements brain.Store in one round trip: every channel is one
// query of a batch, over the scope's parts and, when Scope.SessionID is set,
// the session's messages, and the store fuses the lists with its Ranking.
// Search ranks by the words (full text; rows that hold every term first)
// and by meaning (the query's embedding, same model only; a query that does
// not embed is answered by its words). Exact search ranks by the words and
// by trigram similarity, and puts an exact title match first.
func (s *Store) Search(ctx context.Context, scope brain.Scope, q brain.PartSearch) ([]brain.ScoredID, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	query := strings.TrimSpace(q.Query)
	if query == "" || q.Limit <= 0 {
		return nil, nil
	}
	b := &pgx.Batch{}
	// Parts take $1 (query), $2 (namespace), $3 (principals), $4 (limit),
	// then the filter arguments.
	where, fargs, err := brain.FilterSQL(q.Filters, 5)
	if err != nil {
		return nil, err
	}
	scoped := func(sql string, input any) {
		b.Queue(fmt.Sprintf(sql, where), append([]any{input, scope.Namespace, scope.Principals, q.Limit}, fargs...)...)
	}
	sid := strings.TrimSpace(scope.SessionID)
	session := func(sql string, input any) {
		if sid != "" {
			b.Queue(sql, input, sid, brain.NamespaceForSession(scope.Namespace, sid), q.Limit)
		}
	}
	scoped(partLexicalSQL, query)
	session(sessionLexicalSQL, query)
	if q.Exact {
		scoped(partTrigramSQL, query)
		session(sessionTrigramSQL, query)
	} else if vec, err := s.embedding.EmbedOne(ctx, query); err != nil {
		slog.WarnContext(ctx, "query embed failed; searching text only", "error", err)
	} else if vec != nil {
		// Vectors take the model as $5, so the filters start at $6.
		vwhere, vargs, _ := brain.FilterSQL(q.Filters, 6) // the same filters compiled above
		b.Queue(fmt.Sprintf(partVectorSQL, vwhere), append([]any{vec, scope.Namespace, scope.Principals, q.Limit, s.embedding.Model()}, vargs...)...)
		if sid != "" {
			b.Queue(sessionVectorSQL, vec, sid, brain.NamespaceForSession(scope.Namespace, sid), q.Limit, s.embedding.Model())
		}
	}
	br := s.db.SendBatch(ctx, b)
	defer br.Close()
	lists := make([][]brain.ScoredID, b.Len())
	for i := range lists {
		rows, _ := br.Query()
		if lists[i], err = pgx.CollectRows(rows, scanHit); err != nil {
			return nil, fmt.Errorf("postgres: search: %w", err)
		}
	}
	fused := s.ranking.Fuse(lists...)
	if q.Exact {
		fused = titleFirst(fused, query)
	}
	return fused[:min(len(fused), q.Limit)], nil
}

// Every channel selects the columns scanHit reads. A session message is its
// own parent, with its role as its title.
const (
	// ts_rank_cd with normalization 32 (rank / (rank + 1)) is below 1, so
	// adding 1 when every term matches ranks those rows first.
	partLexicalSQL = `
		SELECT id, coalesce(title, ''), coalesce(content, ''), parent_id, position, updated_at,
		       ts_rank_cd(search_tsv, tacklr_web_terms($1, false), 32)
		         + (search_tsv @@ tacklr_web_terms($1, true))::int AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND search_tsv @@ tacklr_web_terms($1, false)
		  AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])%s
		ORDER BY score DESC
		LIMIT $4`
	partTrigramSQL = `
		SELECT id, coalesce(title, ''), coalesce(content, ''), parent_id, position, updated_at,
		       GREATEST(similarity(coalesce(title, ''), $1), word_similarity($1, coalesce(content, ''))) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND (strpos(lower(coalesce(title, '')), lower($1)) > 0
		    OR strpos(lower(coalesce(content, '')), lower($1)) > 0
		    OR similarity(coalesce(title, ''), $1) > 0.3
		    OR word_similarity($1, coalesce(content, '')) > 0.3)
		  AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])%s
		ORDER BY score DESC
		LIMIT $4`
	// A cosine distance of 1 or more is no likeness at all, so it is no hit.
	partVectorSQL = `
		SELECT id, coalesce(title, ''), coalesce(content, ''), parent_id, position, updated_at,
		       1 - (embedding <=> $1::real[]::vector) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND embedding_model = $5 AND embedding <=> $1::real[]::vector < 1
		  AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])%s
		ORDER BY embedding <=> $1::real[]::vector
		LIMIT $4`
	sessionLexicalSQL = `
		SELECT id, role, search_text, id, position, updated_at,
		       ts_rank_cd(search_tsv, tacklr_web_terms($1, false), 32)
		         + (search_tsv @@ tacklr_web_terms($1, true))::int AS score
		FROM session_messages
		WHERE session_id = $2 AND namespace = $3::jsonb
		  AND search_tsv @@ tacklr_web_terms($1, false)
		ORDER BY score DESC
		LIMIT $4`
	sessionTrigramSQL = `
		SELECT id, role, search_text, id, position, updated_at,
		       GREATEST(similarity(role, $1), word_similarity($1, search_text)) AS score
		FROM session_messages
		WHERE session_id = $2 AND namespace = $3::jsonb
		  AND (strpos(lower(role), lower($1)) > 0
		    OR strpos(lower(search_text), lower($1)) > 0
		    OR similarity(role, $1) > 0.3
		    OR word_similarity($1, search_text) > 0.3)
		ORDER BY score DESC
		LIMIT $4`
	sessionVectorSQL = `
		SELECT id, role, search_text, id, position, updated_at,
		       1 - (embedding <=> $1::real[]::vector) AS score
		FROM session_messages
		WHERE session_id = $2 AND namespace = $3::jsonb
		  AND embedding_model = $5 AND embedding <=> $1::real[]::vector < 1
		ORDER BY embedding <=> $1::real[]::vector
		LIMIT $4`
)

func scanHit(row pgx.CollectableRow) (brain.ScoredID, error) {
	var h brain.ScoredID
	err := row.Scan(&h.ID, &h.Title, &h.Content, &h.ParentID, &h.Position, &h.UpdatedAt, &h.Score)
	return h, err
}

// titleFirst moves hits whose title is the query to the front, keeping the
// order otherwise.
func titleFirst(hits []brain.ScoredID, query string) []brain.ScoredID {
	out := make([]brain.ScoredID, 0, len(hits))
	var rest []brain.ScoredID
	for _, h := range hits {
		if strings.EqualFold(strings.TrimSpace(h.Title), query) {
			out = append(out, h)
		} else {
			rest = append(rest, h)
		}
	}
	return append(out, rest...)
}
