package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/brain"
)

// Search implements brain.Store in one round trip: every channel is one
// query of a batch, over the scope's records and parts and, when
// Scope.SessionID is set, the session's messages, and the store fuses the
// lists with its Ranking. Filters test the record a row belongs to: a part
// is joined to its parent only when there are filters.
// Search ranks by the words (full text; rows that hold every term first)
// and by meaning (the query's embedding, same model only; a query that does
// not embed is answered by its words). Exact search ranks by the words and
// by trigram similarity, and puts an exact title match first. Within keeps
// only that record's parts and leaves out session messages. An empty query
// lists the records the filters match, newest first, in one indexed query.
func (s *Store) Search(ctx context.Context, scope brain.Scope, q brain.RecordSearch) ([]brain.ScoredID, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		return nil, nil
	}
	plan, err := q.Filters.Compile()
	if err != nil {
		return nil, err
	}
	query := strings.TrimSpace(q.Query)
	if query == "" {
		return s.listRecords(ctx, scope, plan, q.Limit)
	}
	join := ""
	if len(plan.Conditions()) > 0 {
		join = " LEFT JOIN objects par ON par.id = o.parent_id"
	}
	b := &pgx.Batch{}
	// Rows take $1 (query), $2 (namespace), $3 (principals), $4 (limit),
	// then the filter arguments.
	where, fargs := restrict(plan, q.Within, 5)
	scoped := func(sql string, input any) {
		b.Queue(fmt.Sprintf(sql, join, where), append([]any{input, scope.Namespace, scope.Principals, q.Limit}, fargs...)...)
	}
	sid := strings.TrimSpace(scope.SessionID)
	if q.Within != uuid.Nil {
		sid = ""
	}
	session := func(sql string, input any) {
		if sid != "" {
			b.Queue(sql, input, sid, scope.Namespace.ForSession(sid), q.Limit)
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
		vwhere, vargs := restrict(plan, q.Within, 6)
		b.Queue(fmt.Sprintf(partVectorSQL, join, vwhere), append([]any{vec, scope.Namespace, scope.Principals, q.Limit, s.embedding.Model()}, vargs...)...)
		if sid != "" {
			b.Queue(sessionVectorSQL, vec, sid, scope.Namespace.ForSession(sid), q.Limit, s.embedding.Model())
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

// restrict is the WHERE suffix and arguments, from $start, of the filters
// and, when within is set, of keeping only within's parts.
func restrict(plan brain.FilterPlan, within uuid.UUID, start int) (string, []any) {
	where, args := filterSQL(plan, recordColumn, start)
	if within != uuid.Nil {
		// A part names its record.
		where += fmt.Sprintf(" AND o.parent_id = coalesce((SELECT w.parent_id FROM objects w WHERE w.id = $%[1]d), $%[1]d)", start+len(args))
		args = append(args, within)
	}
	return where, args
}

// listRecords is the records under scope that plan matches, newest first.
func (s *Store) listRecords(ctx context.Context, scope brain.Scope, plan brain.FilterPlan, limit int) ([]brain.ScoredID, error) {
	where, args := filterSQL(plan, func(column string) string { return "o." + column }, 4)
	rows, _ := s.db.Query(ctx, fmt.Sprintf(listRecordsSQL, where), append([]any{scope.Namespace, scope.Principals, limit}, args...)...)
	hits, err := pgx.CollectRows(rows, scanHit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list records: %w", err)
	}
	return hits, nil
}

// listRecordsSQL takes the filters (%s); idx_objects_records_recent serves
// its order.
const listRecordsSQL = `
	SELECT o.id, coalesce(o.title, ''), coalesce(o.content, ''), o.parent_id, o.position, o.updated_at, 0::float8
	FROM objects o
	WHERE o.deleted_at IS NULL AND o.parent_id IS NULL
	  AND o.tenant = ($1::jsonb)->0 AND o.namespace @> $1::jsonb AND (cardinality(o.readers) = 0 OR o.readers && $2::text[])%s
	ORDER BY o.updated_at DESC, o.id
	LIMIT $3`

// recordColumn is column of the record a searched row belongs to: its
// parent's when the row is a part (the join is in place whenever a filter
// uses it), else its own.
func recordColumn(column string) string {
	return "coalesce(par." + column + ", o." + column + ")"
}

// Every channel selects the columns scanHit reads. A session message is its
// own parent, with its role as its title. The record channels take the
// parent join (%[1]s) and the filters (%[2]s).
const (
	// ts_rank_cd with normalization 32 (rank / (rank + 1)) is below 1, so
	// adding 1 when every term matches ranks those rows first.
	partLexicalSQL = `
		SELECT o.id, coalesce(o.title, ''), coalesce(o.content, ''), o.parent_id, o.position, o.updated_at,
		       ts_rank_cd(o.search_tsv, tacklr_web_terms($1, false), 32)
		         + (o.search_tsv @@ tacklr_web_terms($1, true))::int AS score
		FROM objects o%[1]s
		WHERE o.deleted_at IS NULL
		  AND o.search_tsv @@ tacklr_web_terms($1, false)
		  AND o.tenant = ($2::jsonb)->0 AND o.namespace @> $2::jsonb AND (cardinality(o.readers) = 0 OR o.readers && $3::text[])%[2]s
		ORDER BY score DESC
		LIMIT $4`
	partTrigramSQL = `
		SELECT o.id, coalesce(o.title, ''), coalesce(o.content, ''), o.parent_id, o.position, o.updated_at,
		       GREATEST(similarity(coalesce(o.title, ''), $1), word_similarity($1, coalesce(o.content, ''))) AS score
		FROM objects o%[1]s
		WHERE o.deleted_at IS NULL
		  AND (strpos(lower(coalesce(o.title, '')), lower($1)) > 0
		    OR strpos(lower(coalesce(o.content, '')), lower($1)) > 0
		    OR similarity(coalesce(o.title, ''), $1) > 0.3
		    OR word_similarity($1, coalesce(o.content, '')) > 0.3)
		  AND o.tenant = ($2::jsonb)->0 AND o.namespace @> $2::jsonb AND (cardinality(o.readers) = 0 OR o.readers && $3::text[])%[2]s
		ORDER BY score DESC
		LIMIT $4`
	// A cosine distance of 1 or more is no likeness at all, so it is no hit.
	partVectorSQL = `
		SELECT o.id, coalesce(o.title, ''), coalesce(o.content, ''), o.parent_id, o.position, o.updated_at,
		       1 - (o.embedding <=> $1::real[]::vector) AS score
		FROM objects o%[1]s
		WHERE o.deleted_at IS NULL
		  AND o.embedding_model = $5 AND o.embedding <=> $1::real[]::vector < 1
		  AND o.tenant = ($2::jsonb)->0 AND o.namespace @> $2::jsonb AND (cardinality(o.readers) = 0 OR o.readers && $3::text[])%[2]s
		ORDER BY o.embedding <=> $1::real[]::vector
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
