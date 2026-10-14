package postgres

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/brain"
)

// DefaultEmbeddingDim is the pgvector column size when Store.EmbeddingDim is 0.
const DefaultEmbeddingDim = 1536

// DefaultSchema is the Postgres schema for tacklr tables when Store.Schema is empty.
const DefaultSchema = "tacklr"

// ftsRankNorm is the ts_rank_cd normalization flag that divides the rank by itself + 1.
const ftsRankNorm = 32

var schemaIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// SearchPath is the session search_path: the tacklr schema, then public.
func SearchPath(schema string) (string, error) {
	name, err := schemaName(schema)
	if err != nil {
		return "", err
	}
	return name + ", public", nil
}

// PoolConfig parses connString and sets SearchPath on every connection.
// Port 6543 is rejected: that port is a transaction-mode pooler, and pgx's
// prepared statements need a direct connection or a session-mode pooler (port 5432).
func PoolConfig(connString, schema string) (*pgxpool.Config, error) {
	path, err := SearchPath(schema)
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	if cfg.ConnConfig.Port == 6543 {
		return nil, fmt.Errorf("postgres: port 6543 is a transaction-mode pooler; connect on 5432 (direct or session mode) so pgx prepared statements work")
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = path
	return cfg, nil
}

func schemaName(schema string) (string, error) {
	name := strings.TrimSpace(schema)
	if name == "" {
		name = DefaultSchema
	}
	if !schemaIdent.MatchString(name) {
		return "", fmt.Errorf("postgres: schema %q is not a valid identifier", schema)
	}
	return name, nil
}

// Setup creates the tacklr schema, extensions, tables, and indexes (idempotent),
// then upserts kinds into object_kinds. EmbeddingDim is the pgvector size and must
// match the embedder; zero uses DefaultEmbeddingDim. This does not migrate an
// existing vector column to a new size.
//
// Lexical search uses the stored search_tsv column. pg_trgm and pgvector are
// created. Vector indexes are HNSW.
func (s *Store) Setup(ctx context.Context, kinds ...brain.KindSpec) error {
	name, err := schemaName(s.Schema)
	if err != nil {
		return err
	}
	if _, err := s.db.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
		return fmt.Errorf("postgres: setup: %w", err)
	}
	dim := cmp.Or(s.EmbeddingDim, DefaultEmbeddingDim)
	for _, q := range schemaStatements(dim) {
		if _, err := s.db.Exec(ctx, q); err != nil {
			return fmt.Errorf("postgres: setup: %w", err)
		}
	}
	return brain.PersistKinds(ctx, s, kinds...)
}

func schemaStatements(dim int) []string {
	d := strconv.Itoa(dim)
	return []string{
		`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`,
		`CREATE EXTENSION IF NOT EXISTS "vector"`,
		`CREATE EXTENSION IF NOT EXISTS "pg_trgm"`,
		`CREATE TABLE IF NOT EXISTS object_kinds (
			kind                text PRIMARY KEY,
			description         text,
			is_part             boolean NOT NULL DEFAULT false,
			is_parent           boolean NOT NULL DEFAULT false,
			filterable_fields   jsonb NOT NULL DEFAULT '[]',
			created_at          timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS objects (
			id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			kind                 text NOT NULL,
			title                text,
			summary              text,
			properties           jsonb NOT NULL DEFAULT '{}',
			content              text,
			content_type         text,
			parent_id            uuid REFERENCES objects(id) ON DELETE CASCADE,
			position             integer,
			embedding            vector(` + d + `),
			embedding_model      text,
			embedding_updated_at timestamptz,
			search_tsv           tsvector GENERATED ALWAYS AS (
									 setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
									 setweight(to_tsvector('english', coalesce(summary, '')), 'B') ||
									 setweight(to_tsvector('english', coalesce(content, '')), 'C')
								 ) STORED,
			namespace            jsonb NOT NULL,
			created_at           timestamptz NOT NULL DEFAULT now(),
			updated_at           timestamptz NOT NULL DEFAULT now(),
			deleted_at           timestamptz
		)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_namespace ON objects USING GIN (namespace jsonb_path_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_parent ON objects (parent_id, position) WHERE parent_id IS NOT NULL AND deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_parents_kind ON objects (kind) WHERE deleted_at IS NULL AND parent_id IS NULL`,
		`ALTER TABLE objects ADD COLUMN IF NOT EXISTS search_tsv tsvector
			GENERATED ALWAYS AS (
				setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
				setweight(to_tsvector('english', coalesce(summary, '')), 'B') ||
				setweight(to_tsvector('english', coalesce(content, '')), 'C')
			) STORED`,
		`CREATE INDEX IF NOT EXISTS idx_objects_embedding_hnsw ON objects
			USING hnsw (embedding vector_cosine_ops)
			WITH (m = 16, ef_construction = 64)
			WHERE embedding IS NOT NULL AND deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_fts ON objects USING GIN (search_tsv) WHERE deleted_at IS NULL AND parent_id IS NOT NULL`,
		`CREATE OR REPLACE FUNCTION tacklr_objects_set_updated_at()
		RETURNS trigger AS $$
		BEGIN
			NEW.updated_at = now();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS trg_objects_updated_at ON objects`,
		`CREATE TRIGGER trg_objects_updated_at
			BEFORE UPDATE ON objects
			FOR EACH ROW EXECUTE FUNCTION tacklr_objects_set_updated_at()`,
		`CREATE TABLE IF NOT EXISTS session_messages (
			id              uuid PRIMARY KEY,
			session_id      text NOT NULL,
			namespace       jsonb NOT NULL,
			generation      integer NOT NULL,
			generation_key  text NOT NULL,
			position        integer NOT NULL,
			role            text NOT NULL,
			body            jsonb NOT NULL,
			search_text     text NOT NULL DEFAULT '',
			search_tsv      tsvector GENERATED ALWAYS AS (
								setweight(to_tsvector('english', coalesce(search_text, '')), 'C')
							) STORED,
			embedding       vector(` + d + `),
			created_at      timestamptz NOT NULL DEFAULT now(),
			updated_at      timestamptz NOT NULL DEFAULT now(),
			UNIQUE (session_id, generation, position),
			UNIQUE (session_id, generation_key, position)
		)`,
		`ALTER TABLE session_messages ADD COLUMN IF NOT EXISTS search_tsv tsvector
			GENERATED ALWAYS AS (
				setweight(to_tsvector('english', coalesce(search_text, '')), 'C')
			) STORED`,
		// websearch_to_tsquery ANDs plain words. match_all false ORs those words.
		// Empty and stopword-only input returns NULL.
		`CREATE OR REPLACE FUNCTION tacklr_web_terms(q text, match_all boolean) RETURNS tsquery
		 LANGUAGE sql IMMUTABLE AS $$
		   SELECT CASE
		     WHEN q IS NULL OR btrim(q) = '' THEN NULL
		     WHEN numnode(websearch_to_tsquery('english', q)) = 0 THEN NULL
		     WHEN match_all THEN websearch_to_tsquery('english', q)
		     ELSE replace(websearch_to_tsquery('english', q)::text, ' & ', ' | ')::tsquery
		   END
		 $$`,
	}
}
