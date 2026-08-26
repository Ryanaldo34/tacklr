package brain

import (
	"context"
	"fmt"
	"strconv"
)

// DefaultEmbeddingDim is the pgvector column size when PostgresStore.EmbeddingDim is 0.
const DefaultEmbeddingDim = 1536

// Setup creates extensions, tables, and indexes (idempotent), then upserts kinds
// into object_kinds. EmbeddingDim is the pgvector size and must match the embedder;
// zero uses DefaultEmbeddingDim. This does not migrate an existing column to a new size.
func (s *PostgresStore) Setup(ctx context.Context, kinds ...KindSpec) error {
	dim := s.EmbeddingDim
	if dim <= 0 {
		dim = DefaultEmbeddingDim
	}
	for _, q := range schemaStatements(dim) {
		if _, err := s.db.Exec(ctx, q); err != nil {
			return fmt.Errorf("brain: setup: %w", err)
		}
	}
	return PersistKinds(ctx, s, kinds...)
}

func schemaStatements(dim int) []string {
	d := strconv.Itoa(dim)
	return []string{
		`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`,
		`CREATE EXTENSION IF NOT EXISTS "vector"`,
		`CREATE EXTENSION IF NOT EXISTS "pg_trgm"`,
		`CREATE EXTENSION IF NOT EXISTS "pg_textsearch"`,
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
			search_text          text GENERATED ALWAYS AS (
									 coalesce(title, '') || ' ' ||
									 coalesce(summary, '') || ' ' ||
									 coalesce(content, '')
								 ) STORED,
			namespace_id         uuid NOT NULL,
			created_at           timestamptz NOT NULL DEFAULT now(),
			updated_at           timestamptz NOT NULL DEFAULT now(),
			deleted_at           timestamptz
		)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_namespace_kind ON objects (namespace_id, kind) WHERE deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_parent ON objects (parent_id, position) WHERE parent_id IS NOT NULL AND deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_kind ON objects (kind) WHERE deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_properties ON objects USING GIN (properties jsonb_path_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_title_trgm ON objects USING GIN (title gin_trgm_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_content_trgm ON objects USING GIN (content gin_trgm_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_embedding_hnsw ON objects
			USING hnsw (embedding vector_cosine_ops)
			WITH (m = 16, ef_construction = 64)
			WHERE embedding IS NOT NULL AND deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_updated_at ON objects (namespace_id, updated_at DESC) WHERE deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_created_at ON objects (namespace_id, created_at DESC) WHERE deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_objects_bm25 ON objects
			USING bm25 (search_text)
			WITH (text_config = 'english')`,
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
	}
}
