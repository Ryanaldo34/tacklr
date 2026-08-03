-- Integration-test schema for PostgresStore.
-- Image: brain/testdata/Dockerfile.postgres (pgvector + pg_textsearch).
-- Embedding dimension is intentionally small for fixture vectors.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "vector";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "pg_textsearch";

CREATE TABLE object_kinds (
    kind                text PRIMARY KEY,
    description         text,
    is_part             boolean NOT NULL DEFAULT false,
    is_parent           boolean NOT NULL DEFAULT false,
    filterable_fields   jsonb NOT NULL DEFAULT '[]',
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE objects (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind                 text NOT NULL,
    title                text,
    summary              text,
    properties           jsonb NOT NULL DEFAULT '{}',
    content              text,
    content_type         text,
    parent_id            uuid REFERENCES objects(id) ON DELETE CASCADE,
    position             integer,
    embedding            vector(3),
    embedding_model      text,
    embedding_updated_at timestamptz,
    search_text          text GENERATED ALWAYS AS (
                             coalesce(title, '') || ' ' ||
                             coalesce(summary, '') || ' ' ||
                             coalesce(content, '')
                         ) STORED,
    namespace_id          uuid NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    deleted_at           timestamptz
);

CREATE INDEX idx_objects_namespace_kind ON objects (namespace_id, kind) WHERE deleted_at IS NULL;
CREATE INDEX idx_objects_parent ON objects (parent_id, position) WHERE parent_id IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX idx_objects_title_trgm ON objects USING GIN (title gin_trgm_ops);
CREATE INDEX idx_objects_content_trgm ON objects USING GIN (content gin_trgm_ops);
CREATE INDEX idx_objects_embedding_hnsw ON objects
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL AND deleted_at IS NULL;

-- BM25 via pg_textsearch (same operator surface as production PostgresStore.SearchLexical).
CREATE INDEX idx_objects_bm25 ON objects
    USING bm25 (search_text)
    WITH (text_config = 'english');
