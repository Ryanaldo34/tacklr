-- =============================================================================
-- Signalize / Tacklr Retrieval Pipeline
-- PostgreSQL Schema (Generic, Extensible Object Model)
-- =============================================================================
-- Design decisions (locked):
--   1. Single generic `objects` table for ALL first-class entities
--      (Documents, EmailThreads, Deals, Chunks, Messages, custom kinds…).
--   2. Only the direct containment mapping (part → parent) lives in Postgres
--      via `parent_id` + `position`. This enables efficient parent promotion
--      and ordered child retrieval.
--   3. ALL other relationships (references, discusses, depends_on, reply_to,
--      similar_to, etc.) live exclusively in HelixDB and are traversed by
--      the expand() tool.
--   4. Extensibility for Tacklr users:
--        - free-form `kind` string
--        - arbitrary metadata in `properties` JSONB
--        - optional registry table for documentation / schema() tool
--   5. Search stack:
--        - True BM25 via pg_textsearch (Timescale/Tiger Data) — default
--          (ParadeDB pg_search remains a supported alternative)
--        - Dense vectors via pgvector
--        - Trigram (pg_trgm) for fuzzy / exact-ish matching in find_exact
--        - Temporal ranking bias toward most recently updated information
--          (always-on, deterministic, owned by the retrieval engine)
-- =============================================================================

-- Required extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";      -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "vector";        -- pgvector
CREATE EXTENSION IF NOT EXISTS "pg_trgm";       -- trigram similarity (find_exact fuzzy)
CREATE EXTENSION IF NOT EXISTS "pg_textsearch"; -- Timescale/Tiger Data – real BM25 (preferred)


-- ---------------------------------------------------------------------------
-- Optional: Object kind registry (used by schema() tool and validation)
-- Tacklr users can insert their own kinds here. Not required for operation.
-- ---------------------------------------------------------------------------
CREATE TABLE object_kinds (
    kind                text PRIMARY KEY,
    description         text,
    is_part             boolean NOT NULL DEFAULT false,  -- true for Chunk, Message, etc.
    is_parent           boolean NOT NULL DEFAULT false,  -- true for Document, Thread, etc.
    filterable_fields   jsonb NOT NULL DEFAULT '[]',    -- [{name, type, operators, examples}, ...]
    created_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE object_kinds IS
    'Optional registry of known object kinds. Powers the schema() discovery tool. '
    'Core framework works without rows here; kinds are free-form on the objects table.';


-- ---------------------------------------------------------------------------
-- Core generic objects table
-- ---------------------------------------------------------------------------
CREATE TABLE objects (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Identity & typing
    kind                text NOT NULL,                  -- 'Document', 'Chunk', 'EmailThread', 'Message', 'Deal', ...
    title               text,                           -- human-readable name / subject
    summary             text,                           -- short abstract (used in rich results)

    -- Extensible metadata (arbitrary domain fields)
    properties          jsonb NOT NULL DEFAULT '{}',    -- stage, amount_usd, owner_id, tags, status, path, etc.

    -- Content
    content             text,                           -- full text (for parts) or null (for pure parents)
    content_type        text,                           -- 'text/plain', 'text/markdown', 'application/json', ...

    -- Direct containment mapping ONLY (part → parent)
    -- This is NOT a general graph edge. General relations live in HelixDB.
    parent_id           uuid REFERENCES objects(id) ON DELETE CASCADE,
    position            integer,                        -- ordinal within parent (1-based, for stable ordering)

    -- Dense vector embedding (primarily for content-bearing parts)
    embedding           vector(1536),                   -- adjust dimension to your model (1536, 768, 3072, ...)
    embedding_model     text,                           -- e.g. 'text-embedding-3-small'
    embedding_updated_at timestamptz,

    -- Searchable text expression helper (concatenated fields for BM25)
    -- Used by the BM25 index so we can search across title + summary + content
    search_text         text GENERATED ALWAYS AS (
                            coalesce(title, '') || ' ' ||
                            coalesce(summary, '') || ' ' ||
                            coalesce(content, '')
                        ) STORED,

    -- Host-defined search namespace (isolation key) & lifecycle.
    -- Not multi-tenant SaaS by itself: hosts map user/org/project/default corpus
    -- into this UUID. Retrieval filters by it only when the harness sets one.
    namespace_id         uuid NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    deleted_at          timestamptz                     -- soft delete
);

COMMENT ON TABLE objects IS
    'Generic first-class object store. Both parents (Document, EmailThread, Deal…) '
    'and parts (Chunk, Message, Section…) live here. '
    'parent_id is the ONLY hierarchical pointer stored in Postgres; '
    'all other relationships are modeled in HelixDB.';

COMMENT ON COLUMN objects.parent_id IS
    'Direct containment only (Chunk→Document, Message→EmailThread, etc.). '
    'Used for parent promotion during search and for ordered expand of children. '
    'Do NOT use this column for arbitrary relations.';

COMMENT ON COLUMN objects.properties IS
    'Arbitrary typed metadata. Tacklr users / domain adapters put domain-specific '
    'fields here (e.g. {"stage": "negotiation", "amount_usd": 240000}). '
    'The schema() tool can document expected keys per kind via object_kinds.';

COMMENT ON COLUMN objects.search_text IS
    'Generated concatenation of title + summary + content for the BM25 index.';


-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- Core lookup
CREATE INDEX idx_objects_namespace_kind  ON objects (namespace_id, kind) WHERE deleted_at IS NULL;
CREATE INDEX idx_objects_parent          ON objects (parent_id, position) WHERE parent_id IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX idx_objects_kind            ON objects (kind) WHERE deleted_at IS NULL;

-- JSONB properties (GIN for containment / filter queries)
CREATE INDEX idx_objects_properties      ON objects USING GIN (properties jsonb_path_ops);

-- Trigram for find_exact (filenames, symbols, error strings, partial matches)
CREATE INDEX idx_objects_title_trgm      ON objects USING GIN (title gin_trgm_ops);
CREATE INDEX idx_objects_content_trgm    ON objects USING GIN (content gin_trgm_ops);

-- Dense vector similarity (HNSW)
CREATE INDEX idx_objects_embedding_hnsw  ON objects
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL AND deleted_at IS NULL;

-- Temporal
CREATE INDEX idx_objects_updated_at      ON objects (namespace_id, updated_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX idx_objects_created_at      ON objects (namespace_id, created_at DESC) WHERE deleted_at IS NULL;


-- ---------------------------------------------------------------------------
-- BM25 index via pg_textsearch (real BM25)
-- ---------------------------------------------------------------------------
-- Default recommendation: pg_textsearch (Timescale/Tiger Data).
-- ParadeDB pg_search is a supported alternative if preferred.
--
-- pg_textsearch indexes a single text expression. We use the generated
-- search_text column so title + summary + content are all searchable.

CREATE INDEX idx_objects_bm25 ON objects
USING bm25 (search_text)
WITH (text_config = 'english');

COMMENT ON INDEX idx_objects_bm25 IS
    'pg_textsearch BM25 index on search_text (title + summary + content). '
    'Provides true BM25 ranking (TF + IDF + length normalization). '
    'Scores from <@> are negative (lower = better). '
    'Used by the hybrid search pipeline. Do not fall back to tsvector/ts_rank.';


-- ---------------------------------------------------------------------------
-- Helper: automatic updated_at
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_objects_updated_at
    BEFORE UPDATE ON objects
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------------
-- Convenience views
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW parts AS
SELECT *
FROM objects
WHERE parent_id IS NOT NULL
  AND deleted_at IS NULL;

CREATE OR REPLACE VIEW parents AS
SELECT *
FROM objects
WHERE parent_id IS NULL
  AND deleted_at IS NULL;


-- ---------------------------------------------------------------------------
-- Example BM25 query patterns (pg_textsearch)
-- ---------------------------------------------------------------------------
-- Basic BM25 (lower score = more relevant):
--
--   SELECT id, title, search_text <@> 'oauth pkce implementation' AS score
--   FROM objects
--   WHERE namespace_id = $1              -- only when host set a search namespace
--     AND deleted_at IS NULL
--     AND parent_id IS NOT NULL          -- search parts
--   ORDER BY search_text <@> 'oauth pkce implementation'
--   LIMIT 20;
--
-- With explicit index (recommended when combining with other filters):
--
--   ORDER BY search_text <@> to_bm25query('oauth pkce implementation', 'idx_objects_bm25')
--
-- Hybrid (BM25 + vector + temporal) is performed in the retrieval service:
--   1. BM25 candidates
--   2. Vector candidates
--   3. RRF fusion
--   4. Temporal decay bias using updated_at  (always-on)
--   5. Parent promotion
--
-- Note: pg_textsearch returns negative scores. When doing RRF, convert to rank
-- position first (ROW_NUMBER) so the sign does not matter.


-- ---------------------------------------------------------------------------
-- Notes for the retrieval service
-- ---------------------------------------------------------------------------
-- 1. Lexical retrieval (search + find_exact):
--      Use the BM25 index (idx_objects_bm25) via the <@> operator.
--      This is real BM25 from pg_textsearch.
--
-- 2. Dense retrieval:
--      Use the HNSW index on embedding with cosine distance.
--
-- 3. Hybrid + Temporal ranking (locked pipeline):
--
--      a. Retrieve top-k candidates from BM25.
--      b. Retrieve top-k candidates from vector search.
--      c. Fuse BM25 + vector with Reciprocal Rank Fusion (RRF, k=60).
--      d. Apply temporal bias so results prefer more up-to-date information:
--
--           final_score = rrf_score * temporal_decay
--
--           temporal_decay = exp(-λ * age_in_days)
--
--           where age_in_days = (now() - updated_at) in days
--                 λ           = decay constant (default 0.01–0.03)
--
--         This is a gentle always-on bias:
--         - Strong lexical/semantic matches still win.
--         - When relevance is similar, newer information ranks higher.
--         - updated_at is the primary signal.
--
--      e. Perform parent promotion on the temporally-biased part-level results.
--      f. Return ResultSet of parent objects with evidence.
--
--      Hard temporal filters (created_after, updated_after, etc.) remain
--      available via the filters argument and are applied before retrieval.
--
-- 4. find_exact extras:
--      - Exact id / property equality
--      - BM25
--      - Trigram similarity on title/content (pg_trgm)
--      Merge and re-rank (temporal decay still applied).
--
-- 5. Parent promotion:
--      Group part hits by parent_id, score parent by best (or fused) part score,
--      attach evidence parts, return parents as the ResultSet objects.
--
-- 6. expand children of a parent:
--      SELECT … FROM objects WHERE parent_id = $1 ORDER BY position
--      (the only relation Postgres answers directly).
--
-- 7. All other relations → HelixDB.
--
-- 8. Alternative BM25 backend:
--      ParadeDB pg_search can be used instead of pg_textsearch if desired.
--      The retrieval service should abstract the concrete operator so either
--      backend can be swapped without changing agent-facing tools.
--
-- Temporal ranking parameters (engine-owned, not exposed to the LLM):
--   - λ (decay rate) can be tuned per deployment or per kind later.
--   - Default should be mild so relevance dominates but freshness is favored.
-- =============================================================================
