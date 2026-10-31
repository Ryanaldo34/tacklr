package postgres

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/brain"
)

// DefaultEmbeddingDim is the pgvector column size when Options.EmbeddingDim is 0.
const DefaultEmbeddingDim = 1536

// DefaultSchema is the Postgres schema for tacklr tables when Options.Schema is empty.
const DefaultSchema = "tacklr"

// DefaultStatementTimeout caps one statement when Options.StatementTimeout is 0.
const DefaultStatementTimeout = 30 * time.Second

var schemaIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Options configures Open. Zero fields take the defaults.
type Options struct {
	// Schema holds the tacklr tables. Default DefaultSchema.
	Schema string
	// EmbeddingDim is the pgvector column size; it must match the embedder.
	// Default DefaultEmbeddingDim.
	EmbeddingDim int
	// StatementTimeout is the server-side statement_timeout, so a slow
	// statement is cancelled even when the client is gone (a frozen or
	// torn-down worker). Zero means DefaultStatementTimeout; negative turns
	// it off.
	StatementTimeout time.Duration
	// Embedding embeds objects, parts, session messages and queries; its
	// vectors must have EmbeddingDim values. No Embedder means search ranks
	// by the words alone.
	Embedding brain.Embedding
	// Ranking fuses the search channels.
	Ranking brain.Ranking
}

// Open returns a Store with its own connection pool for connString; pool
// settings ride in connString (pool_max_conns, …). Every connection starts
// with search_path set to the schema, then public, and the statement
// timeout. Statements with arguments are prepared once per connection and
// cached, and an otelpgx tracer emits a span per statement under the
// caller's context. Connections open on first use. Close the Store when done.
//
// A direct connection or a session-mode pooler works as is. A
// transaction-mode pooler (such as port 6543 on hosted Postgres) must
// support prepared statements (PgBouncer 1.21+ with max_prepared_statements,
// Supavisor) and must forward the search_path and statement_timeout startup
// parameters (or set them on the database role).
func Open(ctx context.Context, connString string, opts Options) (*Store, error) {
	schema, err := schemaName(opts.Schema)
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ", public"
	timeout := cmp.Or(opts.StatementTimeout, DefaultStatementTimeout)
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(max(timeout.Milliseconds(), 0), 10)
	cfg.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithTrimSQLInSpanName(), otelpgx.WithDisableAcquireTracer())
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	return &Store{
		db: pool, schema: schema, dim: cmp.Or(opts.EmbeddingDim, DefaultEmbeddingDim),
		embedding: opts.Embedding, ranking: opts.Ranking,
	}, nil
}

// Close closes the Store's connections.
func (s *Store) Close() { s.db.Close() }

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

// Setup creates the schema, tables, indexes and functions the Store uses,
// then upserts kinds into object_kinds. Every statement is idempotent, so run
// it on each deploy; concurrent runs wait for each other on an advisory lock.
// Setup fails when the vector column has another size than
// Options.EmbeddingDim (changing it needs a re-embed, which Setup does not do).
func (s *Store) Setup(ctx context.Context, kinds ...brain.KindSpec) error {
	// One script, run in one transaction with the simple protocol (DDL is
	// never worth preparing). The schema name is a checked identifier, so it
	// is safe to write into the script. Waiting for another Setup's lock and
	// building indexes on large tables can both outlast the pool's statement
	// timeout; ctx bounds them.
	script := fmt.Sprintf(schemaScript, s.schema, pgx.Identifier{s.schema}.Sanitize(), s.dim)
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, script, pgx.QueryExecModeSimpleProtocol)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: setup: %w", err)
	}
	return brain.PersistKinds(ctx, s, kinds...)
}

// schemaScript is the whole schema. Arguments: the schema name, the same
// name as a quoted identifier, and the vector size.
const schemaScript = `
SET LOCAL statement_timeout = 0;
SELECT pg_advisory_xact_lock(hashtext('tacklr.setup:%[1]s'));
CREATE SCHEMA IF NOT EXISTS %[2]s;
-- Keep the connection's path: extension types (vector) may live elsewhere.
SELECT set_config('search_path', '%[2]s, ' || current_setting('search_path'), true);
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS object_kinds (
	kind              text PRIMARY KEY,
	description       text,
	is_part           boolean NOT NULL DEFAULT false,
	is_parent         boolean NOT NULL DEFAULT false,
	filterable_fields jsonb NOT NULL DEFAULT '[]',
	created_at        timestamptz NOT NULL DEFAULT now()
);

-- Pages keep free space, and position is not indexed, so moving a reused
-- tree part is a heap-only (HOT) update with no index writes.
CREATE TABLE IF NOT EXISTS objects (
	id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	kind                 text NOT NULL,
	title                text,
	summary              text,
	properties           jsonb NOT NULL DEFAULT '{}',
	content              text,
	content_type         text,
	parent_id            uuid REFERENCES objects(id) ON DELETE CASCADE,
	position             integer,
	embedding            vector(%[3]d),
	embedding_model      text,
	embedding_updated_at timestamptz,
	search_tsv           tsvector GENERATED ALWAYS AS (
		setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
		setweight(to_tsvector('english', coalesce(summary, '')), 'B') ||
		setweight(to_tsvector('english', coalesce(content, '')), 'C')
	) STORED,
	namespace            jsonb NOT NULL,
	-- The first namespace attr. Every scoped statement matches it by equality.
	tenant               jsonb GENERATED ALWAYS AS (namespace->0) STORED,
	readers              text[] NOT NULL DEFAULT '{}',
	-- Every tree commit increments it; a commit that read an older one fails.
	tree_rev             bigint NOT NULL DEFAULT 0,
	created_at           timestamptz NOT NULL DEFAULT now(),
	updated_at           timestamptz NOT NULL DEFAULT now(),
	deleted_at           timestamptz
) WITH (fillfactor = 85);
CREATE INDEX IF NOT EXISTS idx_objects_tenant ON objects (tenant);
CREATE INDEX IF NOT EXISTS idx_objects_namespace ON objects USING GIN (namespace jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_objects_parent_id ON objects (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_objects_parents_kind ON objects (kind) WHERE deleted_at IS NULL AND parent_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_objects_readers ON objects USING GIN (readers) WHERE cardinality(readers) > 0;
CREATE INDEX IF NOT EXISTS idx_objects_deleted ON objects (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_objects_embedding_hnsw ON objects
	USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)
	WHERE embedding IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_objects_fts ON objects USING GIN (search_tsv) WHERE deleted_at IS NULL AND parent_id IS NOT NULL;
CREATE OR REPLACE FUNCTION tacklr_objects_set_updated_at() RETURNS trigger AS $$
BEGIN
	NEW.updated_at = now();
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_objects_updated_at
	BEFORE UPDATE ON objects
	FOR EACH ROW EXECUTE FUNCTION tacklr_objects_set_updated_at();

-- Parts of tree writes in progress. Unlogged and without vector or text
-- indexes: rows are written once, read once by the commit, and lost on a
-- crash, which only abandons uncommitted writes.
CREATE UNLOGGED TABLE IF NOT EXISTS objects_staging (
	run             uuid NOT NULL,
	id              uuid NOT NULL,
	reused          boolean NOT NULL,
	parent_id       uuid NOT NULL,
	position        integer,
	properties      jsonb NOT NULL DEFAULT '{}',
	kind            text,
	title           text,
	summary         text,
	content         text,
	content_type    text,
	embedding       real[],
	embedding_model text,
	namespace       jsonb NOT NULL,
	tenant          jsonb GENERATED ALWAYS AS (namespace->0) STORED,
	created_at      timestamptz,
	updated_at      timestamptz,
	staged_at       timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (run, id)
);

CREATE TABLE IF NOT EXISTS session_messages (
	id              uuid PRIMARY KEY,
	session_id      text NOT NULL,
	namespace       jsonb NOT NULL,
	tenant          jsonb GENERATED ALWAYS AS (namespace->0) STORED,
	generation      integer NOT NULL,
	generation_key  text NOT NULL,
	position        integer NOT NULL,
	role            text NOT NULL,
	body            jsonb NOT NULL,
	search_text     text NOT NULL DEFAULT '',
	search_tsv      tsvector GENERATED ALWAYS AS (
		setweight(to_tsvector('english', coalesce(search_text, '')), 'C')
	) STORED,
	embedding       vector(%[3]d),
	embedding_model text,
	created_at      timestamptz NOT NULL DEFAULT now(),
	updated_at      timestamptz NOT NULL DEFAULT now(),
	UNIQUE (session_id, generation, position)
);
-- A generation key is unique per host and session, not per session id alone.
CREATE UNIQUE INDEX IF NOT EXISTS ux_session_messages_key ON session_messages (session_id, namespace, generation_key, position);
CREATE INDEX IF NOT EXISTS idx_session_messages_tenant ON session_messages (tenant);
CREATE INDEX IF NOT EXISTS idx_session_messages_namespace ON session_messages USING GIN (namespace jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_session_messages_created ON session_messages (created_at);

CREATE TABLE IF NOT EXISTS audit_events (
	id         bigserial PRIMARY KEY,
	at         timestamptz NOT NULL,
	op         text NOT NULL,
	namespace  jsonb NOT NULL,
	principals text[] NOT NULL DEFAULT '{}',
	session_id text,
	object_ids uuid[] NOT NULL DEFAULT '{}',
	count      integer NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_audit_events_namespace ON audit_events USING GIN (namespace jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_audit_events_at ON audit_events (at);

-- websearch_to_tsquery ANDs plain words; match_all false ORs them. Empty and
-- stopword-only input returns NULL.
CREATE OR REPLACE FUNCTION tacklr_web_terms(q text, match_all boolean) RETURNS tsquery
LANGUAGE sql IMMUTABLE AS $$
	SELECT CASE
		WHEN q IS NULL OR btrim(q) = '' THEN NULL
		WHEN numnode(websearch_to_tsquery('english', q)) = 0 THEN NULL
		WHEN match_all THEN websearch_to_tsquery('english', q)
		ELSE replace(websearch_to_tsquery('english', q)::text, ' & ', ' | ')::tsquery
	END
$$;

DO $check$ DECLARE have text; BEGIN
	SELECT format_type(atttypid, atttypmod) INTO have FROM pg_attribute
	WHERE attrelid = 'objects'::regclass AND attname = 'embedding';
	IF have <> 'vector(%[3]d)' THEN
		RAISE EXCEPTION 'objects.embedding is %%, want vector(%[3]d): EmbeddingDim does not match the database', have;
	END IF;
END $check$;
`
