// Package postgres keeps sessions durable in Postgres: the session Store
// (checkpoints and tool-call bodies), SecretStorage (the users' delegated
// tokens, sealed before they are written) and the EventLog.
//
// The host owns the connection pool and passes it in; the three share it,
// and can share it with brain/postgres. Every table is qualified by the
// schema, so the pool needs no search_path. Run DB.Setup on each deploy.
package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/internal/pgtrace"
)

// DefaultSchema holds the tables when Options.Schema is empty. It is the
// schema brain/postgres uses too.
const DefaultSchema = "tacklr"

// Options configures New. Zero fields take the defaults.
type Options struct {
	// Schema holds the session tables. Default DefaultSchema.
	Schema string
}

// DB is the session tables in one schema of the host's pool.
type DB struct {
	pool *pgxpool.Pool
	// schema is the quoted identifier written before every table name.
	schema string
	// channel is the NOTIFY channel of the schema's event log.
	channel string
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// New returns the session tables of opts.Schema in pool. It does no I/O.
func New(pool *pgxpool.Pool, opts Options) (*DB, error) {
	name := strings.TrimSpace(opts.Schema)
	if name == "" {
		name = DefaultSchema
	}
	if !identifier.MatchString(name) {
		return nil, fmt.Errorf("postgres: schema %q is not a valid identifier", opts.Schema)
	}
	return &DB{pool: pool, schema: pgx.Identifier{name}.Sanitize(), channel: name + "_session_events"}, nil
}

// Configure sets the tracer of a host's pool config, before
// pgxpool.NewWithConfig: a span for each statement under its caller's
// context, so a turn's trace shows its checkpoint, call and event writes.
// A pool that brain/postgres.Configure set up has it already.
func Configure(cfg *pgxpool.Config) {
	cfg.ConnConfig.Tracer = pgtrace.Tracer()
}

// sql writes the schema in place of each {s} of query.
func (db *DB) sql(query string) string { return strings.ReplaceAll(query, "{s}", db.schema) }

// Setup creates the schema and the session tables. Every statement is
// idempotent, so run it on each deploy; concurrent runs (hosts and workers
// that start together) wait for each other on an advisory lock.
func (db *DB) Setup(ctx context.Context) error {
	_, err := db.pool.Exec(ctx, db.sql(setupScript), pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return fmt.Errorf("postgres: setup: %w", err)
	}
	return nil
}

// setupScript runs as one implicit transaction (the simple protocol), which
// holds the lock until it ends: two concurrent CREATE ... IF NOT EXISTS of
// one table can otherwise both try to create it.
const setupScript = `
SELECT pg_advisory_xact_lock(hashtext('tacklr.session_setup:' || '{s}'));
CREATE SCHEMA IF NOT EXISTS {s};

CREATE TABLE IF NOT EXISTS {s}.session_checkpoints (
	session_id text PRIMARY KEY,
	revision   bigint NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {s}.session_checkpoint_rows (
	session_id text NOT NULL,
	kind       text NOT NULL,
	key        text NOT NULL,
	value      bytea NOT NULL,
	PRIMARY KEY (session_id, kind, key)
);

CREATE TABLE IF NOT EXISTS {s}.session_calls (
	session_id text NOT NULL,
	call_key   text NOT NULL,
	body       bytea NOT NULL,
	PRIMARY KEY (session_id, call_key)
);

CREATE TABLE IF NOT EXISTS {s}.session_secrets (
	session_id text PRIMARY KEY,
	sealed     bytea NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {s}.session_event_heads (
	session_id text PRIMARY KEY,
	seq        bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS {s}.session_events (
	session_id text NOT NULL,
	seq        bigint NOT NULL,
	topic      text NOT NULL,
	event      jsonb NOT NULL,
	PRIMARY KEY (session_id, seq)
);
`
