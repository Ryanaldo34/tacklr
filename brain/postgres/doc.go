// Package postgres is the optional Postgres implementation of brain.Store.
//
// Hosts open a Store with Open(ctx, connString, Options) and inject it into
// brain.NewEngine. Every connection's search_path is the tacklr schema, then
// public, with a server-side statement_timeout (Options).
// A transaction-mode pooler must support prepared statements and forward
// those startup parameters. Run Setup on each deploy: its schema script is
// idempotent, and concurrent runs wait for each other on an advisory lock.
// Tables live in Options.Schema (default "tacklr"): objects, objects_staging,
// object_kinds, session_messages, and audit_events.
// Store.Record is a brain.AuditFunc over audit_events (brain.WithAudit(store.Record)).
//
// Lexical search uses a stored tsvector, search_tsv, on objects. Title is weight
// A, summary B, and body C. A partial GIN index covers live parts. Results order
// by ts_rank_cd with normalization 32. The query is websearch_to_tsquery (every
// term). When that page is shorter than requested, a second query ORs the same
// terms.
//
// Tree writes (brain.Engine.PutTree) stage parts with one COPY per batch into
// objects_staging: an unlogged table with no vector or text index, so staged
// rows cost one cheap write and are lost on a crash, which only abandons
// uncommitted writes. Vectors go as binary real[]. CommitTree then sends its
// whole commit as one batch in one transaction: a per-tree advisory lock, the
// parent upsert, the objects.tree_rev check and increment (a stale revision is
// brain.ErrConflict), the staging checks, and the swap. The swap deletes
// replaced parts, inserts new parts with INSERT ... SELECT, and updates
// reused parts only when their position, properties, namespace or readers
// changed. Position is not indexed and the table keeps free space
// (fillfactor 85), so those updates are heap-only (HOT) and write no index
// entries. A failed check rolls the whole batch back. A session window is
// saved as one batch too: each row takes its key's generation or the next one.
//
// Setup runs one script that creates whatever is missing, then checks the
// vector size. PurgeDeleted and Erase are single statements.
//
// Other object indexes: namespace GIN (jsonb @>), parent btree (list children),
// a partial kind btree for live parents, and HNSW for vectors. Session messages
// are a small per-session set and are not indexed beyond their unique keys.
// Property, trigram, and created/updated indexes are not created: those queries
// do not use them. pg_trgm and pgvector are always created.
//
// The Store's pool emits otelpgx spans under the caller context after telemetry.Init.
// Package brain does not import this package. MemoryStore stays in brain for
// tests and offline hosts.
package postgres
