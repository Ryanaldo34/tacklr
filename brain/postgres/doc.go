// Package postgres is the optional Postgres implementation of brain.Store.
//
// Hosts build a pool with PoolConfig and construct a Store with New(pool), then
// inject it into brain.NewEngine. PoolConfig sets search_path to the tacklr
// schema, then public, and rejects port 6543 (a transaction-mode pooler).
// pgx prepared statements need a direct connection or a session-mode pooler
// on port 5432. Call Setup once per database. Tables live in
// Store.Schema (default "tacklr"): objects, object_kinds, and session_messages.
//
// Lexical search uses a stored tsvector, search_tsv, on objects. Title is weight
// A, summary B, and body C. A partial GIN index covers live parts. Results order
// by ts_rank_cd with normalization 32. The query is websearch_to_tsquery (every
// term). When that page is shorter than requested, a second query ORs the same
// terms.
//
// Other object indexes: namespace GIN (jsonb @>), parent btree (list children),
// a partial kind btree for live parents, and HNSW for vectors. Session messages
// are a small per-session set and are not indexed beyond their unique keys.
// Property, trigram, and created/updated indexes are not created: those queries
// do not use them. pg_trgm and pgvector are always created.
//
// Query/Exec emit otelpgx spans under the caller context after telemetry.Init.
// Package brain does not import this package. MemoryStore stays in brain for
// tests and offline hosts.
package postgres
