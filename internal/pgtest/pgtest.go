// Package pgtest names the Postgres image live tests run: pgvector on
// Postgres 17. The brain store's Setup creates the extensions it needs
// (pgcrypto, vector, pg_trgm) on it.
package pgtest

// Image is the Postgres image of the brain store, wire store and Temporal
// live tests.
const Image = "pgvector/pgvector:0.8.7-pg17"
