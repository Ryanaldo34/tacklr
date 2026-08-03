// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Hosts attach an Engine via AgentOptions.Brain. This package does not import
// the harness or session packages; Scope is passed in by the caller.
// Knowledge storage is separate from stores.BaseStore session checkpoints.
//
// Graph expand beyond containment uses GraphReader. Production Helix wiring is
// in brain/helixgraph. SearchContext holds the active ResultSet for continue
// (replaced on each search, find_exact, or large expand).
//
// Integration tests (skipped under -short / without Docker):
//   - PostgresStore: Testcontainers + brain/testdata/Dockerfile.postgres
//     (pgvector + pg_textsearch BM25)
//   - helixgraph: Testcontainers + ghcr.io/helixdb/enterprise-dev (in-memory)
package brain
