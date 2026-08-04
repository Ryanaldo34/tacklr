// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Hosts attach an Engine via AgentOptions.Brain. This package does not import
// the harness, session, or telemetry packages; Scope is passed in by the caller.
// Optional Observer (telemetry.NewBrainObserver) records ops without domain coupling.
//
// SearchContext is the retrieval session surface: host namespace + active ResultSet
// for continue (replaced on each search, find_exact, or large expand).
// Graph expand beyond containment uses GraphReader (brain/helixgraph for Helix).
//
// # Kind schemas (host-defined)
//
// Hosts register object kinds at startup with WithKinds / RegisterKinds so filters
// and schema() stay deterministic. When the process catalog is non-empty:
//   - property filters require a kind filter
//   - search/find_exact only consider registered kinds (implicit kind allow-list)
//   - schema() prefers the catalog over the store registry
//
// Optional SyncKindsToStore / LoadKindsFromStore persist or reload the catalog.
// Agent-defined kinds are out of scope for now (host/user-defined only).
//
// Integration tests (skipped under -short / without Docker):
//   - PostgresStore: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/enterprise-dev (in-memory)
package brain
