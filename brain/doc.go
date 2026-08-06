// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Hosts attach an Engine via AgentOptions.Brain. This package does not import
// the harness, session, or telemetry packages; Scope is passed in by the caller.
// Optional Observer (telemetry.NewBrainObserver) records ops without domain coupling.
//
// SearchContext is the retrieval session surface: host namespace + active ResultSet
// for continue (replaced on each search, find_exact, find_objects, or large expand).
//
// # Kind schemas (host migrations)
//
// Object kinds are host/user-defined for determinism. Register with ApplyKinds
// (or WithKinds). Agent-defined kinds are out of scope.
//
// # Explicit writes (no handoff side effects)
//
// Durable objects are written only via Engine.Put / SoftDelete (host SDK) or
// kind-scoped agent tools. Context handoff never writes the knowledge base.
//
// Hosts map save_* tools via AgentOptions.BrainWriteKinds. Write for retrieval:
// fill title and summary (and useful properties) so search and find_objects work.
//
// Graph nodes are live, not static: every parent Put dual-writes again (updated
// title, summary, properties, search_text, embedding). SoftDelete removes the
// graph node. Revive via Put recreates it.
//
// # Postgres vs Helix (complementary)
//
// Postgres is the source of truth and document corpus:
// full rows, parts/chunks, BM25 + dense hybrid search, property filters, soft-delete,
// containment (parent_id). Tools: search, find_exact, read; expand children.
// search may pass ScopeIDs to limit hits to a parent neighborhood.
//
// Helix holds first-class entity nodes and cross-object edges only:
// dual-written parents (not chunks), EntityIndexText (title, summary, properties,
// capped content) as search_text + embedding, Link edges (email→deal, deal→buyer).
// Tools: find_objects (after Bootstrap on Helix), expand with relation_types, link.
//
// Graph-first then Postgres drill-down:
//
//	find_objects / expand(graph) → Helix ids → hydrate from Postgres
//	expand() containment → Postgres ListChildren (chunks)
//	read / search → Postgres
//
// Embeddings: WithEmbedder on NewEngine. Parents embed EntityIndexText; parts embed
// IndexText with parent title prefix (corpus only). One embedding dimension per process.
// Helix hosts must call helixgraph.Graph.Bootstrap (or EnsureSearchIndexes) so
// HasObjectSearch is true; MemoryGraph is always ready when attached.
//
// # Boot sketch
//
//	store, err := brain.NewPostgresStore(pool)
//	g, err := helixgraph.New(helixURL)
//	if err := g.Bootstrap(ctx, false); err != nil { return err }
//	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithGraph(g))
//	if err := eng.ApplyKinds(ctx, specs...); err != nil { return err }
//
// Integration tests (skipped under -short / without Docker):
//   - PostgresStore: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/enterprise-dev (in-memory)
package brain
