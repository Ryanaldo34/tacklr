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
// Graph nodes are live, not static: every parent Put dual-writes node props in place
// (edges preserved). SoftDelete removes the graph node first, then soft-deletes the
// store row. Revive via Put recreates the graph node.
//
// # Postgres vs Helix (complementary)
//
// Postgres is the source of truth and document corpus:
// full rows, parts/chunks, BM25 + dense hybrid search, property filters, soft-delete,
// containment (parent_id). Tools: search, find_exact, read; expand children.
// search may pass ScopeIDs to limit hits to a parent neighborhood.
//
// Helix holds first-class entity nodes and cross-object edges only (not chunks).
// Helix owns: native text/vector indexes, $distance ranking, graph topology, edge
// props, BothE neighbor walks, optional tenant indexes on namespace_id.
// Tacklr does not reimplement BM25/HNSW or in-process neighbor indexes for Helix.
// We dual-write searchable props (EntityIndexText + embedding), Link edges, fuse
// Helix text+vector channels with RRF (Helix has no single hybrid op), then hydrate
// full rows from Postgres under Scope.
//
// Tools: find_objects (after Bootstrap), expand with relation_types, link.
// Optional: helixgraph.EnsureEdgeTextIndex(rel) + SearchEdgesText for note search
// on a known relation label.
//
// GraphRAG-style composition (host-agnostic):
//
//	find_objects (entity land; filters via schema filterable_fields)
//	  or search/find_exact (corpus) → LandingIDs / LandingIDsFromPage (parent promote)
//	→ expand / ExpandMany (max_hops, direction, WantContainment)
//	  or ExpandByRecipe (host-named ExpandRequest template) → hydrate Postgres
//	→ optional find_links (edge text) for relationship-first land
//	→ search(scope_ids=…) for neighborhood corpus
//	→ optional host Reranker after hydrate; SortRichObjects for peer ordering
//
// Graph-first then Postgres drill-down:
//
//	find_objects / expand(graph) → Helix ids → hydrate from Postgres
//	expand() containment → Postgres ListChildren (chunks)
//	read / search → Postgres
//
// schema() returns filter_usage.tools listing search, find_exact, and find_objects
// so agents know filterable_fields apply to entity find as well as corpus search.
//
// Embeddings: WithEmbedder on NewEngine. Parents embed EntityIndexText; parts embed
// IndexText with parent title prefix (corpus only). One embedding dimension per process.
// Helix hosts must call helixgraph.Graph.Bootstrap (or EnsureSearchIndexes) so
// HasObjectSearch is true; MemoryGraph is always ready when attached.
// Bootstrap(true) enables Helix tenant filtering when the image supports it.
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
