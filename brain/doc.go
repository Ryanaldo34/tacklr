// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Canonical architecture (Engrams, two jobs, search, graph, tools): docs/knowledge.md.
//
// # Public surface
//
// Hosts use Engine (NewEngine + options), a Store implementation, an optional
// graph via WithGraph, kind registration (ApplyKinds / WithKinds), and
// composition helpers (LandingIDs, ExpandMany, ExpandByRecipe, SortRichObjects).
// MemoryStore and MemoryGraph are in-process backends for tests and offline
// hosts. Durable backends are injected: brain/postgres.Store and
// brain/helixgraph.Graph. Agent tools are registered by the harness when
// AgentOptions.Brain is set — they call Engine methods only.
//
// Graph backend packages implement GraphReader / GraphWriter /
// GraphObjectSearcher / GraphEdgeSearcher. Dual-write property keys and Helix
// schema details stay inside those packages.
//
// Hosts attach an Engine via AgentOptions.Brain. This package does not import
// postgres, helixgraph, the harness, session, or telemetry; Scope is passed
// in by the caller.
//
// # Engrams as files (vfs.Provider)
//
// brain.Open returns a vfs.OpenFunc so first-class objects appear as Markdown + YAML
// files (vfs imports stay one-way: this package imports vfs). Layout is host-chosen:
// mode=prefix (default /engram/<kind-slug>/<slug>.md) or mode=roots (/deal/acme.md).
// Kind names are host KindSpecs and must be path-safe (no '/' or '..'). Only parent
// kinds are directories; parts/chunks are not files. Write/Close/PutFile parse,
// validate, and Put (fail closed). Rename is delete+create. Graph edges stay in
// the graph backend and show up through path-native link/expand/find_links — not
// sidecar files.
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
// Durable objects are written only via Engine.Put / SoftDelete / ReplaceParts
// (host SDK) or kind-scoped agent tools. Context handoff never writes the
// knowledge base. ReplaceParts is how a host attaches corpus chunks under a
// parent; Engram files stay parent-only.
//
// Hosts map save_* tools via AgentOptions.BrainWriteKinds. Write for retrieval:
// fill title and summary (and useful properties) so search and find_objects work.
//
// Graph nodes are live, not static: every parent Put dual-writes node props in place
// (edges preserved). SoftDelete removes the graph node first, then soft-deletes the
// store row. Revive via Put recreates the graph node.
//
// # Store vs graph
//
// The Store is the source of truth and the document corpus: full rows,
// parts/chunks, BM25 + dense hybrid search, property filters, soft-delete,
// containment (parent_id). Search and FindExact query parts (title, summary,
// content) and promote hits to the parent. Tools: search, find_exact, read;
// expand children. search may pass ScopeIDs to limit hits to a neighborhood.
//
// The graph holds first-class parent nodes and cross-object edges only (not
// chunks). FindObjects queries that entity index (title, summary, indexed
// properties, body). Tools: find_objects (after graph Bootstrap), expand with
// relation_types, link. Optional: helixgraph.EnsureEdgeTextIndex(rel) +
// SearchEdgesText for note search on a known relation label.
//
// Helix owns native text/vector indexes, $distance ranking, graph topology,
// edge props, BothE neighbor walks, optional tenant indexes on namespace.
// Tacklr does not reimplement BM25/HNSW or in-process neighbor indexes for Helix.
// Dual-write searchable props (EntityIndexText + embedding), Link edges, fuse
// graph text+vector channels with RRF, then hydrate full rows from the Store
// under Scope.
//
// Query strings are free text. Structured fields go in Filters. The same
// filter keys work on search and find_objects; search applies them to the
// part row, find_objects to the parent.
//
// GraphRAG-style composition (host-agnostic):
//
//	find_objects (entity land; filters via schema filterable_fields)
//	  or search/find_exact (corpus) → LandingIDs (parent promote)
//	→ expand / ExpandMany (max_hops, direction, WantContainment)
//	  or ExpandByRecipe (host-named ExpandRequest template) → hydrate Store
//	→ optional find_links (edge text) for relationship-first land
//	→ search(scope_ids=…) for neighborhood corpus
//	→ optional host Reranker after hydrate; SortRichObjects for peer ordering
//
// Graph-first then store drill-down:
//
//	find_objects / expand(graph) → graph ids → hydrate from Store
//	expand() containment → Store.ListChildren (chunks)
//	read / search → Store
//
// schema() returns filter_usage.tools listing search, find_exact, and find_objects
// so agents know filterable_fields apply to entity find as well as corpus search.
//
// Embeddings: NewEngine requires WithEmbedder or WithLexicalOnly. Parents embed
// EntityIndexText; parts embed IndexText with parent title prefix (corpus only).
// WithIndexText rewrites that document at Put without changing the stored object.
// One embedding dimension per process.
// Helix hosts must call helixgraph.Graph.Bootstrap (or EnsureSearchIndexes) so
// HasObjectSearch is true; MemoryGraph is always ready when attached.
// Bootstrap(true) enables Helix tenant filtering when the image supports it.
//
// # Boot sketch
//
//	store, err := postgres.New(pool)
//	store.EmbeddingDim = 1536 // optional; default 1536
//	if err := store.Setup(ctx, specs...); err != nil { return err }
//	g, err := helixgraph.New(helixURL)
//	if err := g.Bootstrap(ctx, false); err != nil { return err }
//	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithGraph(g))
//	if err := eng.LoadKindsFromStore(ctx); err != nil { return err }
//
// Integration tests (skipped under -short / without Docker):
//   - postgres.Store: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/enterprise-dev (in-memory)
package brain
