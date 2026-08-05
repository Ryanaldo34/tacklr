// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Hosts attach an Engine via AgentOptions.Brain. This package does not import
// the harness, session, or telemetry packages; Scope is passed in by the caller.
// Optional Observer (telemetry.NewBrainObserver) records ops without domain coupling.
//
// SearchContext is the retrieval session surface: host namespace + active ResultSet
// for continue (replaced on each search, find_exact, or large expand).
//
// # Kind schemas (host migrations)
//
// Object kinds are host/user-defined for determinism. Register with ApplyKinds
// (or WithKinds). Agent-defined kinds are out of scope.
//
// When the process catalog is non-empty:
//   - property filters require a kind filter
//   - search/find_exact only consider registered kinds
//   - schema() prefers the catalog
//   - Put validates properties against the kind (ValidateObject)
//
// # Explicit writes (no handoff side effects)
//
// Durable objects are written only via Engine.Put / SoftDelete (host SDK) or
// kind-scoped agent tools that call the same path. Context handoff never writes
// the knowledge base. There is no offline "dream" writer in the core path.
//
//	eng.Put(ctx, scope, brain.Object{Kind: "Discovery", Title: "...", NamespaceID: ns, ...})
//
// Hosts map tool roles to kind names via AgentOptions.BrainWriteKinds
// (save_discovery / save_fact / save_memory); the package does not auto-register kinds.
//
// # Postgres vs Helix (complementary)
//
// Postgres is the object source of truth and corpus hybrid search:
// containment (parent_id), BM25, pgvector, trigram, property filters, soft delete.
//
// Helix is the relationship graph and graph-contextual search:
// non-containment edges, expand graph mode, node/edge text and vector indexes
// for entity-centric recall (not a second copy of corpus BM25).
//
// On Put, when WithGraph provides a GraphWriter: dual-write a searchable graph
// node (props + embedding + timestamps via IndexText). Engine.Link writes edges
// for expand; HasGraphWriter gates the agent link tool.
// Embeddings: when WithEmbedder is set, Put embeds IndexText and fails closed on
// embed errors; the same vector feeds Postgres hybrid search and Helix node indexes.
// Graph-contextual text/vector search on Helix is not yet a first-class Engine API
// (expand + corpus search cover the current agent surface).
//
// Temporal ranking (λ decay) is owned by the Engine after candidate fusion;
// updated_at/created_at are stored on objects (and Helix nodes) for filters/sort.
//
// # Boot sketch
//
//	store, err := brain.NewPostgresStore(pool)
//	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb) /* optional */)
//	if err := eng.ApplyKinds(ctx, specs...); err != nil { return err }
//
// Integration tests (skipped under -short / without Docker):
//   - PostgresStore: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/enterprise-dev (in-memory)
package brain
