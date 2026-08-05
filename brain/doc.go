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
// Write objects for retrieval: good title and summary (and content) so corpus
// search and find_objects have signal.
//
// # Postgres vs Helix (complementary)
//
// Postgres is the object source of truth and corpus hybrid search:
// containment (parent_id), BM25, pgvector, trigram, property filters, soft delete.
// Agent tools: search, find_exact, read; expand children/neighborhood.
// All structured content filters belong on search (no separate filtered-search tool).
// search may pass ScopeIDs to restrict hits to a parent neighborhood after expand/find_objects.
//
// Helix is the relationship graph and entity-shaped object find:
// non-containment edges, expand graph mode, dual-written node props, and
// (when indexes are ensured) TextSearchNodes / VectorSearchNodes for FindObjects.
// That is not a second copy of corpus BM25 over documents.
//
// On Put, when WithGraph provides a GraphWriter: dual-write a searchable graph
// node (props + embedding + timestamps via IndexText). Engine.Link writes edges
// for expand; HasGraphWriter gates the agent link tool. HasObjectSearch gates
// find_objects when the graph implements GraphObjectSearcher.
// Embeddings: when WithEmbedder is set, Put embeds IndexText and fails closed on
// embed errors; the same vector feeds Postgres hybrid search and Helix node indexes.
// Hosts should call helixgraph EnsureSearchIndexes so FindObjects uses native APIs.
// Temporal ranking (λ decay) is owned by the Engine after candidate fusion;
// updated_at/created_at are stored on objects (and Helix nodes) for filters/sort.
//
// Long-running workflows may issue many tool calls; each call should pick the
// right path (search vs find_objects vs expand vs save/link).
//
// # Boot sketch
//
//	store, err := brain.NewPostgresStore(pool)
//	g, err := helixgraph.New(helixURL)
//	_ = g.EnsureSearchIndexes(ctx) // object_id + search_text + embedding indexes
//	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithGraph(g))
//	if err := eng.ApplyKinds(ctx, specs...); err != nil { return err }
//
// Integration tests (skipped under -short / without Docker):
//   - PostgresStore: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/enterprise-dev (in-memory)
package brain
