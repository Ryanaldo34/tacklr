// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Canonical architecture (Engrams, two jobs, search, graph, tools): docs/knowledge.md.
//
// # Public surface
//
// Hosts use Engine (NewEngine + options) over a Store implementation, and
// KnowledgeGraph (NewKnowledgeGraph(eng, g)) for the graph features, kind
// registration (ApplyKinds / WithKinds), and composition helpers (LandingIDs,
// ExpandMany, ExpandByRecipe, SortRichObjects). Engine sends every committed
// write to its indexes (Index); a graph is one, and WithIndex adds others.
// MemoryStore is an in-process store for tests and offline hosts. Durable
// backends are injected: brain/postgres.Store and brain/helixgraph.Graph (or
// any backend that passes brain/graphtest). Agent tools are registered by the harness when
// AgentOptions.Brain is set, and the graph tools when AgentOptions.Graph is.
//
// A store implements Store and passes brain/storetest; a graph implements
// Graph and passes brain/graphtest. Each method is one agent operation, and
// the backend owns its retrieval: its own Embedder, channels, ranking and
// fusion, filters, and the scope and readers rules. Engine owns orchestration
// and domain rules: validation, the kind catalog and the text that represents
// an object (Indexed), content-derived part ids, the store-then-graph write
// order, hydrating graph hits from the store, promotion, paging, reranking and
// audit. Backend authors can reuse Embedding (batched, bounded embedding with
// EmbedBatches for tree writes), Ranking (RRF fusion with recency decay) and
// CompileFilter / FilterValueKey (filter evaluation and translation).
//
// Hosts attach an Engine via AgentOptions.Brain. This package does not import
// postgres, helixgraph, the harness, session, or telemetry; Scope is passed
// in by the caller.
//
// Knowledge records are store rows. Hosts and save_* tools write them with Put.
// Search and find_exact retrieve them. They are not files on the VFS.
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
// Durable product objects are written via Engine.Put / PutTree / SoftDelete
// (host SDK) or kind-scoped agent tools. A plan handoff saves the exact live
// window with Engine.SaveSessionMessages into session_messages (host namespace
// plus the session id at the tail). Those rows are not the harness checkpoint.
// Search and find_exact return them together with host-namespace knowledge for
// the active session. Compress and specialist results may still
// write session residue (kind Episode). They never write host product kinds.
// PutTree is how a host attaches corpus chunks under a parent. It streams the
// parts, embeds only those whose content changed (part ids come from
// content), stages them in batches, and swaps them in with one atomic
// commit. Engram files stay parent-only.
//
// # Isolation
//
// Every Engine and store call takes a Scope with a non-empty Namespace; an
// empty one fails with ErrInvalid. The namespace's first attr is the tenant:
// a scope reaches only rows with the same tenant, and stores match it by
// equality before anything else. Writes are guarded: an object must lie
// inside the scope, and an id already held outside the scope returns
// ErrNotFound, so a tenant cannot overwrite another tenant's row. Objects with
// Readers (source ACLs) are visible only to scopes holding one of those
// Principals. PurgeDeleted, Erase, and PurgeSessionMessages hard-delete for
// retention and erasure. WithAudit records every served read and write.
// storetest.Run is the isolation contract a store must pass.
//
// Hosts map save_* tools via AgentOptions.BrainWriteKinds. Write for retrieval:
// fill title and summary (and useful properties) so search and find_objects work.
//
// Graph nodes are live, not static: every parent Put writes its graph node in place
// (edges preserved). SoftDelete removes the graph node first, then soft-deletes the
// store row. Revive via Put recreates the graph node.
//
// # Store vs graph
//
// The Store is the source of truth and the document corpus: full rows,
// parts/chunks, lexical + dense hybrid search, property filters, soft-delete,
// containment (parent_id). Search and FindExact query parts (title, summary,
// content) and promote hits to the parent. Tools: search, find_exact, read;
// expand children. search may pass ScopeIDs to limit hits to a neighborhood.
//
// The graph holds first-class parent nodes and cross-object edges only (not
// chunks). FindObjects queries that entity index (title, summary, indexed
// properties, body). Tools: find_objects, expand with relation_types, link,
// and find_links (edge note text and vector) on relations the graph indexes.
// Engine.ShortestPath returns the fewest-hop path through objects the scope
// can read.
//
// Graph nodes carry the object's namespace and Readers, and every node query
// carries the scope's namespace and Principals. The graph applies the same
// rule as Scope.CanRead while it ranks and walks: an object the caller cannot
// read takes no ranking slot, nor does an edge that touches it, and no walk or
// path goes through it.
//
// Helix owns native text/vector indexes, ranking, graph topology, edge props
// and traversal; its indexes are partitioned by tenant, so search ranks one
// tenant only. helixgraph embeds node text and edge notes, filters before it
// ranks, and fuses its lists itself; Engine hydrates full rows from the Store
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
// Index text: parents are represented by EntityIndexText, parts by IndexText
// with the parent title prefix (corpus only). WithIndexText rewrites that text
// at Put without changing the stored object. Each backend embeds it with its
// own Embedder (none: text only); a backend's vector dimension is its own
// option (postgres.Options.EmbeddingDim, helixgraph.Options.Dimension).
// A write whose text does not embed fails; a search whose query does not
// embed is answered by its words.
//
// # Boot sketch
//
//	emb := brain.Embedding{Embedder: myEmbedder}
//	store, err := postgres.Open(ctx, dsn, postgres.Options{EmbeddingDim: 1536, Embedding: emb})
//	defer store.Close()
//	if err := store.Setup(ctx, specs...); err != nil { return err }
//	client, err := helix.NewClient(helixURL)
//	g, err := helixgraph.Open(ctx, client, helixgraph.Options{Dimension: 1536, EdgeRelations: []string{"about"}, Embedding: emb})
//	eng, err := brain.NewEngine(store)
//	kg := brain.NewKnowledgeGraph(eng, g)
//	if err := eng.LoadKindsFromStore(ctx); err != nil { return err }
//
// Integration tests (skipped without Docker):
//   - postgres.Store: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/helixdb:v0.0.12 (in-memory)
package brain
