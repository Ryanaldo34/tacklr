// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Canonical architecture (Engrams, two jobs, search, graph, tools): docs/knowledge.md.
//
// # Public surface
//
// Hosts build one Engine (NewEngine) over a Store, add a Graph with
// WithGraph, and register kinds (ApplyKinds / WithKinds). The agent-facing
// operations are the five tools' methods: Search (and Next for more),
// Open, Related, Save and Link. Hosts also use PutTree, SoftDelete,
// PurgeDeleted, Erase, Get, Resolve and the session-message methods.
// MemoryStore is an in-process store for tests and offline hosts. Durable
// backends are injected: brain/postgres.Store and brain/helixgraph.Graph (or
// any backend that passes brain/storetest or brain/graphtest). The harness
// registers the tools when AgentOptions.Brain is set: search, open and
// related always, save for AgentOptions.BrainWriteKinds, and link when the
// engine has a graph.
//
// The engine brokers between the store and the graph. A missing graph is a
// missing source, not an error: Search and Related return the store's
// results, and only Link needs a graph (ErrUnsupported). Every committed
// write of a record goes to the store, then each Index (a graph is one;
// WithIndex adds others), then the audit record.
//
// A store implements Store and passes brain/storetest; a graph implements
// Graph and passes brain/graphtest. The backend owns its retrieval: its own
// Embedder, channels, ranking and fusion, filters, and the scope and readers
// rules. Engine owns orchestration and domain rules: validation, the kind
// catalog and the text that represents an object (Indexed), content-derived
// part ids, the write order, fusing the sources by reciprocal rank, reading
// every result from the store (the source of truth), promoting parts to
// their records, paging, reranking and audit. Backend authors can reuse
// Embedding (batched, bounded embedding with EmbedBatches for tree writes),
// Ranking (RRF fusion with recency decay) and Filter.Compile / FilterPlan
// (filter evaluation and translation).
//
// This package does not import postgres, helixgraph, the harness, session,
// or telemetry; Scope is passed in by the caller.
//
// # Search
//
// Search takes free text, an optional kind and filters, and returns one
// ranked list: records (with the parts that matched as evidence) and, with
// a graph and no kind or filters, links whose notes matched. The store ranks
// records and parts; the graph ranks objects and links; Engine fuses the
// lists and reads each result from the store. A UUID query returns that
// object; a query in double quotes is matched literally. Filters test the
// record (for a part, its parent). Each page carries facets (the kinds and
// common field values among its objects) and a Rest that Next pages
// through; a SearchContext keeps the last Rest of each tool, so a tool call
// with more continues it, across checkpoints too.
//
// Related returns a record's children (or a part's parent and nearby parts)
// and, with a graph, the records its relations reach, each tagged with how
// it is related. Open reads one object in full with its kind's fields.
//
// # Kind schemas (host migrations)
//
// Object kinds are host/user-defined for determinism. Register with ApplyKinds
// (or WithKinds). Agent-defined kinds are out of scope. The tools take the
// registered record kinds as a JSON-schema enum; errors on an unknown kind or
// filter name the valid choices.
//
// # Explicit writes (no handoff side effects)
//
// Durable product objects are written via Engine.Save / PutTree / SoftDelete
// (host SDK) or the save tool. A plan handoff saves the exact live window
// with Engine.SaveSessionMessages into session_messages (host namespace plus
// the session id at the tail). Those rows are not the harness checkpoint.
// Search returns them together with host-namespace knowledge for the active
// session. PutTree is how a host attaches corpus chunks under a parent. It
// streams the parts, embeds only those whose content changed (part ids come
// from content), stages them in batches, and swaps them in with one atomic
// commit.
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
// Principals, in the store and in the graph: an object the caller cannot read
// takes no ranking slot, nor does an edge that touches it, and no walk goes
// through it. PurgeDeleted, Erase, and PurgeSessionMessages hard-delete for
// retention and erasure; Erase erases the indexes by scope first, so a
// retry finishes an erase that failed part way. WithAudit records every
// served read and write.
//
// Index text: parents are represented by EntityIndexText, parts by IndexText
// with the parent title prefix. WithIndexText rewrites that text at Save
// without changing the stored object. Each backend embeds it with its own
// Embedder (none: text only); a backend's vector dimension is its own option
// (postgres.Options.EmbeddingDim, helixgraph.Options.Dimension). A write
// whose text does not embed fails; a search whose query does not embed is
// answered by its words.
//
// # Boot sketch
//
//	emb := brain.Embedding{Embedder: myEmbedder}
//	store, err := postgres.Open(ctx, dsn, postgres.Options{EmbeddingDim: 1536, Embedding: emb})
//	defer store.Close()
//	if err := store.Setup(ctx, specs...); err != nil { return err }
//	client, err := helix.NewClient(helixURL)
//	g, err := helixgraph.Open(ctx, client, helixgraph.Options{Dimension: 1536, EdgeRelations: []string{"about"}, Embedding: emb})
//	eng, err := brain.NewEngine(store, brain.WithGraph(g))
//	if err := eng.LoadKindsFromStore(ctx); err != nil { return err }
//
// Integration tests (skipped without Docker):
//   - postgres.Store: Testcontainers + brain/testdata/Dockerfile.postgres
//   - helixgraph: Testcontainers + ghcr.io/helixdb/helixdb:v0.0.12 (in-memory)
package brain
