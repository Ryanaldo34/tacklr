# Knowledge

The knowledge store (package `brain`) is a database the agent can search. The agent saves records, links them, and finds them later, in the same session or another. The agent searches when it needs a fact, so the store's content does not fill the context window.

It holds three things:

- **Records** of the kinds you define, such as `Deal`, `Person`, or `Note`.
- **Indexed files**: copies of workspace files, split into chunks, so that search finds them. The file stays where it is.
- **Session messages**: the messages that left a session's context window, so that the session can still search them.

## Set it up

For tests and development, use the in-memory store:

```go
note := brain.KindSpec{Kind: "Note", Description: "A fact worth keeping", IsParent: true}
eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(note))
ns, err := brain.ParseNamespace("org", "acme")

agent.Brain = eng
agent.SearchNamespace = ns
agent.BrainWriteKinds = []string{"Note"}
```

In production, use Postgres (`brain/postgres`) and, for links between records, an optional Helix graph (`brain/helixgraph`):

```go
emb := brain.Embedding{Embedder: myEmbedder} // your embedding client: Embed(ctx, texts) and Model()

opts := postgres.Options{EmbeddingDim: 1536, Embedding: emb}
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
err = postgres.Configure(cfg, opts) // sets what each connection needs
pool, err := pgxpool.NewWithConfig(ctx, cfg)
store, err := postgres.Open(pool, opts)
err = store.Setup(ctx, append(kinds, vfsindex.MountIndexKinds()...)...) // creates what is missing; run on each deploy

client, err := helix.NewClient(helixURL)
graph, err := helixgraph.Open(ctx, client, helixgraph.Options{
	Dimension:     1536,
	EdgeRelations: []string{"about", "has_contact"}, // relations whose notes search can rank
	Embedding:     emb,
})

eng, err := brain.NewEngine(store, brain.WithGraph(graph))
err = eng.LoadKindsFromStore(ctx)
```

- Tacklr has no embedding client. Without an `Embedder`, search ranks by words only.
- `EmbeddingDim` and `Dimension` must match your embedder. `Setup` fails when the existing vector column has another size.
- The store and the graph can share one `Embedder`. Each stores the model name with every vector and compares only vectors of the same model.
- The pool is yours, and `session/postgres` can share it. A transaction-mode pooler must support prepared statements; otherwise use a direct connection.

## Kinds

You define the kinds of record. A `KindSpec` names the kind and its fields:

```go
deal := brain.KindSpec{
	Kind:        "Deal",
	Description: "A sales opportunity",
	IsParent:    true,
	Fields: []brain.FieldSpec{
		{Name: "stage", Type: brain.FieldTypeString, Enum: []string{"open", "won", "lost"}, Required: true},
		{Name: "amount", Type: brain.FieldTypeNumber},
	},
}
```

- With no kinds registered, the store accepts any kind and any properties.
- With kinds registered, an unknown kind, an unknown property, or a missing required property fails the write.
- A parent kind (`IsParent`) is a record. A part kind (`IsPart`) is a piece of a record, such as a chunk of a file.
- Register kinds with `brain.WithKinds` or `Engine.ApplyKinds`.

## Namespaces and access

Every record belongs to a **namespace**: an ordered list of named values, such as `org=acme, team=west`.

- `AgentOptions.SearchNamespace` is the widest namespace the session can read or write. It is required with `Brain`, unless `SessionScope` is set. A tool call can narrow it, but cannot widen it.
- `AgentOptions.SessionScope` gives each session its own namespace and principals, for an agent that serves many tenants or users. The runtime calls it with the session's id when it builds the session's harness and when it deletes the session's messages. A child session's id starts with its parent's id, so the host can give a child its parent's scope.
- The first value is the **tenant**. A search reaches only records with the same tenant. Every store enforces the tenant with an index, so one tenant's data never ranks in another's search.
- A record from another namespace looks like a record that does not exist.
- A record with `Readers` is visible only to callers whose `AgentOptions.SearchPrincipals` (the user's and groups' ids, or the principals `SessionScope` gives) include one of them. Indexed files get their readers from the source's sharing, with `workspace.Config.IndexReaders`.

## The agent's tools

| Tool | Use it to |
|---|---|
| `search` | Find records, indexed files, links, and earlier messages. |
| `open` | Read a whole record by its id or an indexed file's path, with its kind's fields. |
| `related` | List a record's parts, its parent, and the records linked to it. |
| `save` | Write a record of a kind in `BrainWriteKinds`. Registered only when that list is not empty. |
| `link` | Add or remove a link between two records. Registered only with a graph. |
| `index_file`, `unindex` | Add a workspace file to the store, or remove it. Registered with a workspace. |

The agent usually works in this order: `search` to find candidates, `search` with a kind and filters to narrow, `open` or `related` to read and walk, then `read` on a file's path to see its current content.

## Search

`search` returns one ranked list of records and links.

- **Free text** searches every source at once: keywords and vectors in the store, and records and link notes in the graph. The lists are merged with reciprocal rank fusion.
- **A query in double quotes** matches the words exactly, without vectors or the graph.
- **A record id** returns that record.
- **A kind or filters with no query** lists the matching records, newest first.
- **`within`** searches only the parts of one record or indexed file.
- **`near`** searches only the records linked to one record, up to `depth` links away, for example "what in this deal's neighborhood mentions the budget". It needs a graph.

A file's matching chunks do not appear as results of their own. They are attached to their file's record as **evidence**: a snippet with its line numbers or heading, so the agent can `read` that part of the file.

Filters test the record. The filter keys are `kind`, `title`, `created_after`, `created_before`, `updated_after`, `updated_before`, and the kind's own fields. A wrong kind or filter fails with an error that lists the valid choices, so the model can correct its call. Each page says whether there is `more`, and `more: true` returns the next page of the tool's last call, also after the session restarts.

| Setting | Default | Option |
|---|---|---|
| Candidates from each source | 40 | `EngineConfig.CandidateK` |
| Page size | 10, at most 50 | `EngineConfig.DefaultLimit`, `MaxLimit` |
| Evidence on each result | 3 snippets of 240 characters | `EngineConfig.EvidenceN`, `SnippetCap` |
| Newer records rank higher | No | `brain.Ranking.Decay` on the store or the graph |

`brain.WithReranker` reorders the first candidates with your own reranker before the page is cut.

## Related records and links

`related` follows two kinds of connection:

- **Containment**: a record's parts, its parent, and the parts near a part (`child`, `parent`, `sibling`).
- **Links**: labeled edges in the graph, such as `Deal has_contact Person`, each with an optional note. `related` follows them up to 4 hops, and each result says which record it came through.

The agent can narrow a walk with `relations`, `direction` (`out` or `in`), `kind`, and a link `status`. The graph applies every condition before it fills a page, so a page holds only matching records. Results come nearest first, then newest first. `more` pages through the whole neighborhood, also after the session restarts, and `counts` give the total and the number for each relation, so the agent knows how much a page leaves out.

One hop follows at most `EngineConfig.FrontierK` records (default 1,000) to the next hop. When a hop reaches more, the result says `truncated`, and the agent should narrow the walk.

Hosts call `Engine.Related` with the same options and more: `Edges` filters links by status, role, confidence, and update time, and `Filters` takes every search filter.

`link` needs both records to exist in the namespace. It cannot link parts, and a file must be indexed before it can be linked. A link's note is indexed, so `search` finds the link by what the note says. With Helix, list the relation in `EdgeRelations` for its notes to rank.

Without a graph, `related` returns containment only, and the agent has no `link` tool.

## Indexed files

A workspace file is indexed when the agent calls `index_file`, or when your code calls `MountIndexer.IndexPath`. Nothing else indexes a file: a later write does not update the index.

```go
idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
result, err := idx.IndexPath(ctx, "/workspace/work/notes.md")
```

- The file becomes a `Document` record with its path in `vfs_path`, and `Chunk` parts of about 2 KiB. Markdown, Docs, and Sheets are split by heading or block.
- `IndexPath` compares the file's version with the stored one, and skips a file that did not change. After an edit, only the chunks that changed are embedded again.
- A file that no longer exists is removed from the index. A binary file is skipped.
- A member mounted with `.NotIndexed()` is never indexed.
- Do not also write a record as a file and index the file: search would return it twice.

Register the `Document` and `Chunk` kinds with `vfsindex.MountIndexKinds()` when you register kinds of your own.

## Session messages and specialist results

When a session's window collapses or hands off, the messages that leave it are saved as **session messages** first. They are in the session's own namespace (the host namespace plus `session=<id>`), so `search` in that session finds them and no other session does. Closing the session deletes them.

The result of a blocking specialist is saved as an `Episode` record, so later sessions in the namespace can find it. Register `tacklr.EpisodeKinds()` when you register kinds of your own.

## When something fails

| Situation | Result |
|---|---|
| The embedder fails while saving | The write fails and leaves nothing behind. A session message is saved without vectors. |
| The embedder fails while searching | The search ranks by words only. |
| The graph fails while searching | The error is logged and the store's results return, unless `EngineConfig.FailOnGraphError`. |
| The graph write fails after the store write | The record stays in the store, and the error wraps `brain.ErrIndexPut`. Save it again after you fix the graph. |
| Two writers re-index one file | The second gets `ErrConflict`, and `IndexPath` reads the file again. |
| A re-index fails part-way | The previous chunks stay until a new set commits. |
| The embedding model changes | Old vectors drop out of vector search, and the next re-index embeds those parts again. |

To delete data, `SoftDelete` hides a record, `PurgeDeleted` and `PurgeSessionMessages` delete hidden data, and `Erase` deletes everything under a scope from the graph first, then the store. `brain.WithAudit` records every read and write.

## Write a backend

A backend implements `brain.Store`, and optionally `brain.Graph`. The engine gives a backend whole requests, so a database with native hybrid search, filters, or embedding can use them. For a database that lacks a feature, `brain` has helpers:

| Missing feature | Helper |
|---|---|
| Batched embedding | `brain.Embedding.EmbedBatches` |
| Rank fusion and recency decay | `brain.Ranking.Fuse`, `FuseEdges` |
| Native filters | `Filter.Compile()`, a `FilterPlan` that matches in process or lists conditions to translate |
| Tenant and reader checks | `Scope.CanRead`, `Namespace.Covers` |

Prove a backend with the shared contract tests: `storetest.Run(t, newStore)` and `graphtest.Run(t, newGraph)`. The memory store, `brain/postgres`, and `brain/helixgraph` pass them.

## Reference: indexed file rows

Write indexed files through `MountIndexer.IndexPath` or `Engine.PutTree` when you can, because they embed the rows and update the graph. A pipeline outside Go must write the same rows:

| Document field | Value |
|---|---|
| `id` | `MountIndexer.DocumentID`: a UUID version 5 with namespace `a1b2c3d4-e5f6-7890-abcd-ef1234567890`, over the namespace string, a zero byte, and the virtual path |
| `kind`, `title`, `summary` | `Document`, the file's base name, the virtual path |
| `content_type` | The media type |
| `properties` | `vfs_path`, `source_version` (`FileInfo.Version`), `size`, `mtime` (RFC 3339), `media_type` |

| Chunk field | Value |
|---|---|
| `kind`, `parent_id`, `position` | `Chunk`, the document's id, the chunk's 0-based place in the file |
| `content` | The chunk's text, which search matches |
| `title` | Empty, or `<file>#<block id>` for a heading block |
| `properties` | `start_line`, `end_line` (1-based, inclusive), `byte_start`, `byte_end`, and `block_id` and `heading_path` for a block |

Chunk ids come from their content, which only `PutTree` can compute: a pipeline sends parts without ids.
