# Context window

This guide tells how Tacklr keeps the model's context window under its limit. It covers the data, the parts that own the data, and each step from a prompt to a saved checkpoint.

**Related:** [session.md](session.md) (rounds, steps and runtimes), [knowledge.md](knowledge.md) (the Brain that keeps the history that leaves the window).

## Rules

- The window holds **complete rounds** only. A round is one user message, or one model reply and a result for each call that it made. The model never sees a call without its result.
- The window changes only when a round commits, or when a plan install or a handoff replaces it. A model request (`Infer`) does not change it.
- The size of a request comes from the provider. Tacklr does not count tokens with a local tokenizer.
- The window is the only owner of its messages. Other parts read a copy, or tell the window to change.

## Parts and what they own

| Part | Owns | Converts |
|------|------|----------|
| `window` (`window.go`) | The committed messages. The **count**: the provider's input token count of the last request, and how many messages that request carried. The rules: the size (`MaxWindowSize`), the pressure (85% of the size), the protected prefix (the first user message and the plan document), and whether the provider can count. | Messages to checkpoint rows, and back. Messages and the count to the size of a request. |
| `Harness` | The parts of a request that are not messages: the system prompt, the tools and the cache key. The model tasks that write a summary or a handoff. It is the window's host (`windowHost`). | Messages to a `ModelRequest` (`request`). Messages that leave the window to session messages (`archive`). A full tool result to an Overflow file (`keep`). |
| `ModelRequest` | One request: messages, tools, system prompt, cache key. | Itself to its encoded size in bytes (`encodedSize`). |
| `Model` (for example `openai`) | The provider's wire format. | A `ModelRequest` to the provider's request. The provider's stream to `LLMResponseChunk`s, with the reported input tokens. A provider error to a sentinel error (`ErrCannotCount`, `session.ErrContextLength`). |
| `Reply`, `Round`, `Call` | One reply and its calls with their results, from the model request to the commit. A `Reply` carries the provider's count of its request (`InputTokens`). | They are the data between steps. A runtime keeps them as JSON in its journal. |
| `loop.Turn` | The order of the steps. One retry after a refused request. | A step's error to the turn's `FailKind`. |
| Brain | The messages that left the window (session messages). | Messages to searchable `SessionMessage`s. |
| Overflow | The full text of results that were cut. | A result to a file at `<OverflowPath>/<session>/<call id>.txt`. |

The window uses the host for three things that it cannot own: the parts of a request that are not messages, model calls, and the stores for data that leaves it. The host does not change the window.

## Flow

```
prompt ──► Commit{Users} ──► window.Fit ──► window
                                               │
   Infer ◄── Harness.request(window) ──────────┘
     │          └─► Model: the provider's request ──► provider
     │                                                   │
     │  ◄── chunks: deltas, items, input tokens ◄────────┘
     ├─ deltas ──► live readers (not stored)
     └─ Reply (messages, calls, InputTokens)
           └─► Round ──► one Tool step for each call
                            └─► Commit{Round} ──► window.Fit(round, InputTokens)
                                                     or a window effect
                                                         │
                       window.rows() ◄───────────────────┘
                            └─► checkpoint rows ──► Snapshot diff ──► session.Store
```

1. **A prompt commits.** `Commit` gives the user message to `window.Fit` as a round of its own.
2. **The model is asked.** `Infer` asks `Harness.request` for a request on the window. The `Model` converts it to the provider's format and sends it. As the reply streams back, deltas go to the readers that listen now, and each complete item becomes a message. The provider's input token count goes on the `Reply`. The window does not change.
3. **The calls run.** Each call runs as a Tool step, and its result goes on its `Call`.
4. **The round commits.** When each call has a result, `Commit` applies each tool's state writes in call order. Then it gives the round and the reply's `InputTokens` to `window.Fit`. The count is of the request on the window as it is before the round, so the window keeps it with that message count.
5. **The checkpoint is saved.** The runtime saves the checkpoint rows that changed. The window gives its own rows: one row for each message, keyed by its position, and one row for the count. An append writes only the new rows.

## Size of a request

The window uses three sources, in this order:

1. **The bound.** The window adds the count to the encoded size in bytes of the messages after the ones it counted. When there is no count, the bound is the encoded size of the full request (`ModelRequest.encodedSize`). A byte-level tokenizer gives each token one byte or more, so the bound is never less than the real count. When the bound is under the pressure, the round is appended, and the provider is not asked.
2. **The provider's count.** Over the pressure, `Model.CountTokens` asks the provider for an exact count (for `openai`, `POST /responses/input_tokens`).
3. **The bound alone.** When `CountTokens` fails with `tacklr.ErrCannotCount` (for `openai`, a provider without the count endpoint, for example Azure OpenAI or a local server), the window stops asking and uses the bound for the rest of the harness's life. The bound is high for text (about four bytes for each token), so such a window collapses and cuts results earlier.

The count stays valid while the window only grows, because the request that it counted is the start of each next request. A collapse, a plan install or a handoff replaces the window, and the window forgets the count.

## Fit

`window.Fit` puts a round into the window:

1. **Append.** When the bound is under the pressure, the round is appended.
2. **Collapse.** When the size is over the pressure, everything after the protected prefix (an earlier summary also) leaves the window. The host archives it, then the model writes one summary of it. The summary replaces it, and the round goes after the summary. With a Brain, the summary tells the agent that `search` finds the earlier messages and that `open` reads one. The window is then small, so the next rounds append without a model call.
3. **Cut.** When the round is still over `MaxWindowSize`, its largest tool results are cut on a rune boundary until the request fits. Before the first cut of a result, the host keeps its full text in Overflow, when there is one. The cut result ends with a note that tells how much is left, and the path to `read` with `start` and `end`, or that the full text was not kept. A round with no result to cut fails the commit.

A commit that fails (a count, the archive, the summary request, the Overflow write, or a round that cannot fit) fails its step, and the step saves nothing.

## Window effects

A round with a window effect goes in as it is, without a fit, because the effect then replaces the window:

- **Plan install** (`create_plan`): the window becomes the first user message and the plan document.
- **Handoff** (`complete_todo` while todos are open): the host archives the window after the first user message. The model writes a handoff of the window. The window becomes the first user message, the plan document, the handoff, and a nudge to continue. When the model call fails or writes nothing, the handoff is made from the plan.

## A request that the provider refuses

The bound can be low when a message has content that is larger in tokens than in bytes, for example a file reference. When the provider refuses a request as over its context window (`session.ErrContextLength`):

1. The turn commits a collapse (`CommitIn{Collapse: true}`, which runs `window.Refit`).
2. `Refit` summarizes the messages after the protected prefix that the last counted request carried. The provider accepted that request, so the summary request fits. The messages after them stay. When there is no count, `Refit` summarizes everything after the prefix.
3. The model is asked again, one time.

A second refusal, or a window with nothing to collapse, fails the turn with `FailKind` `context_length`. `ErrContextLength` is also `ErrMaxTokens`, so a client that checks for the token limit also finds it.

## Checkpoint and restore

- The window is in the checkpoint as `message` rows and one `count` row (schema version 7).
- `RestoreCheckpoint` gives the window its rows, and the window loads its messages and count from them. A restored session, for example a Durable Object after eviction, sizes its next commit from the saved count.
- A runtime builds a new harness when the session's MCP servers change. The system prompt and the tools are therefore fixed for the life of one harness, and the part of the count that they make stays correct.
- The round in progress (the reply and its calls) is not in the checkpoint. The runtime keeps it in its journal, with the reply's count.

## What a provider client does

A `Model` must:

- Count a request with the provider's API in `CountTokens`, or fail with an error that wraps `tacklr.ErrCannotCount`.
- Report each response's input tokens on a chunk (`LLMResponseChunk.InputTokens`).
- Report a request that the provider refused as over the context window with `session.ErrContextLength`.

It must not count with a local tokenizer.

## Configuration

| Option | Effect |
|--------|--------|
| `AgentOptions.MaxWindowSize` | The window's size in tokens. Zero uses the model's `MaxContextWindow`. |
| `AgentOptions.Brain` | Keeps the messages that leave the window as session messages, which this session can search. |
| `AgentOptions.Overflow` | Keeps the full text of results that are cut (the workspace's `workspace.Config.OverflowPath`). |
