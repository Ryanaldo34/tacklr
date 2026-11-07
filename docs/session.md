# Sessions in production

This guide covers running sessions for real users: the Temporal client and worker, the Postgres stores, credentials, live streaming, specialists and jobs, and Durable Objects. Read [Concepts](concepts.md) first.

## Two kinds of process

A deployment on Temporal has two programs, which scale on their own:

```go
// Host: serves your clients. It does not link the agent.
sessions := temporal.Open(c, temporal.Config{Store: store, Secrets: secrets, Live: temporal.Live{Key: liveKey}})

// Worker: runs the sessions.
w := worker.New(c, worker.Config{
	Agent: agent, Store: store, Secrets: secrets,
	Live: worker.Live{URL: "https://worker-1.internal:8081", Key: liveKey},
})
err := w.Start() // Stop it on shutdown; Run(interruptCh) blocks instead
```

- A **host** imports `session/temporal` and the protocol server. It keeps no session state.
- A **worker** imports `session/temporal/worker` and your agent, workspace sources, and tools. Each session is one Temporal workflow (`SessionWorkflow`), and each model request and tool call is an activity. The worker registers them; do not register them yourself.
- Both use the same `Store`, `Secrets` and `TaskQueue`. In-memory stores work only when both run in one process.

A session's steps go to the worker that already holds its harness (its tools, MCP connections, and workspace). If that worker is gone, another worker builds the harness again and continues.

[examples/temporal](../examples/temporal) is a complete deployment of both programs.

## Stores

Package `session/postgres` keeps every store in Postgres, over a pool you own:

```go
db, err := sessionpg.New(pool, sessionpg.Options{}) // schema "tacklr"
err = db.Setup(ctx)                                  // creates what is missing; run on each deploy
sealer, err := session.NewAESGCM(key)                // 32 bytes, or your own session.Sealer over a KMS
store, secrets, events := db.Store(), db.Secrets(sealer), db.EventLog()
```

| Store | Holds |
|---|---|
| `session.Store` | Each session's checkpoint (its context window, plan, state, and permission answers) and its tool results. A save writes only the rows that changed. |
| `session.SecretStorage` | The credentials users send, sealed with the session id, so one session's row cannot be opened as another's. |
| `session.EventLog` | The session's durable events, for clients that subscribe. Use it with `DisableStreams` when you do not use Temporal's Workflow Streams. |

To use your own database, implement the interfaces, and run the shared contract tests in `session/storetest`.

## Credentials

Credentials belong to the work, not to the connection. A client sends them with each prompt:

| Field | Carries |
|---|---|
| `CreateSession.Mounts` | Recipes for the user's folders: provider, name, and folder ids. No secrets. |
| `Prompt.Auth`, `Resume.Auth` | Access tokens, and folders to add or drop. |

The client puts tokens in `SecretStorage` before it signals the session, so tokens are never in Temporal history, the checkpoint, or the protocol's wire store. A worker reads them when it builds the session's harness, and a child session falls back to its parent's. `Close` deletes them.

`SecretStorage` holds only these session credentials, such as a user's Google Drive token. Your own service credentials stay in the sources and tools you build.

When a token expires during a turn, the step fails on its first attempt, and the client sends a new token with the next prompt. For MCP servers, inline `Env` and `Headers` are never stored; a `CredentialRef` is resolved by `AgentOptions.MCPCredentialResolver` when the server connects.

For data that is not a credential (prompt text, tool arguments), use a Temporal payload codec if you need it encrypted at rest.

## Live streaming

The model runs on a worker, so clients get the text as it streams only when they can reach that worker. Set the same HMAC key on both sides:

- `worker.Config.Live`: the URL where you serve `Worker.LiveHandler()` (with your own TLS), and the key.
- `temporal.Config.Live`: the key.

`Subscribe` asks the session for its worker's URL and streams the deltas from there. When the session moves to another worker, it asks again. Without `Live`, clients get whole messages only.

## Prompts, cancel, and close

- **A prompt during a turn** waits in a queue. It enters the window between rounds, never between a tool call and its result. Over ACP, a second `session/prompt` joins the running turn, and the turn's end answers both.
- **`Prompt.Auth` and `Prompt.State`** apply at once. New MCP servers apply when the harness is built next.
- **`Cancel`** stops the turn and the session's children, and drops queued prompts. A commit that is running finishes first. The session stays open for the next prompt.
- **A client that disconnects** cancels nothing. The turn goes on, and a client that loads the session follows it again.
- **`Close`** stops and deletes the children, deletes the session's checkpoint, credentials, and saved messages, and ends the session.

## Specialists and jobs

A specialist or a job runs as a **child session**. `Client.Jobs` lists a session's children, `Status` reports one, and `Cancel` and `Close` reach them too.

| | Specialist | Job |
|---|---|---|
| Is | An agent from `AgentOptions.Specialists` | A `session.JobHandler` from the worker's `Config.Jobs` |
| Started by | The model, with `spawn_specialist` | A tool, with `CallContext.Schedule` |
| Session id | `{parent}/w/{specialist}/{call}` | `{parent}/j/{name}/{call}` |

- **Blocking** (`spawn_specialist` with `block`, the default, or `Schedule` with `Wait`): the call waits for the child, and the child's output is the call's result.
- **Not blocking**: the call returns at once. When the child ends, its result enters the parent's window as a new message. The turn does not complete while children run.
- **A child that fails** does not fail the parent. The parent gets the failure as the child's result.
- **A question in a child** pauses the child, not the parent. Resume it on the child's id.
- A specialist gets its own workspace on its own session id. It does not share the parent's tools: list them on `Specialist.Tools`.

## Retries and idempotency

The runtime retries a step that fails with `tacklr.ErrNetwork` (a transport failure, or an upstream 408, 429, or 5xx), a model refusal, or a stale checkpoint. Any other error stops the step on its first attempt.

A step can run more than once before the runtime records it, for example when a worker dies. Model requests and tool calls change nothing that a later step reads, so this is safe for them. A tool with a side effect outside Tacklr should use `CallContext.CurrentToolCallID` as its idempotency key.

## Temporal settings

| `temporal.Config` | Default | Effect |
|---|---|---|
| `ActivityTimeout` | 10 minutes | The longest one step may run. A tool's `Timeout` plus a minute replaces it when longer. |
| `HeartbeatTimeout` | 30 seconds | How long a step may go without a heartbeat. |
| `ActivityAttempts` | 3 | How many times a step runs. 1 turns retries off. |
| `MaxConcurrentToolCalls` | 32 | The tool calls of one reply that run at once. |
| `MaxChildren` | 1,000 | The children one session runs at once. |
| `WorkerQueueTimeout` | 10 seconds | How long a step waits for the worker that holds its harness before it goes to another worker. |
| `ContinueAsNewEvents` | Temporal's suggestion | The history size at which a waiting session continues as new. |

| `worker.Config` | Default | Effect |
|---|---|---|
| `Workspace`, `FuseRoot` | none | The agent's workspace and shell. See [Workspace](workspace.md). |
| `Jobs` | none | The jobs tools can start, by name. |
| `MaxHarnesses` | 256 | The harnesses one worker keeps. The one unused longest closes first. |
| `HarnessIdle` | 10 minutes | How long an unused harness stays open. |
| `MaxConcurrentActivityExecutionSize` | Temporal's | The steps one worker runs at once. |
| `HeartbeatThrottle` | 2 seconds | The longest a cancel takes to reach a running step. |

Temporal limits one payload to 2 MB by default, and model replies are payloads. For large replies, configure Temporal's external storage (`client.Options.ExternalStorage`) on the client you pass to both `temporal.Open` and `worker.New`.

## Durable Objects

Package `session/durableobject` runs each session as a durable object: an object with its own ordered storage, an alarm, and an address. It needs no Temporal. The session's steps run inside its object, and a journal in the object's storage makes each step run once: an object that stops replays its journal and continues.

The package is plain Go and builds for WebAssembly. A platform implements `Host` (an object's storage, alarm, and neighbors) and `Platform` (reaching objects by id). `durableobject.NewClient` is the `session.Client`, and `dotest.Platform` runs objects in memory for tests.

```go
obj := durableobject.NewObject(host, durableobject.Config{Agent: agent, Sealer: sealer, Jobs: jobs})
sessions := durableobject.NewClient(platform)
```

| `durableobject.Config` | Default | Effect |
|---|---|---|
| `MaxToolCalls` | 32 | The tool calls of one reply that run at once. |
| `StepAttempts` | 3 | How many times a step runs. |
| `RetryBackoff` | 1 second | The first wait between attempts. It doubles, up to 30 seconds. |
| `Watchdog` | 1 minute | The alarm that restarts an object that stopped during a step. |

A durable object has no FUSE and no shell, so its agent has no workspace.

### Cloudflare Workers

Package `session/durableobject/cloudflare` runs each session as a SQLite-backed Durable Object, and serves ACP from a connection object for each WebSocket. It is the only package with a build tag (`js && wasm`).

Your Worker's `main` calls `cloudflare.Serve` with the two Durable Object bindings, the `durableobject.Config`, and the same `server.Server` that a host on net/http runs. Your Worker entry imports `shim.js` from the package directory, with Go's `wasm_exec.js` and the compiled program.

- One Go program serves every object in an isolate. At most `App.MaxIdleObjects` (64) idle objects stay in memory; another one starts again from its storage.
- A value is stored in 1 MiB parts, because a row holds at most 2 MB.
- Objects reach each other through their stubs, inside Cloudflare's network.
- The example's program is about 17 MB (4 MB compressed). Workers allow 64 MiB.

[examples/cloudflare](../examples/cloudflare) is a complete Worker with an end-to-end test.

## Runtimes compared

| | Temporal | Durable Objects |
|---|---|---|
| A session | A workflow | An object |
| A step | An activity on a worker | A journaled call in the object |
| A child session | A child workflow | A child object |
| A question | A `Resume` signal | A mailbox wake |
| Durable events | Workflow Streams, or an `EventLog` | The object's event log |
| Credentials | `SecretStorage` | Sealed in the object's storage |
| Workspace and shell | Yes, on Linux workers | No |
