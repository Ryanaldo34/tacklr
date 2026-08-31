# Durable runtime

Tacklr’s session API is `durable.Runtime`. A `server.Protocol` maps wire frames to Runtime calls. ACP is the native implementation (`NewACPProtocol`); hosts implement `Protocol` for their own streaming and delivery. Runtime does not import protocol types. Autonomous workflows call Runtime directly.

## Vocabulary

| Word | Meaning |
|------|---------|
| **Session** | Long-lived wait loop (`CreateSession` … `Close`) |
| **Turn** | One `Prompt` or `Resume` until complete or park |
| **TurnManager** | Per-turn mind: infer, tool batch, snapshot. Runtime constructs it for each Prompt/Resume |
| **Specialist** | Catalog nested agent (`spawn_specialist`). Not a Temporal worker process |
| **Child** | Nested session. Agent tools `list_children` / `get_child` / `cancel_child` |
| **Park** | Session idle waiting for `Resume`. Parent-facing `Status` stays `running`; `Waiting` is true until the interrupt is resolved. Parent park does not stop children |
| **Cancel** | Abort the in-flight turn and stop child sessions (`Runtime.Cancel`, original Prompt/Resume context cancel, client stop). The parent session stays open for a later Prompt |
| **Close** | Destroy the session and recursively stop children |
| **Turn locality** | Optional: keep a turn’s Temporal activities on one process (`Config.TurnLocality`) so VFS stays put |

The host API is `durable.Runtime` (`inprocess.New` or `temporal.New`). Tests use the same API. `TurnManager` is not a host type.

Host tools on `AgentSpec.Options.Tools` close over their clients at catalog register. That closure is the client for every later turn. Rebuild the tool if the client must change. See [tools.md](tools.md).

## In-process Runtime

```go
cat := durable.NewCatalog("agent")
cat.Register("agent", durable.AgentSpec{Options: opts})
snaps := inprocess.NewMemorySnapshot()
rt := inprocess.New(inprocess.Config{
	Catalog:    cat,
	Snapshots:  snaps,
	Projection: vfs.DirectProjection{},
})
id, _ := rt.CreateSession(ctx, durable.CreateSession{
	AgentID: "agent",
	State:   map[string]any{"user": "Ada", "company": "Acme"},
})
_ = rt.Prompt(ctx, id, durable.Prompt{Text: prompt, Auth: auth})
sub, _ := rt.Subscribe(ctx, id, 0)
```

`State` merges into checkpoint `userState` (tools read it with `StateGet`). Update it on a later `Prompt` or `Resume`. Tokens on `Auth` stay in process RAM for the turn; recipes land on `Snapshot.Mounts`.

One goroutine per session runs the harness wait loop. HITL parks that goroutine and waits for `Runtime.Resume`. The session record lives in `SnapshotStore`.

`Status` and the stream agree on when a turn finished. `StreamEventComplete` is published only after the checkpoint is saved and `Status` is already `complete`. `StreamEventError` that ends the turn is the same for `failed`. Park publishes `yield`; `Status` stays `running` with `Waiting` true. A later `Prompt` on a completed session starts a new turn: when `Prompt` returns, `Status` is `running` again.

## Temporal

The host runs:

1. A Tacklr Temporal worker (`NewWorker`) that registers `SessionWorkflow` and the turn activities. Do not register those yourself.
2. A protocol process (optional) whose `durable.Runtime` is `temporal.New(client, cfg)`. `temporal.Config` is the single host config for both `New` and `NewWorker`. **Secrets** is required and must be the same instance on both. Autonomous work skips the protocol and calls Runtime.

```go
c, err := tacklrtemporal.Dial(client.Options{HostPort: temporalHost})
cfg := tacklrtemporal.Config{
	Catalog:    cat,
	Snapshots:  snaps,   // session record
	Secrets:    secrets, // VFS tokens; shared with the worker
	Projection: vfs.DirectProjection{},
}
w := tacklrtemporal.NewWorker(c, cfg)
_ = w.Start()
rt := tacklrtemporal.New(c, cfg)
```

| Tacklr concept | Temporal |
|----------------|----------|
| Agent session | One workflow (`SessionWorkflow`) |
| Harness loop | Workflow function |
| Inference / tool | Activities (`Inference`, `Tool`) |
| Specialist | Child workflow |
| Turn locality | `Config.TurnLocality` keeps the turn’s activities on one Temporal worker. Zero (default) does not pin them. |
| Activity timeout | `Config.ActivityTimeout` is StartToCloseTimeout for Inference/Tool. Zero is 10 minutes. |
| Heartbeat | `Config.HeartbeatTimeout`. Zero is 30 seconds. |
| Activity retries | `Config.ActivityAttempts` is Temporal MaximumAttempts. Zero is 3. 1 means no retry. |
| Progress | Workflow Streams (`events`, `retry`, `close`) |
| HITL | Signal `Resume` (never inside an activity) |
| Leftover tools after HITL | Workflow variable (`rest`) replayed from history; not SnapshotStore |
| Spawn specialist | Child `SessionWorkflow` (wait for started). `ParentClosePolicy` is request-cancel. Tools call `HarnessRuntime` child methods; the workflow reconciles the child ledger after each Tool activity (start, cancel, wait). Child HITL signals the parent (`ChildWaiting`) then parent `Resume` signals the child. |

The worker registers `SessionWorkflow`, `Inference`, `Tool`, `CommitToolOutput`, and `EmitEvent`. Inference and Tool do not publish complete, yield, or turn-ending error. The workflow commits `Status`, then `EmitEvent` publishes the matching stream event.

## Child sessions

A child is a nested Runtime session, not a host-owned supervisor. The id is `{parent}/w/{specialist}/{call}`. The same wait loop runs. The child inherits MCP Durable topology and mount recipes from the parent, then overlays the named `Specialist`. Tokens come from `SecretStorage` (child id, then parent id). Each child turn opens its own VFS (`OpenTurnVFS` on the child id). It does not reuse the parent’s live `MountSession`.

Register specialists on `AgentOptions.Specialists`. The model sees four tools:

| Tool | Job |
|------|-----|
| `spawn_specialist` | Start a child. `block` defaults to true (parent waits). `block=false` returns a scheduled message immediately |
| `list_children` | Ids and parent-facing status. Interrupted children do not appear as a separate state; they stay `running` |
| `get_child` | Collect one result. `block=true` parks the parent until the child finishes (or the child parks for HITL, then parent `Resume` forwards) |
| `cancel_child` | Stop that child |

Child HITL does not change parent-facing `Status.State` from `running`. `Waiting` is internal until the interrupt is resolved. The parent stays `running` while a child HITL is outstanding.

### Park, cancel, close

| Event | Children |
|-------|----------|
| Parent parks (HITL on the parent, or `get_child` wait) | Keep running. Child Prompt uses the session kids context, not the parent turn |
| `Runtime.Cancel`, original Prompt/Resume context cancel, client stop | Stop all children, then abort the parent turn. The parent session stays open for a later Prompt |
| `Runtime.Close` | Recursively stop and destroy children |

A later `Prompt` on a session that was cancelled does not resurrect killed children.

### When a child fails

The parent does not fail with the child. The child becomes `failed` and stays on the parent’s list until collected or the parent is closed.

`get_child` (including `block=true`) returns the error as tool text, drops the child from the parent list, and the parent continues. The child session stays Status-able until the parent is closed or the child is cancelled. Uncollected complete or failed children still count toward the “cannot finish while children remain” nudge; the parent must `get_child` or `cancel_child` before it can complete.

Child sessions are nested Runtime sessions (in-process or Temporal). A panic in an in-process child turn goroutine is not recovered and can leave the child `running`. Temporal starts an async child without waiting; collect a failed async child with `get_child`.

## Tool batches

A model round can emit several tool calls. Each `function_call` is pending until a matching tool result is appended (`function_call_output` / `RoleTool`). The wait loop **does not infer again** until every call in that batch has a result, or a call is parked for HITL.

`spawn_specialist` is the same pairing. `block=false` appends a scheduled message immediately. `block=true` (the default) waits for that child; the child’s output **is** the tool result. A mixed batch (some blocking, some not) still waits for every blocking call to return before the next model round. Non-blocking results may already be in the window; blocking results must be too. The next round starts only when the batch has no open tool calls.

| Runtime | How the batch runs | Where leftovers live |
|---------|--------------------|----------------------|
| In-process | Parallel `RunToolCall` on one harness; snapshot once at join/yield | SnapshotStore (parked interrupt only) |
| Temporal | Sequential `Tool` activities (each loads/saves SnapshotStore) | Workflow history, not the snapshot |

Conversation (window, plan, parked interrupt) is always SnapshotStore. Temporal history is the scheduler: which calls remain after HITL. Do not copy that leftover list into the snapshot.

Azure/OpenAI Responses requires each `function_call` to be followed by a `function_call_output` with the same `call_id`. Pairing happens at marshal time. The invariant is: never start Inference with an open batch.

## Auth and VFS context

Credentials belong on the **work item**, not on a protocol-specific bind RPC and not in process RAM as the source of truth.

```text
CreateSession.Mounts   secret-free recipes (provider, alias, folder/drive/item ids)
Prompt.Auth            tokens + optional new bindings / drops
Resume.Auth            tokens for remount after park or worker recycle
```

`AuthContext` is protocol-neutral. ACP `_tacklr/vfs/bind` only stashes on the ACP wire session; `BindTurn` copies that stash onto `Prompt.Auth`. An autonomous host sets `Prompt.Auth` (and optional `CreateSession.Mounts`) when it queues the workflow. No protocol is required.

Recipes are cached on the session snapshot (`Snapshot.Mounts`): where a mount came from, not file contents. Providers lazy-load bytes on open/read. Tokens are not snapshotted and are not written to Temporal event history. The Temporal adapter puts them in `Config.Secrets` (`durable.SecretStorage`) before signaling a secret-free `AuthContext`. Activities load the bag at harness time (child sessions fall back to the parent id). Close deletes the session’s secrets.

`SecretStorage` is not `SnapshotStore`. Client and worker must share one instance (Redis, Postgres, Vault, or `MemorySecretStorage` when they share a process). There is no default: a private memory map per process looks like a successful Prompt and then remounts nothing.

After HITL or a worker restart, the next Prompt/Resume supplies tokens and they are Put again. A retry on another worker remounts only if that worker can `Get` the same store.

A 401 during a turn ends the activity. The client (or host) `Resume`s with a new token. There is no live callback from an activity into ACP.

MCP `Env`/`Headers` are stripped with `DurableConfigs` before Temporal payloads. `CredentialRef` stays and is resolved at activity time.

Encrypt remaining work-item payloads (prompt text, tool args, HITL bytes) at rest with a Temporal payload codec if the store is untrusted. That is defense in depth for non-token data, not the token control.

## Protocol contract

`server.Protocol` is the host extension point. Implement HTTP/WebSocket routes, map each `StreamEvent` to wire frames in `OnStreamEvent`, and call `server.RunTurn` to pump `Runtime.Subscribe`. ACP is one implementation:

```go
srv := server.NewServer(rt, cat, server.NewACPProtocol(wire), myProtocol{})
```

A protocol is the handshake: create a session, start a turn, stream `StreamEvent`, end a turn, return HITL answers. Map wire auth into `AuthContext`. Hosts that persist ACP wire envelopes in Postgres call `PostgresWireStore.Setup`.

| Runtime | ACP example | Host protocol / autonomous |
|---------|-------------|----------------------------|
| `CreateSession` | `session/new` | host start |
| `Prompt` + `Subscribe` | `session/prompt` | `RunTurn` / host prompt payload |
| `Resume` | `session/resume` (or mid-turn permission RPC) | `OnStreamEvent` Resume / host event |
| `Cancel` | `session/cancel` | host cancel |
| `Close` | `session/close` | host close |
| `Prompt.Auth` | `_tacklr/vfs/bind` stash → BindTurn | payload field |

Runtime, harness, VFS, and Temporal files compile with no protocol imports.

## Session data planes (frozen)

Three stores. Do not add a fourth. Do not copy a field from one into another except as a turn-scoped cache that the canonical store then owns.

| Plane | Lifetime | Owns | Never |
|-------|----------|------|-------|
| **SnapshotStore** | One Runtime session | Window, plan, parked interrupt, host `userState`, VFS recipes, identity (`AgentID`, `Parent`, `Specialist`, child ids) | Tokens, file bytes, leftover unstarted Temporal tool calls, MCP env/headers, child workflow futures |
| **Wait loop** | In-process `sessionProc` / Temporal workflow replay | Leftover unstarted Temporal batch calls, MCP Durable topology, child futures, secret-free `ApplyAuth` on the current signal, `Status` | Window, plan, tokens, file bytes. `userState` after the first snapshot save of the slice |
| **SecretStorage** | Session, deleted on Close | VFS credentials | Snapshot rows, Temporal payloads |

`Prompt.State` / `Resume.State` / `CreateSession.State` merge into checkpoint `userState`. They are not a second Temporal copy of that map.

In-process leftover tools stay in the checkpoint (one harness, one batch). Temporal leftover tools stay on the workflow (`rest`) because later calls in the batch have not run yet. Conversation is always SnapshotStore.

`Close` deletes the snapshot and the secret bag. A new session id does not load a previous snapshot. `Save` takes the `Revision` from the last `Load` (zero on first write) so two workers cannot overwrite each other.

## Map to Azure / Lambda (later)

| Tacklr | Temporal (now) | Azure DF (later) | Lambda (later) |
|--------|----------------|------------------|----------------|
| Runtime | Client + worker | Same | Same |
| Session workflow | Workflow | Orchestration | Durable execution |
| Inference / tool | Activity | Activity | Invoke + heartbeat |
| Child / specialist | Child workflow | Sub-orchestration | Nested execution |
| HITL | Signal Resume | WaitForExternalEvent | waitForEvent |
| Progress | Workflow Streams | Event Hubs / queue | SQS / stream |
| Auth | SecretStorage + secret-free signal | orchestration input / event | invocation payload |
| Session record | SnapshotStore | Same | Same |

Do not put Temporal `workflow.Context` on `durable.Runtime`.

## Observability

The wait loop (in-process goroutine or `SessionWorkflow`) starts `tacklr.turn`. Inference and Tool activities inherit that span through Temporal OpenTelemetry v2 header propagation. Do not add extra activity wrapper spans.

| Runtime | How the turn span starts |
|---------|--------------------------|
| In-process | wait loop calls `telemetry.StartTurnSpan` |
| Temporal | `temporalotel.Tracer` inside `SessionWorkflow` |

Host setup: `telemetry.Init` installs the process-wide ReplaySafe tracer (and OTLP exporters when an endpoint is set). `Dial` prepends Temporal’s official OpenTelemetry v2 plugin onto that global provider. Postgres Query/Exec spans join that same trace because `postgres.Store` / `PostgresWireStore` run otelpgx against the caller context.

```go
shutdown, err := telemetry.Init(ctx, telemetry.Config{
    ServiceName:  "tacklr-worker",
    OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), // Alloy / collector
    Insecure:     true, // local
})
c, err := tacklrtemporal.Dial(client.Options{HostPort: temporalHost})
w := tacklrtemporal.NewWorker(c, cfg) // same Config as temporal.New, including Secrets
```

Span attributes are closed enums and ids (`tacklr.runtime`, `tacklr.turn.kind`, `tacklr.agent_id`, `tacklr.outcome`). Logs carry prompt length, resume counts, retries, and error text.
