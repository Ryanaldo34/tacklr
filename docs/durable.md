# Durable runtime

Tacklr’s session kernel is `durable.Runtime`. A `server.Protocol` maps wire frames to Runtime calls. ACP is the native implementation (`NewACPProtocol`); hosts implement `Protocol` for their own streaming and delivery. The kernel does not import protocol types. Autonomous workflows call Runtime directly.

There are three ways to run an agent:

## Path A — embedder (`NewAgent` + `Run`)

```go
h := tacklr.NewAgent(ctx, opts)
events, _ := h.Run(ctx, prompt)
// HITL: read yield, then h.ReturnFromInterrupt
```

Human-in-the-loop waits in-process on the same harness. Persistence is `h.Checkpoint` / `h.RestoreCheckpoint` (the same blob `durable.Runtime` writes to SnapshotStore). This path does not require Temporal.

## Path B — in-process Runtime

```go
cat := durable.NewCatalog("agent")
cat.Register("agent", durable.AgentSpec{Options: opts})
rt := inprocess.New(cat, inprocess.WithProjection(vfs.DirectProjection{}))
id, _ := rt.CreateSession(ctx, durable.CreateSession{AgentID: "agent"})
_ = rt.Prompt(ctx, id, durable.Prompt{Text: prompt, Auth: auth})
sub, _ := rt.Subscribe(ctx, id, 0)
```

One goroutine per session runs the harness wait loop. HITL parks that goroutine and waits for `Runtime.Resume`. Session conversation lives in `SnapshotStore`.

## Path C — Temporal

The host runs:

1. A Tacklr Temporal worker (`EnableSessionWorker: true`) that registers `SessionWorkflow`, `Inference`, and `Tool`.
2. A protocol process (optional) whose `durable.Runtime` is `temporal.New(client, taskQueue, catalog)`. Autonomous jobs skip the protocol and call Runtime (or start the workflow) with a payload.

| Tacklr concept | Temporal |
|----------------|----------|
| Agent session | One workflow (`SessionWorkflow`) |
| Harness loop | Workflow function |
| Inference / tool | Activities (`Inference`, `Tool`) |
| Subagent | Child workflow |
| Sticky VFS locality | Worker Sessions (`CreateSession` while the turn runs; `CompleteSession` before HITL park) |
| Progress | Workflow Streams (`events`, `retry`, `close`) |
| HITL | Signal `Resume` (never inside an activity) |

## Auth and VFS context

Credentials belong on the **work item**, not on a protocol-specific kernel API and not in process RAM as the source of truth.

```text
CreateSession.Mounts   secret-free recipes (provider, alias, folder/drive/item ids)
Prompt.Auth            tokens + optional new bindings / drops
Resume.Auth            tokens for remount after park or worker recycle
```

`AuthContext` is protocol-neutral. ACP `_tacklr/vfs/bind` only stashes on the ACP wire session; `BindTurn` copies that stash onto `Prompt.Auth`. An autonomous host sets `Prompt.Auth` (and optional `CreateSession.Mounts`) when it queues the workflow. No protocol is required.

Recipes are cached on the session snapshot (`Snapshot.Mounts`): where a mount came from, not file contents. Providers lazy-load bytes on open/read. Tokens are not snapshotted. After HITL or a worker restart, the next Prompt/Resume supplies tokens; cached recipes remount the same folders.

A 401 during a turn ends the activity. The client (or host) `Resume`s with a new token. There is no live callback from an activity into ACP.

Encrypt work-item payloads at rest with a Temporal payload codec (or the equivalent on Azure/Lambda) if the store is untrusted.

## Protocol contract

`server.Protocol` is the host extension point. Implement HTTP/WebSocket routes, map each `StreamEvent` to wire frames in `OnStreamEvent`, and call `server.RunTurn` to pump `Runtime.Subscribe`. ACP is one implementation:

```go
srv := server.NewServer(rt, cat, server.NewACPProtocol(wire), myProtocol{})
```

A protocol is the handshake: create a session, start a turn, stream `StreamEvent`, end a turn, return HITL answers. Map wire auth into `AuthContext`.

| Runtime | ACP example | Host protocol / autonomous |
|---------|-------------|----------------------------|
| `CreateSession` | `session/new` | host start |
| `Prompt` + `Subscribe` | `session/prompt` | `RunTurn` / host prompt payload |
| `Resume` | `session/resume` (or mid-turn permission RPC) | `OnStreamEvent` Resume / host event |
| `Cancel` | `session/cancel` | host cancel |
| `Close` | `session/close` | host close |
| `Prompt.Auth` | `_tacklr/vfs/bind` stash → BindTurn | payload field |

Kernel, harness, VFS, and Temporal files compile with no protocol imports.

## SnapshotStore

| Store | Lifetime | Contents |
|-------|----------|----------|
| SnapshotStore | One Runtime session | Window, plan, pending tools, interrupts, parked-worker checkpoints, VFS recipes (no tokens, no file bytes) |

`Close` deletes the runtime snapshot. A new session id does not load a previous snapshot.

## Map to Azure / Lambda (later)

| Tacklr | Temporal (now) | Azure DF (later) | Lambda (later) |
|--------|----------------|------------------|----------------|
| Runtime | Client + worker | Same | Same |
| Session workflow | Workflow | Orchestration | Durable execution |
| Inference / tool | Activity | Activity | Invoke + heartbeat |
| Subagent | Child workflow | Sub-orchestration | Nested execution |
| HITL | Signal Resume | WaitForExternalEvent | waitForEvent |
| Progress | Workflow Streams | Event Hubs / queue | SQS / stream |
| Auth | Prompt/Resume payload | orchestration input / event | invocation payload |

Do not put Temporal `workflow.Context` on `durable.Runtime`.

## Observability

The wait loop (in-process goroutine or `SessionWorkflow`) starts `tacklr.turn`. Inference and Tool activities inherit that span through Temporal OpenTelemetry v2 header propagation. Do not add extra activity wrapper spans.

| Runtime | How the turn span starts |
|---------|--------------------------|
| Path A embedder | `AgentHarness.Run` / `ReturnFromInterrupt` |
| Path B in-process | wait loop calls `telemetry.StartTurnSpan` |
| Path C Temporal | `temporalotel.Tracer` inside `SessionWorkflow` |

Host setup: `telemetry.Init` installs the process-wide ReplaySafe tracer (and OTLP exporters when an endpoint is set). `Dial` prepends Temporal’s official OpenTelemetry v2 plugin onto that global provider. Postgres Query/Exec spans join the same trace when the host calls `telemetry.InstrumentPgx` on its pgx config before `brain.NewPostgresStore`.

```go
shutdown, err := telemetry.Init(ctx, telemetry.Config{
    ServiceName:  "tacklr-worker",
    OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), // Alloy / collector
    Insecure:     true, // local
})
c, err := tacklrtemporal.Dial(client.Options{HostPort: temporalHost})
w := tacklrtemporal.NewWorker(c, taskQueue, opts)
```

Span attributes are closed enums and ids (`tacklr.runtime`, `tacklr.turn.kind`, `tacklr.agent_id`, `tacklr.outcome`). Logs carry prompt length, resume counts, retries, and error text.
