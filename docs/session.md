# Session runtime

Tacklr’s session API is `session.Runtime`. A `server.Protocol` maps wire frames to Runtime calls. ACP is the built-in in package `server/acp` (`acp.New`). Hosts implement `Protocol` for their own streaming and delivery. Runtime does not import protocol types. Autonomous workflows call Runtime directly.

## Vocabulary

| Word | Meaning |
|------|---------|
| **Session** | Long-lived wait loop (`CreateSession` … `Close`) |
| **Turn** | One `Prompt` or `Resume` until complete or park |
| **TurnManager** | Per-turn mind: infer, tool calls, snapshot. Runtime constructs it for each Prompt/Resume |
| **Specialist** | Assistant defined on the one agent (`spawn_specialist`). Not a Temporal worker process |
| **Child** | Nested session job. Agent tools `list_children` / `cancel_child` |
| **Park** | Session idle waiting for `Resume`. Parent-facing `Status` stays `running`; `Waiting` is true until the interrupt is resolved. Parent park does not stop children |
| **Cancel** | Abort the in-flight turn and stop child sessions (`Runtime.Cancel`, original Prompt/Resume context cancel, client stop). The parent session stays open for a later Prompt |
| **Checkpoint** | Committed harness state inside the snapshot: the window of complete rounds, plan, user state, permission decisions, search cursors. `SessionCheckpoint`. Only a Commit step saves it |
| **Session messages** | Messages that left the context window (a collapse or a plan handoff), saved in the brain, searchable for this session only. Not the checkpoint. Deleted on Close |
| **Close** | Destroy the session, stop children, and delete its session messages |
| **Turn locality** | Optional: keep a turn’s Temporal activities on one process (`Config.TurnLocality`) so VFS stays put |

The host API is `session.Runtime`, implemented by `temporal.Open`. `TurnManager` is not a host type.

Host tools on `AgentOptions.Tools` close over their clients when the host builds the agent. That closure is the client for every later turn. Rebuild the tool if the client must change. See [tools.md](tools.md).

## Session

```go
cfg := tacklrtemporal.Config{
	Agent:     opts,
	Snapshots: snaps,
	Secrets:   secrets,
}
rt := tacklrtemporal.Open(c, cfg)
id, _ := rt.CreateSession(ctx, session.CreateSession{
	State: map[string]any{"user": "Ada", "company": "Acme"},
})
_ = rt.Prompt(ctx, id, session.Prompt{Text: prompt, Auth: auth})
sub, _ := rt.Subscribe(ctx, id, 0)
```

`State` is written into the checkpoint's user state by the session's first step (tools read it with `StateGet`). Update it on a later `Prompt` or `Resume`. Tokens on `Auth` go to `SecretStorage`. Mount recipes are part of the session's `Identity`, which the runtime holds.

One Temporal workflow per session runs the turn. HITL parks that workflow until `Runtime.Resume`. The session record lives in `SnapshotStore`.

`Status` and the stream agree on when a turn finished. `StreamEventComplete` is published only after the checkpoint is saved and `Status` is already `complete`. `StreamEventError` that ends the turn is the same for `failed`. Park publishes `yield`; `Status` stays `running` with `Waiting` true. A later `Prompt` on a completed session starts a new turn: when `Prompt` returns, `Status` is `running` again.

### Prompt during a live turn

`Runtime.Prompt` while a turn is live or parked **queues**. It does not cancel in-flight `Invoke` or blocking tools. `Prompt` returns when the session accepted the work (queued, not yet in the window). `Subscribe` still delivers the same turn’s completion. A second concurrent `session/prompt` / `RunTurn` subscriber may see that same completion. No new ACP method.

Queued messages are appended only between rounds: no round is open and the turn is not parked. `session.Turn.Run` uses that gate. They never append while `Invoke` is running or between a `function_call` and its result. A drain that writes messages forces another inference instead of complete.

`Prompt.Auth` and `Prompt.State` apply immediately (tokens and `userState` must not wait). `MCPServers` apply on the next idle construct, not the live harness.

`session/cancel` (`Runtime.Cancel`) remains the abort path: it cancels the turn, stops children, and **drops unread inbox items**. Close drops the inbox. Resume does not start from a queued Prompt; HITL stays parked until the open round's calls finish after Resume, then the inbox drains. A Commit is never cancelled: a cancel waits for it, so the turn always knows the snapshot revision it saved.

A worker crash replays the workflow. Prompts that the turn has not committed yet stay in workflow history, and so does the open round: replay rebuilds it from each step's recorded result.

## Temporal

The host runs:

1. `temporal.Open`, then `StartWorker`. The worker registers `SessionWorkflow` and the turn activities. Do not register those yourself.
2. A protocol process (optional) uses that same runtime. Zero `Snapshots` and `Secrets` are in-memory stores kept on the runtime. Pass shared stores when another process must see them.

```go
c, err := tacklrtemporal.Dial(client.Options{HostPort: temporalHost})
cfg := tacklrtemporal.Config{
	Agent:     agent,
	Snapshots: snaps,   // optional; zero is memory
	Secrets:   secrets, // optional; zero is memory
}
rt := tacklrtemporal.Open(c, cfg)
w := rt.StartWorker()
_ = w.Start()
```

| Tacklr concept | Temporal |
|----------------|----------|
| Agent session | One workflow (`SessionWorkflow`) |
| Harness loop | `session.Session` (`temporal` only runs steps, signals, and child workflows) |
| Steps | Activities: `Commit`, `Inference` (one model request), `Tool` (one tool call) |
| Specialist | Child workflow |
| Turn locality | `Config.TurnLocality` keeps the turn’s activities on one Temporal worker. Zero (default) does not pin them. |
| Activity timeout | `Config.ActivityTimeout` is StartToCloseTimeout for every step. Zero is 10 minutes. |
| Heartbeat | `Config.HeartbeatTimeout`. Zero is 30 seconds. |
| Activity retries | `Config.ActivityAttempts` is Temporal MaximumAttempts. Zero is 3. 1 means no retry. An activity retries `tacklr.ErrNetwork` (wrap a dial, read, timeout, or upstream 408/429/5xx at the call), a model refusal, and a stale checkpoint. Any other error stops on the first attempt. |
| Progress | Workflow Streams (`events`, `retry`, `close`) |
| HITL | Signal `Resume` (never inside an activity) |
| Calls left after HITL | The open round in workflow state (`session.Turn.Round`): the parked call runs with its answer, then each call that has no outcome yet |
| Spawn specialist | Child `SessionWorkflow` (wait for started). `ParentClosePolicy` is request-cancel. Tools call `HarnessRuntime` child methods; the workflow starts and cancels children from each recorded `Tool` outcome and waits for blocking ones. Child HITL signals the parent (`ChildWaiting`) then parent `Resume` signals the child. |

The worker registers `SessionWorkflow`, `Commit`, `Inference`, `Tool`, `EmitEvent`, and `RunJob`. Steps do not publish complete, yield, or turn-ending error. The workflow commits `Status`, then `EmitEvent` publishes the matching stream event. A blocking specialist's `Tool` outcome names the child; the session loop starts it, waits, and runs a `Tool` step for that call with the child's result. It does not park the parent.

## Child sessions

A child is a nested Runtime session, not a host-owned supervisor. Specialist ids are `{parent}/w/{specialist}/{call}`. Named worker ids are `{parent}/j/{name}/{call}`. Both are child sessions: `Runtime.Children` / `Runtime.Jobs` list them, `Status` reports them, Cancel/Close recurse. Specialists run the same wait loop with that specialist's model, tools, and instructions. Workers run `Config.Jobs[name]` instead of a model (Temporal: `RunJob` activity with the same retry policy as Inference/Tool). Tokens come from `SecretStorage` (child id, then parent id). Each specialist turn opens its own VFS (`OpenTurnVFS` on the child id). It does not reuse the parent’s live `MountSession`.

Register specialists on `AgentOptions.Specialists`. The model sees three tools and is the only one that starts specialists, through `spawn_specialist`. Host tools start Runtime job workers through `HarnessRuntime.Schedule`, and list or stop jobs with `Jobs` / `CancelJob`. The harness resolves both itself: it checks the name against the agent's specialists and the runtime's registered workers, and derives the child id from the call (`tacklr.ChildID`).

| Tool | Job |
|------|-----|
| `spawn_specialist` | Start a specialist job. `block` defaults to true (parent tool waits). `block=false` returns a scheduled message immediately |
| `list_children` | Ids and parent-facing status. Interrupted children do not appear as a separate state; they stay `running` |
| `cancel_child` | Stop that job |

Child HITL does not change parent-facing `Status.State` from `running`. `Waiting` is internal until the interrupt is resolved. The parent stays `running` while a child HITL is outstanding.

### Park, cancel, close

| Event | Children |
|-------|----------|
| Parent parks (HITL on the parent) | Keep running. Child Prompt uses the session kids context, not the parent turn |
| `Runtime.Cancel`, original Prompt/Resume context cancel, client stop | Stop all children, then abort the parent turn. The parent session stays open for a later Prompt |
| `Runtime.Close` | Recursively stop and destroy children. Deletes that session's messages from the brain |

A later `Prompt` on a session that was cancelled does not resurrect killed children.

### When a child fails

The parent does not fail with the child. The child becomes `failed` and stays on the parent’s list until collected or the parent is closed.

Drain auto-collects terminal `block=false` jobs at the next safe window point: a `RoleUser` steer (`Job {id} ({name}) completed|failed`) is appended, and the job is dropped from `Jobs`. `block=true` spawn waits for the child: its result **is** the tool output, and it never uses the inbox. Only `spawn_specialist` starts specialists; a tool's `Schedule` starts Runtime job workers. A later job result is a new `RoleUser` message, never a second `RoleTool` for the schedule `call_id`.

The turn does not complete while jobs remain. The wait-loop blocks without parent park and without another parent model call. A finished job or a human `Prompt` wakes it through the inbox. Specialist child HITL is resumed on the **child** session.

Child sessions are nested `session.Runtime` sessions. Temporal starts an async child without waiting. A terminal async child is collected as a job message.

## Rounds

The context window holds **complete rounds** only. A round is a user message, or one model reply and a result for each call it made. The window changes only when a round commits (or a plan install or handoff replaces it), so the model never sees a call without its result, and no placeholder result exists.

A reply with calls opens a round. It stays out of the window, in the checkpoint, while its results arrive:

| Call | Its result | When |
|------|------------|------|
| Success, error, correction, unknown tool | The output or the error | When the reply's calls run, in call order |
| `block=false` spawn, or `Schedule` | "Job … scheduled …" | Then too |
| Blocking specialist | The child's output; status `error` and its error when the child failed | When the child finishes |
| Parked (permission, `ask_user_choice`) | None yet: the turn yields | After Resume runs the call again |
| A new Prompt while calls are open | `CancelledToolResultContent` | Before the Prompt commits |

When the last call has a result, the round commits. The model is called only when no round is open. When a `block=false` job later completes, the drain appends a separate `RoleUser` job result (not another `RoleTool` for that `call_id`).

A commit fits the round into `MaxWindowSize`:

1. A round with a plan install or handoff goes in as it is; the effect then replaces the window.
2. Under 85% of `MaxWindowSize`, the round is appended.
3. Over it, the window **collapses**: everything after the first user message and the plan document (an earlier summary too) is saved as session messages, then replaced by one summary of the window as it was. The new round is not in the summary request; it is appended after the summary. With a Brain the summary says that `search` finds the earlier messages and `open` reads one. The window is then small, so the next rounds append without another model call: a session pays for a summary only when the window fills again.
4. A round still over `MaxWindowSize` alone has its largest tool results cut. With `AgentOptions.OverflowPath` (a directory on a writable mount, for example an object-storage mount), each one's full text is first written to `<OverflowPath>/<session>/<call id>.txt`. The cut result ends with a note: how much was kept, and the path to `read` with `start` and `end`, or that the full text was not kept.

A commit that fails (a token count, the session-message save, the summary request, a round that cannot fit, or the overflow write) fails the step and saves nothing.

### Steps

A runtime implements one interface, `session.Driver`: the steps below, the session mailbox (`Prompts`, `Ready`, `Recv`, `Wait`), and its children (`Children`, `Start`, `Await`, `CancelAll`). `Session.Run` and `Turn.Run` are the same for every runtime.

Each non-deterministic action is one step, so a session replays safely: the runtime records each completed step and its result, and a replay returns the recorded result instead of running the step again. Neither the harness nor the snapshot records which steps ran.

| Step | Does | Writes the snapshot |
|------|------|---------------------|
| `Infer` | One model request on the committed window. Returns the reply. | No |
| `Tool` | One tool call. Input: the call, and from its last outcome the answer to its park or its child's result. Returns the call's outcome: its result, a park, or a wait on a child, the children it starts or cancels, and the shared-state writes it made (user state, plan changes, permission decisions, search cursors). | No |
| `Commit` | Host state, a complete round (each call's writes in call order, then its messages, then collapse or trim), and user messages. | Yes, the only one |

The open round lives in workflow state (`session.Turn.Round`): the reply and each call's last outcome. `Turn.Run` runs every call without an outcome at once (`Driver.Tools`: one `Tool` activity each on Temporal; goroutines in a simple runtime), keeps the outcomes in call order, starts the children they ask for (a child id comes from its call, so a child is started once), waits for blocking children and runs their calls again with the results, and commits the round once each call has its result. Plan changes are replayed at Commit (`planOp`), so plan changes of calls that ran at once all land.

A runtime may run a step more than once before it records it (a worker dies before the step reports). `Infer` and `Tool` change nothing a later step reads, so that is safe; a tool with external side effects uses its call id (`HarnessRuntime.CurrentToolCallID`) as the idempotency key. `Commit` is idempotent: its `base` is the revision the last Commit returned, and a Commit that finds the snapshot past it already saved and applies nothing again. A cancel does not stop a Commit. Steps carry only what changes per step; the session's identity, MCP servers and children are the runtime's, which builds each step's world from them.

A turn parks on one interrupt at a time. When several calls of a reply park, the first in call order parks the turn; the others keep no outcome and run after resume. A resume runs the parked call with its answer, then the calls without an outcome. A prompt that moves past an open round (a turn that failed) gives each call without a result a cancelled result and commits the round before the prompt.

Azure/OpenAI Responses requires each `function_call` to be followed by a `function_call_output` with the same `call_id`. Rounds make that hold by construction.

## Auth and VFS context

Credentials belong on the **work item**, not on a protocol-specific bind RPC and not in process RAM as the source of truth.

```text
CreateSession.Mounts   secret-free recipes (provider, alias, folder/drive/item ids)
Prompt.Auth            tokens + optional new bindings / drops
Resume.Auth            tokens for remount after park or worker recycle
```

`AuthContext` is protocol-neutral. ACP `_tacklr/vfs/bind` only stashes on the ACP wire session; `BindTurn` copies that stash onto `Prompt.Auth`. An autonomous host sets `Prompt.Auth` (and optional `CreateSession.Mounts`) when it queues the workflow. No protocol is required.

Recipes are on the session's `Identity` (`Identity.Mounts`), which the runtime holds and gives each step: where a mount came from, not file contents. Providers lazy-load bytes on open/read. Tokens are not snapshotted and are not written to Temporal event history. The Temporal adapter puts them in `Config.Secrets` (`session.SecretStorage`) before signaling a secret-free `AuthContext`. Activities load the bag at harness time (child sessions fall back to the parent id). Close deletes the session’s secrets.

`SecretStorage` is not `SnapshotStore`. Client and worker must share one instance (Redis, Postgres, Vault, or `MemorySecretStorage` when they share a process). There is no default: a private memory map per process looks like a successful Prompt and then remounts nothing.

After HITL or a worker restart, the next Prompt/Resume supplies tokens and they are Put again. A retry on another worker remounts only if that worker can `Get` the same store.

A 401 during a turn fails that activity on the first attempt. The host sends a new token on the next `Prompt` or `Resume`. There is no live callback from an activity into ACP.

MCP `Env`/`Headers` are stripped with `DurableConfigs` before Temporal payloads. `CredentialRef` stays and is resolved at activity time.

Encrypt remaining work-item payloads (prompt text, tool args, HITL bytes) at rest with a Temporal payload codec if the store is untrusted. That is defense in depth for non-token data, not the token control.

## Protocol contract

`server.Protocol` is the host extension point. ACP’s built-in remote transport is WebSocket on `GET /acp` (JSON-RPC both ways). Hosts may add their own HTTP routes. Map each `StreamEvent` to wire frames in `OnStreamEvent`, and call `server.RunTurn` to pump `Runtime.Subscribe`:

```go
srv := server.NewServer(rt, agent, acp.New(wire), myProtocol{})
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
| **SnapshotStore** | One Runtime session | `tacklr.SessionCheckpoint`: window of complete rounds, plan, `userState`, permission decisions, search rests | Tokens, file bytes, MCP env/headers, identity (specialist, parent, mounts), child workflow futures, host config (search namespace, principals, `AllowTools`) |
| **Wait loop** | In-process `sessionProc` / Temporal workflow replay | `Identity` (specialist, parent, mount recipes), MCP Durable topology, child futures, the open round (each call's last outcome), the last Commit's snapshot revision, Prompt/job inbox (steer + auto-collected jobs), host state not yet committed, secret-free `ApplyAuth` on the current signal, `Status` | Window, plan, tokens, file bytes, `userState`. Inbox is not SnapshotStore. |
| **SecretStorage** | Session, deleted on Close | VFS credentials | Snapshot rows, Temporal payloads |

`Prompt.State` / `Resume.State` / `CreateSession.State` are sent once, to the next step, which writes them into the checkpoint's `userState`. The wait loop holds them only until then, so a tool's later `StateSet` of the same key is not overwritten.

`Close` deletes the snapshot, the secret bag, and that session's messages from the brain. A new session id does not load a previous snapshot. `Save` takes the `Revision` from the last `Load` (zero on first write) so two workers cannot overwrite each other.

Session messages live in the brain table `session_messages`, not in SnapshotStore. A collapse or a plan handoff writes the exact messages that leave the window there, so this session can search them later. They are not the checkpoint.

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

Do not put Temporal `workflow.Context` on `session.Runtime`.

## Observability

The session loop starts `tacklr.turn`. Inference and tool steps inherit that span. Do not add extra wrapper spans around a step.

Host setup: `telemetry.Init` installs the process-wide OpenTelemetry providers and, when an endpoint is set, the OTLP exporters. `Dial` replaces the tracer with a replay-safe one that keeps that export configuration, then prepends Temporal’s OpenTelemetry plugin. Postgres statement spans from `postgres.Store` join the same trace: `postgres.Open` sets pgx's otelpgx tracer (`ConnConfig.Tracer`). A host that builds its own pool for `server.PostgresWireStore` sets that tracer itself.

```go
shutdown, err := telemetry.Init(ctx, telemetry.Config{
    ServiceName:  "tacklr-worker",
    OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), // Alloy / collector
    Insecure:     true, // local
})
c, err := tacklrtemporal.Dial(client.Options{HostPort: temporalHost})
w := rt.StartWorker()
```

Span attributes are closed enums and ids (`tacklr.runtime`, `tacklr.turn.kind`, `tacklr.agent_id`, `tacklr.outcome`). Logs carry prompt length, resume counts, retries, and error text.
