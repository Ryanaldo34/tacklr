# Sessions

Tacklr’s session API is `session.Client`. A `server.Protocol` maps wire frames to Client calls. ACP is the built-in in package `server/acp` (`acp.New`). Hosts implement `Protocol` for their own streaming and delivery. `session` does not import protocol types. Autonomous workflows call the Client directly.

The packages split along that line. A host that only talks to sessions imports `session` (the contract: `Client`, `Store`, `Event`, `Status`) and `session/temporal` (`temporal.Client`); neither imports the harness (`tacklr`), the VFS, the brain, the session loop or the worker. The process that runs sessions imports `session/temporal/worker`, which runs the session loop (`session/loop`) over the harness.

## Vocabulary

| Word | Meaning |
|------|---------|
| **Session** | Long-lived wait loop (`CreateSession` … `Close`) |
| **Turn** | One `Prompt` or `Resume` until complete or park |
| **Harness** | `tacklr.Harness`, the session's harness: infer, tool calls, commit. A worker keeps it for the session |
| **Harness cache** | A session's `tacklr.Harness` and VFS on one worker: tools, MCP connections and their tools, skills, the system prompt, mounts. Built once and used by every step of the session on that worker; committed state in it is reloaded only when the snapshot's revision moved |
| **Specialist** | Assistant defined on the one agent (`spawn_specialist`). Not a Temporal worker process |
| **Child** | Nested session job. Agent tools `list_children` / `cancel_child` |
| **Park** | Session idle waiting for `Resume`. Parent-facing `Status` stays `running`; `Waiting` is true until the interrupt is resolved. Parent park does not stop children |
| **Cancel** | Abort the in-flight turn and stop child sessions (`Client.Cancel`, original Prompt/Resume context cancel, client stop). The parent session stays open for a later Prompt |
| **Checkpoint** | Committed harness state inside the snapshot: the window of complete rounds, plan, user state, permission decisions, search cursors. Opaque bytes (`Harness.Checkpoint`) in `session.Store`. Only a Commit step saves it |
| **Session messages** | Messages that left the context window (a collapse or a plan handoff), saved in the brain, searchable for this session only. Not the checkpoint. Deleted on Close |
| **Close** | Destroy the session, stop children, and delete its session messages |

The host API is `session.Client`, implemented by `temporal.Client` (`temporal.Open`). `tacklr.Harness` is not a host type.

Host tools on `AgentOptions.Tools` close over their clients when the host builds the agent. That closure is the client for every later turn. Rebuild the tool if the client must change. See [tools.md](tools.md).

## Session

```go
sessions := temporal.Open(c, temporal.Config{Store: store, Secrets: secrets})
id, _ := sessions.CreateSession(ctx, session.CreateSession{
	State: map[string]any{"user": "Ada", "company": "Acme"},
})
_ = sessions.Prompt(ctx, id, session.Prompt{Text: prompt, Auth: auth})
sub, _ := sessions.Subscribe(ctx, id, 0)
```

`State` is written into the checkpoint's user state by the session's first step (tools read it with `StateGet`). The client rejects state JSON cannot hold, and an empty key, with `session.ErrInvalid`. Update it on a later `Prompt` or `Resume`. Tokens on `Auth` go to `SecretStorage`. Mount recipes are part of the session's `Identity`, which the runtime holds.

One Temporal workflow per session runs the turn. HITL parks that workflow until `Client.Resume`. The session record lives in `session.Store`.

`Status` and the stream agree on when a turn finished. `session.EventComplete` is published only after the checkpoint is saved and `Status` is already `complete`. `session.EventError` that ends the turn is the same for `failed`. Park publishes `yield`; `Status` stays `running` with `Waiting` true. A later `Prompt` on a completed session starts a new turn: when `Prompt` returns, `Status` is `running` again.

### Prompt during a live turn

`Client.Prompt` while a turn is live or parked **queues**. It does not cancel in-flight `Invoke` or blocking tools. `Prompt` returns when the session accepted the work (queued, not yet in the window). `Subscribe` still delivers the same turn’s completion. A second concurrent `session/prompt` / `RunTurn` subscriber may see that same completion. No new ACP method.

Queued messages are appended only between rounds: no round is open and the turn is not parked. The session loop (`loop.Run`) uses that gate. They never append while `Invoke` is running or between a `function_call` and its result. A drain that writes messages forces another inference instead of complete.

`Prompt.Auth` and `Prompt.State` apply immediately (tokens and `userState` must not wait). `MCPServers` apply on the next idle construct, not the live harness.

`session/cancel` (`Client.Cancel`) remains the abort path: it cancels the turn, stops children, and **drops unread inbox items**. Close drops the inbox. Resume does not start from a queued Prompt; HITL stays parked until the open round's calls finish after Resume, then the inbox drains. A Commit is never cancelled: a cancel waits for it, so the turn always knows the snapshot revision it saved.

A worker crash replays the workflow. Prompts that the turn has not committed yet stay in workflow history, and so does the open round: replay rebuilds it from each step's recorded result.

## Temporal

The host runs two halves, in one process or many:

1. A worker: `worker.New` (package `session/temporal/worker`), then `Start` or `Run`. It registers the session workflow (type name `SessionWorkflow`) and the turn activities. Do not register those yourself.
2. A client: `temporal.Open` (package `session/temporal`). A protocol process or an autonomous host uses it to start sessions and talk to them. It starts the workflow by name, so it imports no worker code.

Both take the same `Store` and `Secrets` (required), `TaskQueue` and `Fallback`. In-memory stores work only when both halves share a process; pass shared stores otherwise. The client's `Config` holds the session's step policy (timeouts, attempts, fan-out limits, continue-as-new), which it sends in the workflow input; the worker's `Config` holds the agent, the jobs and what one worker keeps (harnesses, concurrency, heartbeats).

```go
c, err := temporal.Dial(client.Options{HostPort: temporalHost})
w := worker.New(c, worker.Config{Agent: agent, Store: store, Secrets: secrets, Jobs: jobs})
_ = w.Start()
sessions := temporal.Open(c, temporal.Config{Store: store, Secrets: secrets})
```

| Tacklr concept | Temporal |
|----------------|----------|
| Agent session | One workflow (`SessionWorkflow`) |
| Harness loop | `loop.Run` over the worker's `loop.Executor` (the worker only runs steps, signals, and child workflows) |
| Steps | Activities: `Commit`, `Inference` (one model request), `Tool` (one tool call) |
| Specialist | Child workflow |
| Harness routing | Each worker also polls its own task queue. A session's first harness step asks a worker on the shared queue for its queue (`WorkerQueue`), and the workflow sends the session's `Commit`, `Inference` and `Tool` steps there, so they share that worker's harness. A step that does not start within `temporal.Config.WorkerQueueTimeout` (zero: 10 seconds; the worker is gone) goes back to the shared queue, and the worker that takes it builds the harness again. The session's end closes its harness (`CloseHarness`); a harness unused for `worker.Config.HarnessIdle` (zero: 10 minutes) closes too. |
| Worker capacity | `worker.Config.MaxConcurrentActivityExecutionSize` caps the steps one worker runs at once. Zero is Temporal's default. `worker.Config.MaxHarnesses` (zero: 256) caps the harnesses one worker keeps; past it the harness unused longest closes. |
| Reply fan-out | The calls of one reply run as one `Tool` activity each, at most `temporal.Config.MaxConcurrentToolCalls` at once (zero: 32); the rest start as those finish. Results stay in call order. |
| Children | A session runs at most `temporal.Config.MaxChildren` children at once (zero: 1,000, under Temporal's default limit of 2,000 incomplete child workflows). A start past it is a correction for the model. |
| Activity timeout | `temporal.Config.ActivityTimeout` is StartToCloseTimeout for `Commit`, `Inference` and `Tool`. Zero is 10 minutes. A tool whose `ToolConfig.Timeout` plus one minute is longer gets that instead. `WorkerQueue` is a local activity (one history marker, no task) and `CloseHarness` has 30 seconds. |
| Heartbeat | `temporal.Config.HeartbeatTimeout`. Zero is 30 seconds. |
| Activity retries | `temporal.Config.ActivityAttempts` is Temporal MaximumAttempts. Zero is 3. 1 means no retry. Retries back off from one second, doubling, to thirty. An activity retries `tacklr.ErrNetwork` (wrap a dial, read, timeout, or upstream 408/429/5xx at the call), a model refusal, and a stale checkpoint. Any other error stops on the first attempt. |
| History size | A session continues as new where its turn waits: idle (`Recv`), waiting on background jobs (`Wait`), or waiting on blocking specialists (`Await`). It does so once Temporal suggests it (`GetContinueAsNewSuggested`), or once its history reaches `temporal.Config.ContinueAsNewEvents` events when that is set. The next run gets the session's workflow state (open round, last commit revision, inbox, queued host state, MCP, its worker queue, whether the turn finished or was waiting) and the event stream's log and offsets, so subscribers keep reading. Running children carry over by id: children use `ParentClosePolicy` ABANDON, so continue-as-new does not stop them. A child sends its result to its parent's current run with the `tacklr_child_done` signal (an empty run id names the current run). |
| Tool results | A `Tool` step keeps the call's `session.Outcome` (its result text, and the record of its result, park or wait) in `Store.PutOutcome` under the session and the call key, and returns a `CallOutcome` with `Stored` set and no record. `Commit` reads the records back (`Outcomes`). The stream's tool-result event carries the call key, not the result; `Client.Subscribe` reads the outcome's result text from the store, so the client never decodes a harness record. The fallback `EventLog` is not history and gets the whole event. A retried `Tool` step puts the same key again, so it leaves one record. History holds each result zero times: four calls of 100 KB each make about 25 KB of history (85 events), the same as four calls of 10 KB. Before, each result was in history three times (the `Tool` result, the stream publish, the `Commit` input). |
| Large payloads | Model replies are activity results in history, and the reply is the `Commit` input too. Temporal limits one payload to 2 MB by default. Configure Temporal's External Storage on the client you pass to `temporal.Open` and `worker.New` (`client.Options.ExternalStorage`, with the SDK's S3 or GCS driver): payloads over its threshold (256 KiB by default) go to the bucket and history keeps a reference. Workers inherit it from that client. |
| Progress | Workflow Streams (`events`, `retry`, `close`) |
| HITL | Signal `Resume` (never inside an activity) |
| Calls left after HITL | The open round in workflow state (`loop.Turn.Round`): the parked call runs with its answer, then each call that has no outcome yet |
| Spawn specialist | Child `SessionWorkflow` (wait for started). `ParentClosePolicy` is ABANDON, so a child outlives a continue-as-new of its parent; the parent cancels its running children by id (`RequestCancelExternalWorkflow`) on Cancel, Close and the end of its run. Tools call `CallContext` job methods; the session starts and cancels jobs from each recorded `Tool` outcome (`Turn.Jobs`). A job reports its result once, by the `tacklr_child_done` signal, which reaches a parent that continued as new. A child that parks is resumed by its own id. |

The worker registers `SessionWorkflow`, `Commit`, `Inference`, `Tool`, `EmitEvent`, `RunJob`, `WorkerQueue`, `CloseHarness` and `Forget`. A closed session runs `Forget`, which deletes its saved messages and its jobs' from the brain, then `CloseHarness`. Steps do not publish complete, yield, or turn-ending error. The workflow commits `Status`, then `EmitEvent` publishes the matching stream event. A blocking specialist's `Tool` outcome names the child; the session loop starts it, waits, and runs a `Tool` step for that call with the child's result. It does not park the parent.

## Child sessions

A child is a nested session, not a host-owned supervisor. Specialist ids are `{parent}/w/{specialist}/{call}`. Named worker ids are `{parent}/j/{name}/{call}`. Both are child sessions: `Client.Jobs` lists them with their status, `Status` reports one, Cancel/Close recurse. Specialists run the same wait loop with that specialist's model, tools, and instructions. Workers run `worker.Config.Jobs[name]` instead of a model (Temporal: `RunJob` activity with the same retry policy as Inference/Tool). Tokens come from `SecretStorage` (child id, then parent id). Each specialist turn opens its own VFS on the child id. It does not reuse the parent’s live `MountSession`.

Register specialists on `AgentOptions.Specialists`. The model sees three tools and is the only one that starts specialists, through `spawn_specialist`. Host tools start job workers through `CallContext.Schedule`, and list or stop jobs with `Jobs` / `CancelJob`. The harness resolves both itself: it checks the name against the agent's specialists and the worker's registered jobs, and derives the child id from the call (`tacklr.ChildID`).

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
| `Client.Cancel`, original Prompt/Resume context cancel, client stop | Stop all children, then abort the parent turn. The parent session stays open for a later Prompt |
| `Client.Close` | Recursively stop and destroy children. The session's worker deletes that session's messages from the brain (`Forget`) |

A later `Prompt` on a session that was cancelled does not resurrect killed children.

### When a child fails

The parent does not fail with the child. The child becomes `failed` and stays on the parent’s list until collected or the parent is closed.

Drain auto-collects terminal `block=false` jobs at the next safe window point: a `RoleUser` steer (`Job {id} ({name}) completed|failed`) is appended, and the job is dropped from `Jobs`. `block=true` spawn waits for the child: its result **is** the tool output, and it never uses the inbox. Only `spawn_specialist` starts specialists; a tool's `Schedule` starts job workers. A later job result is a new `RoleUser` message, never a second `RoleTool` for the schedule `call_id`.

The turn does not complete while jobs remain. The wait-loop blocks without parent park and without another parent model call. A finished job or a human `Prompt` wakes it through the inbox. Specialist child HITL is resumed on the **child** session.

Child sessions are nested sessions of the same `session.Client`. Temporal starts an async child without waiting. A terminal async child is collected as a job message.

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

A runtime implements one interface, `loop.Executor`: the steps below, one mailbox read (`Next`), and running jobs (`Start`, `Cancel`). `loop.Run` and the turn are the same for every runtime: one session function in `loop.Run` decides what every wake does given the session's state (idle, running a turn, waiting on jobs, parked), and the session owns each job's lifecycle (`Turn.Jobs`: started by id, running, ended into its waiting call's result or an inbox message).

Each non-deterministic action is one step, so a session replays safely: the runtime records each completed step and its result, and a replay returns the recorded result instead of running the step again. Neither the harness nor the snapshot records which steps ran.

| Step | Does | Writes the snapshot |
|------|------|---------------------|
| `Infer` | One model request on the committed window. Returns the reply. | No |
| `Tool` | One tool call. Input: the call, and from its last outcome the answer to its park or its child's result. Returns the call's outcome: its result, a park, or a wait on a child, the children it starts or cancels, and the shared-state writes it made (user state, plan changes, permission decisions, search cursors). | No |
| `Commit` | Host state, a complete round (each call's writes in call order, then its messages, then collapse or trim), and user messages. | Yes, the only one |

The open round lives in workflow state (`loop.Turn.Round`): the reply and each call's last outcome. The loop runs every call without an outcome at once (`Executor.Tools`: one `Tool` activity each on Temporal; goroutines in a simple runtime), keeps the outcomes in call order, starts the jobs they ask for (a job id comes from its call, so a job is started once), waits for blocking jobs' ends and runs their calls again with the results, and commits the round once each call has its result. Plan changes are replayed at Commit (`planOp`), so plan changes of calls that ran at once all land.

A runtime may run a step more than once before it records it (a worker dies before the step reports). `Infer` and `Tool` change nothing a later step reads, so that is safe; a tool with external side effects uses its call id (`CallContext.CurrentToolCallID`) as the idempotency key. `Commit` is idempotent: its `base` is the revision the last Commit returned, and a Commit that finds the snapshot past it already saved and applies nothing again. A cancel does not stop a Commit. Steps carry only what changes per step; the session's identity and MCP servers are the runtime's, which builds each step's harness from them. A cancel reaches a running step on its next heartbeat reply (`worker.Config.HeartbeatThrottle` bounds how long that takes, 2 seconds by default). Tracing is the host's to set up; Tacklr changes no process-wide tracing state. To trace sessions, install Temporal's replay-safe tracer provider and add Temporal's OpenTelemetry plugin to the client you dial:

```go
otel.SetTracerProvider(temporalotel.NewReplaySafeTracerProvider( /* your exporter options */ ))
plugin, err := temporalotel.NewPlugin(temporalotel.PluginOptions{}) // go.temporal.io/sdk/contrib/opentelemetry-v2
c, err := temporal.Dial(client.Options{HostPort: addr, Plugins: []client.Plugin{plugin}})
```

A worker built from that client (`worker.New(c, …)`) gets the plugin too; do not add it again on the worker. A host process that only runs the client side still needs the plugin on its own client for client-side spans.

### Harnesses

A step needs the session's harness: its `tacklr.Harness` (tools, MCP connections and their tools, skills, the system prompt) and its VFS. A worker keeps one harness per session (its harness cache) and every step of the session it runs uses it, so a reply's calls do not each open the VFS and connect every MCP server. The harness is named by a key of everything it is built from: the specialist, the mounts and their auth, the MCP servers and the job names. A step whose key differs (a prompt changed the MCP servers, auth was refreshed) closes the old harness and builds a new one. MCP servers connect and list their tools when the harness is built, because the model's tool list needs them. The tool list and system prompt are built in a fixed order from those inputs, so every step sends the model the same request prefix, and a harness built again on another worker sends the same one: the provider's prompt cache holds.

Committed state is the store's. A step reads the snapshot's revision (`Store.Revision`) and loads the snapshot only when it moved past the one the harness holds, after a commit on another worker. The calls of a reply read at once; `Commit` holds the harness alone. A Commit that fails drops the harness, since its state may be ahead of the store. A step the turn moved past (a cancelled tool that has not returned) never blocks the session: a writer that finds it still reading builds a new harness.

The routing that sends a session's steps to the worker holding its harness is the runtime's (Temporal: a task queue per worker, see above). Without it a step still works: the worker it lands on builds the harness.

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

Recipes are on the session's `Identity` (`Identity.Mounts`), which the runtime holds and gives each step: where a mount came from, not file contents. Providers lazy-load bytes on open/read. Tokens are not snapshotted and are not written to Temporal event history. The Temporal adapter puts them in `Config.Secrets` (client and worker) (`session.SecretStorage`) before signaling a secret-free `AuthContext`. Activities load the bag at harness time (child sessions fall back to the parent id). Close deletes the session’s secrets.

`SecretStorage` is not `session.Store`. Client and worker must share one instance (Redis, Postgres, Vault, or `MemorySecretStorage` when they share a process). There is no default: a private memory map per process looks like a successful Prompt and then remounts nothing.

After HITL or a worker restart, the next Prompt/Resume supplies tokens and they are Put again. A retry on another worker remounts only if that worker can `Get` the same store.

A 401 during a turn fails that activity on the first attempt. The host sends a new token on the next `Prompt` or `Resume`. There is no live callback from an activity into ACP.

MCP `Env`/`Headers` are stripped with `DurableConfigs` before Temporal payloads. `CredentialRef` stays and is resolved at activity time.

Encrypt remaining work-item payloads (prompt text, tool args, HITL bytes) at rest with a Temporal payload codec if the store is untrusted. That is defense in depth for non-token data, not the token control.

## Protocol contract

`server.Protocol` is the host extension point. ACP’s built-in remote transport is WebSocket on `GET /acp` (JSON-RPC both ways). Hosts may add their own HTTP routes. Map each `session.Event` to wire frames in `OnEvent`, and call `server.RunTurn` to pump `Client.Subscribe`:

```go
srv := server.NewServer(sessions, agent, acp.New(wire), myProtocol{})
```

A protocol is the handshake: create a session, start a turn, stream `session.Event`, end a turn, return HITL answers. Map wire auth into `AuthContext`. Hosts that persist ACP wire envelopes in Postgres call `PostgresWireStore.Setup`.

| Client | ACP example | Host protocol / autonomous |
|---------|-------------|----------------------------|
| `CreateSession` | `session/new` | host start |
| `Prompt` + `Subscribe` | `session/prompt` | `RunTurn` / host prompt payload |
| `Resume` | `session/resume` (or mid-turn permission RPC) | `OnEvent` Resume / host event |
| `Cancel` | `session/cancel` | host cancel |
| `Close` | `session/close` | host close |
| `Prompt.Auth` | `_tacklr/vfs/bind` stash → BindTurn | payload field |

The client, the loop, the harness, the VFS, and the Temporal client and worker compile with no protocol imports.

## Session data planes (frozen)

Three stores. Do not add a fourth. Do not copy a field from one into another except as a turn-scoped cache that the canonical store then owns.

| Plane | Lifetime | Owns | Never |
|-------|----------|------|-------|
| **session.Store** | One session | The checkpoint (opaque bytes): window of complete rounds, plan, `userState`, permission decisions, search rests; each tool call's `Outcome` (`PutOutcome`/`Outcomes`), deleted with the session | Tokens, file bytes, MCP env/headers, identity (specialist, parent, mounts), child workflow futures, host config (search namespace, principals, `AllowTools`) |
| **Wait loop** | In-process `sessionProc` / Temporal workflow replay | `Identity` (specialist, parent, mount recipes), MCP Durable topology, child futures, the open round (each call's last outcome), the last Commit's snapshot revision, Prompt/job inbox (steer + auto-collected jobs), host state not yet committed, secret-free `ApplyAuth` on the current signal, `Status` | Window, plan, tokens, file bytes, `userState`. Inbox is not `session.Store`. |
| **SecretStorage** | Session, deleted on Close | VFS credentials | Snapshot rows, Temporal payloads |

`Prompt.State` / `Resume.State` / `CreateSession.State` are sent once, to the next step, which writes them into the checkpoint's `userState`. The wait loop holds them only until then, so a tool's later `StateSet` of the same key is not overwritten.

`Close` deletes the snapshot and the secret bag; the closed session's worker deletes its messages from the brain. A new session id does not load a previous snapshot. `Save` takes the `Revision` from the last `Load` (zero on first write) so two workers cannot overwrite each other.

Session messages live in the brain table `session_messages`, not in `session.Store`. A collapse or a plan handoff writes the exact messages that leave the window there, so this session can search them later. They are not the checkpoint.

## Map to Azure / Lambda (later)

| Tacklr | Temporal (now) | Azure DF (later) | Lambda (later) |
|--------|----------------|------------------|----------------|
| `session.Client` | `temporal.Client` + `worker.New` | Same | Same |
| Session workflow | Workflow | Orchestration | Durable execution |
| Inference / tool | Activity | Activity | Invoke + heartbeat |
| Child / specialist | Child workflow | Sub-orchestration | Nested execution |
| HITL | Signal Resume | WaitForExternalEvent | waitForEvent |
| Progress | Workflow Streams | Event Hubs / queue | SQS / stream |
| Auth | SecretStorage + secret-free signal | orchestration input / event | invocation payload |
| Session record | `session.Store` | Same | Same |

Do not put Temporal `workflow.Context` on `session.Client`.

## Observability

The session loop starts `tacklr.turn`. Inference and tool steps inherit that span. Do not add extra wrapper spans around a step.

Host setup: `telemetry.Init` installs the process-wide OpenTelemetry providers and, when an endpoint is set, the OTLP exporters. `Dial` replaces the tracer with a replay-safe one that keeps that export configuration, then prepends Temporal’s OpenTelemetry plugin. Postgres statement spans from `postgres.Store` join the same trace: `postgres.Open` sets pgx's otelpgx tracer (`ConnConfig.Tracer`). A host that builds its own pool for `server.PostgresWireStore` sets that tracer itself.

```go
shutdown, err := telemetry.Init(ctx, telemetry.Config{
    ServiceName:  "tacklr-worker",
    OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), // Alloy / collector
    Insecure:     true, // local
})
c, err := temporal.Dial(client.Options{HostPort: temporalHost})
w := worker.New(c, cfg)
```

Span attributes are closed enums and ids (`tacklr.runtime`, `tacklr.turn.kind`, `tacklr.agent_id`, `tacklr.outcome`). Logs carry prompt length, resume counts, retries, and error text.
