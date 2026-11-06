# Sessions

Tacklr’s session API is `session.Client`. A `server.Protocol` maps wire frames to Client calls. ACP is the built-in in package `server/acp` (`acp.New`). Hosts implement `Protocol` for their own streaming and delivery; [protocol.md](protocol.md) follows a turn through it. `session` does not import protocol types. Autonomous workflows call the Client directly.

The packages split along that line. A host that only talks to sessions imports `session` (the contract: `Client`, `Store`, `Event`, `Status`) and `session/temporal` (`temporal.Client`); neither imports the harness (`tacklr`), the VFS, the brain, the orchestrator or the worker. The process that runs sessions imports `session/temporal/worker`, which runs the orchestrator (`session/orchestrator`) over the harness.

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
| **Cancel** | Abort the in-flight turn and stop child sessions (`Client.Cancel`; over ACP, `session/cancel` or a cancelled answer to a permission or a choice). A client that disconnects does not cancel: the turn goes on. The parent session stays open for a later Prompt |
| **Checkpoint** | Committed harness state inside the snapshot: the window of complete rounds, plan, user state, permission decisions, search cursors. Rows (`Harness.Checkpoint`, `session.CheckpointEntry`) in `session.Store`: one per window message, todo, user-state key, permission decision and search rest, plus the provider's count of the last request, the plan and the schema version. A Commit diffs them against the rows it last saved (`session.Diff`) and saves only the changed ones, so an append writes the new messages, not the window. Only a Commit step saves it |
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

### Events

A subscriber gets two kinds of event:

- **Durable events** are stored in the session's event log: complete messages and reasoning (with their full text), tool calls, tool results, plan updates, and turn ends. A message is stored only when it is complete.
- **Live events** (`Event.Live`) are deltas (`Event.Delta`: pieces of a message's text as the model streams it) and tool progress updates. They go straight from where the step runs to the subscribers that listen now, and are never stored. `Event.Offset` is where a delta starts in its message's text.

`Subscribe` merges both (`session.Merge`). It sends each message's deltas in order without gaps or repeats, and none after the message's complete event. The protocol server's follow of a session (`server.Peer.Follow`) sets `Event.Streamed` on a complete event to the bytes its reader already got as deltas, so a protocol sends only the rest: a reader that joins in the middle of a message, or misses deltas, still gets the whole text once.

A turn's failure is an `EventError` whose `Error` a client checks with `errors.Is` (`session.ErrModelRefused`, `ErrMaxTokens`, `ErrContextLength`, `context.Canceled`). An error value cannot cross a log, a store or a wire, so the `Event` type converts it in both directions: encoding writes its text (`Fail`) and kind (`FailKind`), and decoding gives `Error` back. No log, store or runtime converts on its own. A struct that holds an event names the field (`Event session.Event`): an embedded event would make the event's JSON methods the struct's.

The event log holds one turn. When a new turn starts (a prompt, not a resume), the runtime deletes the events of the turns before it, and the turn's first `Commit` deletes their call bodies (`Store.ClearCalls`) once it committed what they left open. Sequence numbers do not restart, and a `Subscribe` from a trimmed position starts at the oldest event kept. Within a turn there is no limit, so a park or a crash replays the whole turn.

### Prompt during a live turn

`Client.Prompt` while a turn is live or parked **queues**. It does not cancel in-flight `Invoke` or blocking tools. `Prompt` returns when the session accepted the work (queued, not yet in the window). `Subscribe` still delivers the same turn’s completion. Over ACP, a second `session/prompt` while the turn runs joins it: the turn's end answers both requests. No new ACP method.

Queued messages are appended only between rounds: no round is open and the turn is not parked. The orchestrator (`orchestrator.Run`) uses that gate. They never append while `Invoke` is running or between a `function_call` and its result. A drain that writes messages forces another inference instead of complete.

`Prompt.Auth` and `Prompt.State` apply immediately (tokens and `userState` must not wait). `MCPServers` apply on the next idle construct, not the live harness.

`session/cancel` (`Client.Cancel`) remains the abort path (over ACP, a cancelled answer to a permission or a choice is one too): it cancels the turn, stops children, and **drops unread inbox items**. Close drops the inbox. Resume does not start from a queued Prompt; HITL stays parked until the open round's calls finish after Resume, then the inbox drains. A Commit is never cancelled: a cancel waits for it, so the turn always knows the snapshot revision it saved.

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
| Orchestration | `orchestrator.Run` over the worker's `orchestrator.Runtime` (the worker only runs steps, signals, and child workflows) |
| Steps | Activities: `Commit`, `Inference` (one model request), `Tool` (one tool call) |
| Specialist | Child workflow |
| Harness routing | Each worker also polls its own task queue. A session's first harness step asks a worker on the shared queue for its queue (`WorkerQueue`), and the workflow sends the session's `Commit`, `Inference` and `Tool` steps there, so they share that worker's harness. A step that does not start within `temporal.Config.WorkerQueueTimeout` (zero: 10 seconds; the worker is gone) goes back to the shared queue, and the worker that takes it builds the harness again. The session's end closes its harness (`CloseHarness`); a harness unused for `worker.Config.HarnessIdle` (zero: 10 minutes) closes too. |
| Worker capacity | `worker.Config.MaxConcurrentActivityExecutionSize` caps the steps one worker runs at once. Zero is Temporal's default. `worker.Config.MaxHarnesses` (zero: 256) caps the harnesses one worker keeps; past it the harness unused longest closes. |
| Reply fan-out | The calls of one reply run as one `Tool` activity each, at most `temporal.Config.MaxConcurrentToolCalls` at once (zero: 32); the rest start as those finish. Results stay in call order. |
| Children | A session runs at most `temporal.Config.MaxChildren` children at once (zero: 1,000, under Temporal's default limit of 2,000 incomplete child workflows). A start past it is a correction for the model. |
| Activity timeout | `temporal.Config.ActivityTimeout` is StartToCloseTimeout for `Commit`, `Inference` and `Tool`. Zero is 10 minutes. A tool whose `ToolConfig.Timeout` plus one minute is longer gets that instead. `WorkerQueue` is a local activity (one history marker, no task) and `CloseHarness` has 30 seconds. |
| Heartbeat | `temporal.Config.HeartbeatTimeout`. Zero is 30 seconds. |
| Activity retries | `temporal.Config.ActivityAttempts` is Temporal MaximumAttempts. Zero is 3. 1 means no retry. Retries back off from one second, doubling, to thirty. An activity retries `tacklr.ErrNetwork` (wrap a dial, read, timeout, or upstream 408/429/5xx at the call), a model refusal, and a stale checkpoint. Any other error stops on the first attempt. |
| History size | A session continues as new where its turn waits: idle (`Recv`), waiting on background jobs (`Wait`), or waiting on blocking specialists (`Await`). It does so once Temporal suggests it (`GetContinueAsNewSuggested`), or once its history reaches `temporal.Config.ContinueAsNewEvents` events when that is set. Children continue as new too. The next run gets the turn's `orchestrator.Carry` (open round, last commit revision, inbox, queued host state, MCP, running jobs, the wakes that waited for the running turn, whether the turn finished or was waiting), its worker's route, and the event stream's log and offsets, so subscribers keep reading. `orchestrator` owns the carry, so every runtime keeps the same state across a restart. Running children carry over by id: children use `ParentClosePolicy` ABANDON, so continue-as-new does not stop them. A child sends its result to its parent's current run with the `tacklr_child_done` signal (an empty run id names the current run). |
| Tool results | A tool call is one `tacklr.Call`: what the model asked, how its last run ended (`Done`, `Park`, `Await`), the jobs it started and stopped, and its `Body` (its result message, park, OnCall layers and state writes), which only the harness reads. A `Tool` step runs the call (`Harness.RunCall`) and keeps its body in `Store.PutCall` under the session and the call key, and the call it returns has no body, so history holds each result zero times: four calls of 100 KB each make about 25 KB of history (85 events), the same as four calls of 10 KB. `Commit` and the call's next run read the body back (`Store.Calls`). The stream's tool-result event carries the call key, not the result; `Client.Subscribe` reads the result text from the stored body (`tacklr.ResultText`). The fallback `EventLog` is not history and gets the whole event. A retried `Tool` step puts the same key again, so it leaves one body. |
| Large payloads | Model replies are activity results in history, and the reply is the `Commit` input too. Temporal limits one payload to 2 MB by default. Configure Temporal's External Storage on the client you pass to `temporal.Open` and `worker.New` (`client.Options.ExternalStorage`, with the SDK's S3 or GCS driver): payloads over its threshold (256 KiB by default) go to the bucket and history keeps a reference. Workers inherit it from that client. |
| Progress | Durable events: Workflow Streams (`events`, `retry`, `close`), or the `Fallback` log when `DisableStreams` is set, never both. A new turn truncates the stream. Deltas: the worker's live endpoint (below). |
| Live deltas | The model call runs on a worker, and the client is elsewhere, so the client dials the worker. `worker.Config.Live` names the URL where the host serves `Worker.LiveHandler()` (with its own TLS) and an HMAC key; `temporal.Config.Live` has the same key. The workflow learns its worker's URL when it routes (at its start) and answers it to the `tacklr_live` query. `Subscribe` asks, dials `GET /tacklr/live/{session}` with the session's bearer token, and reads one JSON delta per line; it asks again when the stream ends, which finds the worker that took a rerouted session. Without `Live`, clients get whole messages only. |
| HITL | Signal `Resume` (never inside an activity) |
| Calls left after HITL | The open round in workflow state (`orchestrator.State.Round`): the parked call runs with its answer, then each call that has not run yet |
| Spawn specialist | Child `SessionWorkflow` (wait for started). `ParentClosePolicy` is ABANDON, so a child outlives a continue-as-new of its parent; the parent cancels its running children by id (`RequestCancelExternalWorkflow`) on Cancel, Close and the end of its run. Tools call `CallContext` job methods; the session starts and cancels the jobs each call's run asks for (`Call.Starts`, `Call.Cancels`, kept in `State.Jobs`). A job reports its result once, by the `tacklr_child_done` signal, which reaches a parent that continued as new. A child that parks is resumed by its own id. |

The worker registers `SessionWorkflow`, `Commit`, `Inference`, `Tool`, `EmitEvent`, `RunJob`, `WorkerQueue`, `CloseHarness` and `Forget`. A closed session runs `Forget`, which deletes its saved messages and its jobs' from the brain, then `CloseHarness`. Steps do not publish complete, yield, or turn-ending error. The workflow commits `Status`, then `EmitEvent` publishes the matching stream event. A blocking specialist's call waits on the child (`Call.Await`); the orchestrator starts it, waits, and runs a `Tool` step for that call with the child's result. It does not park the parent.

## Durable objects

Package `session/durableobject` runs one session per durable object: an object with its own ordered storage, alarm and address, as a Cloudflare Durable Object has. The package is plain Go and builds for WebAssembly. A platform implements `Host` for each object (its storage, its alarm, and the other objects) and `Platform` for reaching objects by id, calls the session's `Object` for every request and alarm, and keeps the object alive until `Object.Idle` returns. `NewClient` is the `session.Client` over a `Platform`. `dotest.Platform` runs objects in memory for tests: it stops them, fires their alarms and fails chosen writes the way a platform can.

Where objects are remote, they reach each other over the object protocol: `Handler(ref)` serves an object over HTTP (create, wake, status, jobs, head, and an NDJSON event stream), and `Remote(client)` is the `Ref` on the caller's side, whose client's Transport reaches the one object. An error keeps its kind (`session.ErrNotFound`, `ErrExists`, `ErrInvalid`) across the protocol. The protocol has no authentication and carries credentials in clear text, so a platform keeps it inside its own network. `dotest.NewWire` sends every call over it, and its `Evict` breaks the object's open streams.

### Cloudflare Workers

Package `session/durableobject/cloudflare` (built with `GOOS=js GOARCH=wasm`) runs each session as a SQLite-backed Durable Object, and serves the host's protocol server (ACP) from a connection object for each WebSocket. The host's `main` calls `cloudflare.Serve` with its `App`: the two Durable Object bindings (`Sessions`, `Connections`), the `durableobject.Config` built from the Worker's env, and the `server.Server` over a `durableobject.Client` of the session objects. It is the same server a host on net/http runs (see [protocol.md](protocol.md)), and `Client.WireStore` keeps each session's ACP wire state in the session's own object. `shim.js`, in the package directory, is the JavaScript half: the host's Worker entry imports it with Go's `wasm_exec.js` and the compiled program, and exports its `Session` and `Connection` classes. `examples/cloudflare` is a complete Worker with an end-to-end test.

| Concern | On Workers |
|---------|------------|
| Program | One Go program per isolate serves every object in it. `durableobject.Registry` routes requests by object id, makes a new `Object` when the platform starts a new instance of one, and keeps at most `App.MaxIdleObjects` idle objects in memory (zero: 64); a dropped object starts again from its storage. |
| Storage | One SQLite table per object (`tacklr_kv`). A value is split into 1 MiB parts, because a row holds at most 2 MB; a `Write` is one `transactionSync`. |
| Between objects | The object protocol through each object's stub (`fetch`), inside Cloudflare's network. The protocol server is the only way in. |
| Connections | The Worker sends each WebSocket upgrade to a new connection object (`newUniqueId`). The object runs `Server.Connect` on the request (so the host's authentication runs there), accepts the socket with the standard WebSocket API, which keeps the object in memory while the socket is open, and serves it with `Connection.Serve`. A deploy closes the socket; the client connects again and loads its session. |
| Alarm | The platform's one alarm per object. The alarm handler waits until the object runs nothing (`Object.Idle`), which keeps the object alive while the step it restarted runs (up to 15 minutes). |
| Eviction | A step in flight keeps the object alive while it waits on the network. An object evicted anyway loses only its memory: the watchdog alarm, or the next request, replays its journal from SQLite. |
| Size | The example's program is about 16 MB (4 MB gzipped); Workers allow 64 MiB. CI fails it over 24 MiB. |

```go
obj := durableobject.NewObject(host, durableobject.Config{Agent: agent, Sealer: sealer, Jobs: jobs})
sessions := durableobject.NewClient(platform)
```

| Tacklr concept | Durable objects |
|----------------|-----------------|
| Agent session | One object, which holds the session's storage |
| State | In memory while the object lives: the session's meta, mailbox, status, outbox and store heads. The object reads storage once when it starts and writes every change to storage before it changes memory. |
| Orchestration | `orchestrator.Run` over the object's `orchestrator.Runtime`, on a goroutine of the object. The goroutine ends whenever the session has nothing to do; the next wake starts it again. |
| Steps | Calls of the object: `Commit`, `Infer` (one model request), `Tools` (one goroutine per call, at most `Config.MaxToolCalls` at once; zero: 32), `Job` (a named `session.JobHandler`) |
| Durability | A journal in the object's storage. Each runtime call writes once: its journal entry, the mailbox wakes it took, its durable events, what it stored (a commit's changed checkpoint rows) and its letters to other objects land together or not at all. An object that stopped (evicted, redeployed) runs `orchestrator.Run` again on its next request or alarm, and each call returns its entry until the journal ends; the first call past the end acts. |
| Journal size | One turn. When the session would wait, it suspends instead: its state goes to its carry (`orchestrator.Carry`), and the journal and the wakes it took are deleted in one write. |
| Restart mid-step | A step sets the object's alarm `Config.Watchdog` ahead (zero: one minute), and sets it again while it runs. An object that stops mid-step is restarted by the alarm, and the unfinished step runs again (a tool with side effects uses its call id as the idempotency key). |
| Step retries | A step that fails on the network (`tacklr.ErrNetwork`), on a refusal or on a stale checkpoint runs again, up to `Config.StepAttempts` runs (zero: 3), backing off from `Config.RetryBackoff` (zero: one second), doubling, to thirty seconds. |
| Tool results | A call writes its body (by call key), its result event and itself in one write when its run ends, so a step that runs again runs only the calls that did not end. |
| Progress | Durable events: the object's event log, each event once. Deltas and tool updates: from the object's memory to its subscribers, never stored. |
| Status | `Status` and `Jobs` answer from the session's last write, without running the session. |
| Credentials | `Prompt`, `Resume` and a child's start carry them to the object, which seals them with `Config.Sealer` into its storage. The mailbox, the journal and the outbox hold them only sealed or not at all. |
| Children | A child is its own object. Its start, a cancel, a close and its end are letters: written with the step that sends them, then delivered in order until each is done, again on the alarm after a failure. A child starts with its parent's credentials, sealed for it. Once the parent took a child's end, it closes the child, whose object deletes what it saved. |
| Close | Callers see the session as gone at once. The object then stops its running jobs, closes the children it still has, deletes its messages from the brain and deletes its storage. Closing again is not an error. |
| Workspace | None: a durable object has no FUSE and no shell. |

## Child sessions

A child that a tool starts (a specialist or a named job) is a task session (`orchestrator.Identity.Task`): its runtime puts its task first in its mailbox, as its first prompt, and the session ends when that turn ends and is not parked. Its end reaches the parent as a job wake (`orchestrator.JobEnd`). It is otherwise a session like any other, so it suspends and continues the same way.

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
| `Client.Cancel` | Stop all children, then abort the parent turn. The parent session stays open for a later Prompt |
| The client disconnects | Nothing stops: the turn and its children go on, and a client that loads the session follows them again |
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

A commit fits the round into `MaxWindowSize`: it appends the round, collapses the window into a summary, or cuts the round's largest results. The size of each request comes from the provider. See [context.md](context.md).

### Steps

A runtime implements one interface, `orchestrator.Runtime`: the steps below, one mailbox read (`Next`), and running jobs (`Start`, `Cancel`). `orchestrator.Run` and the turn are the same for every runtime: one session function in `orchestrator.Run` decides what every wake does given the session's state (idle, running a turn, waiting on jobs, parked), and the session owns each job's lifecycle (`State.Jobs`: started by id, running, ended into its waiting call's result or an inbox message).

Each non-deterministic action is one step, so a session replays safely: the runtime records each completed step and its result, and a replay returns the recorded result instead of running the step again. Neither the harness nor the snapshot records which steps ran.

| Step | Does | Writes the snapshot |
|------|------|---------------------|
| `Infer` | One model request on the committed window. Returns the reply. | No |
| `Tool` | One tool call (`tacklr.Call`), with the input of its run: the answer to its park, or its child's result. Returns the call as the run left it: its result, a park, or a wait on a child, the children it starts or cancels, and in its body the shared-state writes it made (user state, plan changes, permission decisions, search cursors). | No |
| `Commit` | Host state, a complete round (each call's writes in call order, then its messages, then collapse or trim), and user messages. | Yes, the only one |

The open round lives in workflow state (`orchestrator.State.Round`): the reply and its calls. The loop runs every call that has not run at once (`Runtime.Tools`: one `Tool` activity each on Temporal; goroutines in a simple runtime), puts the calls back in call order, starts the jobs they ask for (a job id comes from its call, so a job is started once), waits for blocking jobs' ends and runs their calls again with the results, and commits the round once each call has its result. Plan changes are replayed at Commit (`planOp`), so plan changes of calls that ran at once all land.

A runtime may run a step more than once before it records it (a worker dies before the step reports). `Infer` and `Tool` change nothing a later step reads, so that is safe; a tool with external side effects uses its call id (`CallContext.CurrentToolCallID`) as the idempotency key. `Commit` is idempotent: its `base` is the revision the last Commit returned, and a Commit that finds the snapshot past it already saved and applies nothing again. A cancel does not stop a Commit. Steps carry only what changes per step; the session's identity and MCP servers are the runtime's, which builds each step's harness from them. A cancel reaches a running step on its next heartbeat reply (`worker.Config.HeartbeatThrottle` bounds how long that takes, 2 seconds by default). Tracing is the host's to set up: [telemetry.md](telemetry.md). A turn's span starts once (in a `workflow.SideEffect` on Temporal, a journal entry on Durable Objects) and each step carries its context, so the standard SDK tracer provider works and Temporal's tracing plugin is not needed.

### Harnesses

A step needs the session's harness: its `tacklr.Harness` (tools, MCP connections and their tools, skills, the system prompt) and its VFS. A worker keeps one harness per session (its harness cache) and every step of the session it runs uses it, so a reply's calls do not each open the VFS and connect every MCP server. The harness is named by a key of everything it is built from: the specialist, the mounts and their auth, the MCP servers and the job names. A step whose key differs (a prompt changed the MCP servers, auth was refreshed) closes the old harness and builds a new one. MCP servers connect and list their tools when the harness is built, because the model's tool list needs them. The tool list and system prompt are built in a fixed order from those inputs, so every step sends the model the same request prefix, and a harness built again on another worker sends the same one: the provider's prompt cache holds.

Committed state is the store's. A step reads the snapshot's revision (`Store.Revision`) and loads the snapshot only when it moved past the one the harness holds, after a commit on another worker. The calls of a reply read at once; `Commit` holds the harness alone. A Commit that fails drops the harness, since its state may be ahead of the store. A step the turn moved past (a cancelled tool that has not returned) never blocks the session: a writer that finds it still reading builds a new harness.

The routing that sends a session's steps to the worker holding its harness is the runtime's (Temporal: a task queue per worker, see above). Without it a step still works: the worker it lands on builds the harness.

A turn parks on one interrupt at a time. When several calls of a reply park, the first in call order parks the turn; the others count for nothing and run after resume. A resume runs the parked call with its answer, then the calls that have not run. A prompt that moves past an open round (a turn that failed) gives each call without a result a cancelled result and commits the round before the prompt.

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

`SecretStorage` is not `session.Store`. Client and worker must share one instance: `session/postgres` (`DB.Secrets`), a store of the host's own over its vault, or `MemorySecretStorage` when they share a process. There is no default: a private memory map per process looks like a successful Prompt and then remounts nothing. It holds only these session-scoped tokens (an end user's delegated OAuth tokens, per-session object-store credentials); service-to-service credentials stay with the providers the host builds.

### Durable stores in Postgres

`session/postgres` keeps `session.Store`, `SecretStorage` and `EventLog` in Postgres over a pool the host owns, so every worker and client sees one copy:

```go
db, err := sessionpg.New(pool, sessionpg.Options{}) // schema "tacklr"; tables are schema-qualified
if err := db.Setup(ctx); err != nil { /* ... */ }    // idempotent; run on each deploy
sealer, err := session.NewAESGCM(key)                 // 32 bytes; or a session.Sealer over the host's KMS
store, secrets, events := db.Store(), db.Secrets(sealer), db.EventLog()
defer events.Close()
```

- **Store:** one row per checkpoint row (`session_checkpoint_rows`) and one per session holding its revision. `Save` compares and bumps the revision and applies the changes in one transaction, so of two saves from one revision exactly one wins and the loser's changes are not applied. Call bodies are keyed by session and call key; `Calls` reads every key in one query; `Delete` removes the checkpoint and its calls together.
- **SecretStorage:** each session's bag is sealed before it is written, with the session id as associated data, so the row holds no plaintext token and one session's row does not open as another's.
- **EventLog:** a counter row numbers each session's events in the statement that writes them, so sequences have no gaps. The statement also `NOTIFY`s the session id (never the event: a payload is at most 8000 bytes). One listening connection per `EventLog` wakes its subscribers, which read what they have not seen; after a reconnect it wakes them all, so nothing appended meanwhile is missed. `CloseSession` deletes the log and ends its subscribers in every process.

A store of the host's own runs `session/storetest`, the contract these and the in-memory stores meet.

After HITL or a worker restart, the next Prompt/Resume supplies tokens and they are Put again. A retry on another worker remounts only if that worker can `Get` the same store.

A 401 during a turn fails that activity on the first attempt. The host sends a new token on the next `Prompt` or `Resume`. There is no live callback from an activity into ACP.

MCP `Env`/`Headers` are stripped with `DurableConfigs` before Temporal payloads. `CredentialRef` stays and is resolved at activity time.

Encrypt remaining work-item payloads (prompt text, tool args, HITL bytes) at rest with a Temporal payload codec if the store is untrusted. That is defense in depth for non-token data, not the token control.

## Protocol contract

`server.Protocol` is the host extension point; ACP is the built-in (`server/acp`). [protocol.md](protocol.md) follows a turn from the client's prompt to the model's answer through the protocol server: its key abstractions, the cancel and reconnect rules, who owns each piece of data, and how to write a protocol of your own.

| Client | ACP |
|--------|-----|
| `CreateSession` | `session/new` |
| `Prompt` + `Subscribe` | `session/prompt` (the connection follows the session) |
| `Resume` | `session/resume`, or the answer to `session/request_permission` / `elicitation/create` |
| `Cancel` | `session/cancel`, or a cancelled answer to a permission or a choice |
| `Close` | `session/close` |
| `Prompt.Auth`, `Prompt.MCPServers` | `_tacklr/vfs/bind`, and `mcpServers` on `session/load`: the connection keeps them for the session's next prompt |

## Session data planes (frozen)

Three stores. Do not add a fourth. Do not copy a field from one into another except as a turn-scoped cache that the canonical store then owns.

| Plane | Lifetime | Owns | Never |
|-------|----------|------|-------|
| **session.Store** | One session | The checkpoint (rows only the harness reads): window of complete rounds, plan, `userState`, permission decisions, search rests; each tool call's body (`PutCall`/`Calls`), cleared when the next turn starts and deleted with the session | Tokens, file bytes, MCP env/headers, identity (specialist, parent, mounts), child workflow futures, host config (search namespace, principals, `AllowTools`) |
| **Wait loop** | In-process `sessionProc` / Temporal workflow replay | `Identity` (specialist, parent, mount recipes), MCP Durable topology, child futures, the open round (the reply and its calls), the last Commit's snapshot revision, Prompt/job inbox (steer + auto-collected jobs), host state not yet committed, secret-free `ApplyAuth` on the current signal, `Status` | Window, plan, tokens, file bytes, `userState`. Inbox is not `session.Store`. |
| **SecretStorage** | Session, deleted on Close | VFS credentials | Snapshot rows, Temporal payloads |

`Prompt.State` / `Resume.State` / `CreateSession.State` are sent once, to the next step, which writes them into the checkpoint's `userState`. The wait loop holds them only until then, so a tool's later `StateSet` of the same key is not overwritten.

`Close` deletes the snapshot and the secret bag; the closed session's worker deletes its messages from the brain. A new session id does not load a previous snapshot. `Save` takes the `Revision` from the last `Load` (zero on first write) so two workers cannot overwrite each other.

Session messages live in the brain table `session_messages`, not in `session.Store`. A collapse or a plan handoff writes the exact messages that leave the window there, so this session can search them later. They are not the checkpoint.

## Map to other runtimes

| Tacklr | Temporal (now) | Durable objects (now) | Azure DF (later) | Lambda (later) |
|--------|----------------|-----------------------|------------------|----------------|
| `session.Client` | `temporal.Client` + `worker.New` | `durableobject.Client` + `NewObject` | Same | Same |
| Session workflow | Workflow | One object | Orchestration | Durable execution |
| Inference / tool | Activity | Journaled call | Activity | Invoke + heartbeat |
| Child / specialist | Child workflow | Child object | Sub-orchestration | Nested execution |
| HITL | Signal Resume | Mailbox wake | WaitForExternalEvent | waitForEvent |
| Progress | Workflow Streams | Object event log | Event Hubs / queue | SQS / stream |
| Auth | SecretStorage + secret-free signal | Sealed in the object | orchestration input / event | invocation payload |
| Session record | `session.Store` | The object's storage | Same | Same |

Do not put Temporal `workflow.Context` on `session.Client`.

## Observability

A turn is one trace, from the client's request through each system it touches to the answer: see [telemetry.md](telemetry.md) for the spans, logs and metrics, how the trace crosses each system, and how a host sets it up.
