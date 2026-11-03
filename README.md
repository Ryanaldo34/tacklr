# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

Tacklr is an opinionated Go SDK for building agent harnesses. It is a framework: it says how a turn runs, how tools execute, how context is structured around a plan, and how a session survives interrupts and restarts.

You bring a model, tools, and the storage you already use. Tacklr sits in the middle of that stack and keeps the run deterministic and replayable from the harness's point of view, even though the model is not.

**Tacklr supports Linux only.** It builds, runs, and is tested on Linux. Other operating systems are not supported, and the module does not build for them.

```bash
go get github.com/ryanaldo34/tacklr
```

---

## Why it exists

Work spans many model calls, tools hit real systems, and the window fills with things that no longer matter. Tacklr is built around four ideas.

**Context is structured around the current work.** The agent writes a plan, works a to-do, and on `complete_todo` the window is rebuilt as a hand-off for the next work (Adaptive Case Management). Unused history does not stay in the prompt. Specialists are the same idea at a larger grain: a nested session that returns only what the parent asked for.

**The agent's world is bounded.** A virtual filesystem gives one path API over the mounts you attach: local disk, S3, Azure Blob, Google Drive and Docs, Microsoft Graph, and git. The agent sees `/workspace/work/notes.md`, not a host path or a bucket key. Credentials travel on the prompt (`Prompt.Auth`, `Resume.Auth`), never in checkpoints or workflow history.

**Sessions live in the cloud.** Hosts call a `session.Client`. A durable runtime (Temporal today, others later) runs the session, so it outlives any one process. A human-in-the-loop question parks the session until `Resume`.

**Knowledge is queried, not stuffed into the window.** The optional brain is a host-owned store with hybrid search, an optional graph, and namespaces. The agent asks when it needs a fact.

---

## A turn

One `Prompt` or `Resume` is a turn.

- **Every non-deterministic action is one step.** A model request is an `Infer` step. Each tool call is its own `Tool` step. The durable runtime records each completed step and its result, so a replay never runs a completed step again.
- **The window holds complete rounds only.** A round is a user message, or one model reply plus a result for every call in it. A reply with calls opens a round. The round stays out of the window until each call has its result. The model is not called again until the round commits.
- **The calls of one reply run at once.** Their results land in call order. A call's writes (session state, plan changes, permission answers) apply when its round commits, in call order.
- **`Commit` is the only step that writes the snapshot.** It is idempotent: it applies its input once, even when the runtime runs it again.
- **Park and resume.** A tool can ask the user a question. The turn parks on the first interrupt in call order. `Resume` runs the parked call again with the answer; the calls that already finished do not run again.
- **Children.** The model starts specialists with the built-in `spawn_specialist`. Tools start Runtime job workers with `Schedule`. A blocking child's output is its call's result.
- **One harness per session per worker.** A step needs the session's harness: its tools, MCP connections and their tools, skills, system prompt and VFS. A worker builds it once and every step of the session it runs shares it; committed state in it reloads only when the snapshot's revision moved. The tool list and system prompt are built in a fixed order, so every request has the same prefix and the provider's prompt cache holds. Temporal sends a session's steps to the worker that holds its harness (a task queue per worker), and to another worker if that one is gone.
- **Collapse.** When a commit puts the window over 85% of its size, everything after the first message and the plan collapses into one summary. The collapsed messages are saved, and `search` finds them. A round still too large has its biggest results trimmed; with `AgentOptions.OverflowPath` each full result is written to the workspace first.

```text
prompt → Commit → Infer → Tool, Tool, Tool (at once) → Commit → Infer → … → complete
                                   └ park → Resume → Tool (the parked call) → Commit
```

The planning built-ins (`create_plan`, `list_plan`, `edit_plan`, `complete_todo`) belong to the harness. Your tools cannot rewrite the plan.

---

## Get started

A host builds an `AgentOptions`, starts a worker that runs sessions, and talks to sessions through a `session.Client`. A host that only talks to sessions imports `session` and `session/temporal`, not the harness.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/worker"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/web"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The model: any tacklr.Model. This one speaks the OpenAI Responses API.
	model := openai.New(&http.Client{Timeout: 2 * time.Minute}).
		WithURL(os.Getenv("OPENAI_BASE_URL")).
		WithApiKey(os.Getenv("OPENAI_API_KEY")).
		WithModel(os.Getenv("OPENAI_MODEL"))

	// The brain: a store (Postgres here) behind the knowledge engine.
	store, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"), postgres.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	finding := brain.KindSpec{Kind: "Finding", Description: "A research finding", IsParent: true}
	if err := store.Setup(ctx, finding); err != nil {
		log.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithKinds(finding))
	if err != nil {
		log.Fatal(err)
	}
	ns, err := brain.ParseNamespace("org", "acme")
	if err != nil {
		log.Fatal(err)
	}

	exa := web.NewExa(os.Getenv("EXA_API_KEY"))
	agent := tacklr.AgentOptions{
		Name:            "researcher",
		SystemPrompt:    "You are a careful research agent.",
		Model:           model,
		Brain:           eng,
		SearchNamespace: ns,
		BrainWriteKinds: []string{"Finding"},
		Tools:           []*tacklr.Tool{web.WebSearch(exa), web.WebFetch(exa), weatherTool(http.DefaultClient)},
		// The agent's world: /workspace/work on local disk.
		OpenVFS: vfs.Tree(vfs.At("work", vfs.Local("/srv/tacklr/work"))),
	}

	// The durable runtime: Temporal. The worker runs the sessions; the
	// client starts and talks to them. Both use the same session stores:
	// in memory here (one process), shared stores in production. For
	// tracing, add Temporal's OpenTelemetry plugin to client.Options.Plugins
	// (docs/session.md).
	c, err := temporal.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_ADDRESS")})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	records, secrets := session.NewMemoryStore(), session.NewMemorySecretStorage()
	w := worker.New(c, worker.Config{Agent: agent, Store: records, Secrets: secrets})
	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
	defer w.Stop()
	sessions := temporal.Open(c, temporal.Config{Store: records, Secrets: secrets})

	// The host API: create a session, follow its events, prompt it.
	id, err := sessions.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		log.Fatal(err)
	}
	head, err := sessions.Head(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	sub, err := sessions.Subscribe(ctx, id, head)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	if err := sessions.Prompt(ctx, id, session.Prompt{Text: "What changed in Go 1.27? Save what you find."}); err != nil {
		log.Fatal(err)
	}
	for ev := range sub.Events() {
		switch ev.Type {
		case session.EventMessage:
			fmt.Print(ev.Content)
		case session.EventInterrupt:
			fmt.Println("\nwaiting for the user:", string(ev.Data))
			return
		case session.EventComplete, session.EventError:
			return
		}
	}
}
```

`weatherTool` is the tool in [Tools and CallContext](#tools-and-callcontext). With a `Brain`, the agent gets `search`, `open`, `related` and `save` (and `link` when the engine has a graph). With `OpenVFS`, it gets `read`, `write`, `write_document`, `write_spreadsheet`, `run_command`, and `index_file`/`unindex` when a brain is set too.

---

## Key interfaces

### Model and ModelRequest

The model is an interface. `Invoke` and `CountTokens` take one `ModelRequest` with everything for that request, so a model holds no per-request state and one value is safe to share across sessions. The harness builds every request in one place, so a token count measures exactly what the request sends.

```go
type ModelRequest struct {
	Messages     []*message.Message
	Tools        []*Tool
	SystemPrompt string
	CacheKey     string // the session id, for provider prompt caching
}

type Model interface {
	Invoke(ctx context.Context, req ModelRequest) (chan LLMResponseChunk, error)
	CountTokens(ctx context.Context, req ModelRequest) (int, error)
	MaxContextWindow() (int, error)
	ModelTelemetryIdentity() telemetry.ModelIdentity
	SupportsMIME(mimeType string) bool
}
```

A minimal model:

```go
type echoModel struct{}

func (echoModel) Invoke(ctx context.Context, req tacklr.ModelRequest) (chan tacklr.LLMResponseChunk, error) {
	ch := make(chan tacklr.LLMResponseChunk, 1)
	last := req.Messages[len(req.Messages)-1]
	ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "you said: " + last.Content, IsComplete: true}
	close(ch)
	return ch, nil
}

func (echoModel) CountTokens(ctx context.Context, req tacklr.ModelRequest) (int, error) {
	n := len(req.SystemPrompt)
	for _, m := range req.Messages {
		n += len(m.Content)
	}
	return n / 4, nil
}

func (echoModel) MaxContextWindow() (int, error)    { return 128_000, nil }
func (echoModel) SupportsMIME(mimeType string) bool { return strings.HasPrefix(mimeType, "text/") }
func (echoModel) ModelTelemetryIdentity() telemetry.ModelIdentity {
	return telemetry.ModelIdentity{Provider: "echo", Model: "echo-1"}
}
```

`openai.New` is the built-in client for OpenAI-compatible endpoints.

### Tools and CallContext

A tool is a Go function. `NewTool` reads its arguments struct (`json` and `desc` tags) to build the JSON schema. Give a tool its clients by closing over them in the constructor; a test passes a fake client to the same constructor.

```go
type weatherArgs struct {
	City string `json:"city" desc:"City name"`
}

func weatherTool(client *http.Client) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "get_weather",
		Description: "Return the current weather for a city.",
		Handler: func(ctx context.Context, args weatherArgs) (string, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://wttr.in/"+url.PathEscape(args.City)+"?format=3", nil)
			if err != nil {
				return "", err
			}
			resp, err := client.Do(req)
			if err != nil {
				return "", err
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			return string(body), err
		},
	})
}
```

A handler that also takes `cc *tacklr.CallContext` gets the running call. The harness makes one for each call; a tool never makes one.

| Method | What it does |
|--------|--------------|
| `StateGet`, `StateSet`, `StateDelete` | Session key-values. A call reads its own writes first; its writes apply when its round commits. Hosts set values on `CreateSession.State`, `Prompt.State`, `Resume.State`. |
| `Park(intr)` | Asks the user. The call parks; after `Resume` it runs again and `Park` returns the answered interrupt. |
| `Schedule(ctx, JobRequest)` | Starts a job worker. With `Wait`, the job's output is the call's result; without it, the output arrives later as a message. |
| `Jobs`, `CancelJob` | Lists and stops this session's jobs. |
| `EmitUpdate` | Sends a progress event to the client. |
| `CurrentToolCallID` | The call's id. A runtime may run a call again when a worker dies before it reports, so a tool with external side effects passes this id as its idempotency key. |

```go
type deployArgs struct {
	Service string `json:"service" desc:"Service to deploy"`
}

func deployTool() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "deploy",
		Description: "Deploy a service to the environment in session state.",
		Access:      tacklr.ToolWriteAccess,
		Handler: func(ctx context.Context, args deployArgs, cc *tacklr.CallContext) (string, error) {
			// Ask the user before a production deploy. The call parks here; after
			// Resume it runs again and Park returns the answered interrupt.
			answered, err := cc.Park(&tacklr.UserSelectionInterrupt{
				Question: "Deploy " + args.Service + "?",
				Options:  []tacklr.UserChoice{{Title: "Deploy"}, {Title: "Cancel"}},
			})
			if err != nil {
				return "", err
			}
			if answered.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title != "Deploy" {
				return "deploy cancelled by the user", nil
			}
			env, _ := cc.StateGet("env") // host state from CreateSession/Prompt.State
			cc.EmitUpdate("starting the pipeline")
			// A job worker; with Wait the job's output is this call's result.
			_, err = cc.Schedule(ctx, tacklr.JobRequest{
				Name: "pipeline",
				Task: fmt.Sprintf(`{"service":%q,"env":%q,"key":%q}`, args.Service, env, cc.CurrentToolCallID()),
				Wait: true,
			})
			return "", err
		},
	})
}
```

A tool with `ToolWriteAccess` is locked until the agent has a plan (`create_plan`). Details: [docs/tools.md](docs/tools.md).

### Interrupt

An interrupt is a typed question a call parks on. The built-ins are `UserSelectionInterrupt` (`ask_user_choice`), `ToolPermissionInterrupt`, and `AuthExpired`. A custom one implements `Interrupt` and is registered once, so a parked call can be read back from the snapshot. `Park` fails for a type that is not registered.

```go
type approvalInterrupt struct {
	Amount   int  `json:"amount"`
	Approved bool `json:"approved"`
}

func (a *approvalInterrupt) TypeName() string           { return "spend_approval" }
func (a *approvalInterrupt) Serialize() ([]byte, error) { return json.Marshal(a) }
func (a *approvalInterrupt) Return(answer []byte) error { return json.Unmarshal(answer, a) }
func (a *approvalInterrupt) Error() string              { return fmt.Sprintf("approval needed for %d", a.Amount) }

func init() {
	tacklr.RegisterInterrupt(func() tacklr.Interrupt { return &approvalInterrupt{} })
}
```

The client gets a `session.EventInterrupt` whose `MessageID` is the call id and whose `Data` is the serialized interrupt. It answers with `Client.Resume(ctx, id, session.Resume{Responses: map[string][]byte{callID: answer}})`.

### ToolInterceptor, OnCall and AllowTools

A `ToolInterceptor` wraps every call. It can short-circuit, or change the `ToolOutcome` that `next` returns (its `Output` and its window `Effect`). An `OnCallFunc` runs before one tool's handler and may return an interrupt to park on; `ToolPermissionOnCall` asks for permission and remembers allow-always and reject-always. `AllowTools` grants named tools up front.

```go
func auditInterceptor(ctx context.Context, inv tacklr.ToolInvocation, next tacklr.ToolCallFunc) (tacklr.ToolOutcome, error) {
	out, err := next(ctx, inv)
	slog.InfoContext(ctx, "tool call", "tool", inv.Tool.Name(), "call", inv.CallContext.CurrentToolCallID(), "error", err)
	return out, err
}

func gatedTool() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:    "drop_table",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, // asks once; allow-always is remembered
		Handler: func(ctx context.Context) (string, error) { return "dropped", nil },
	})
}

func agentWithPolicy(model tacklr.Model) tacklr.AgentOptions {
	return tacklr.AgentOptions{
		Model:            model,
		Tools:            []*tacklr.Tool{gatedTool(), deployTool()},
		ToolInterceptors: []tacklr.ToolInterceptor{auditInterceptor},
		AllowTools:       []string{"run_command"}, // runs without a prompt
	}
}
```

The built-ins `write`, `write_document`, `write_spreadsheet` and `run_command` ask for permission unless they are in `AllowTools`. An unknown name in `AllowTools` fails construction.

### Specialists and job workers

A specialist is a nested agent: its own model, tools and instructions, run as a child session over the parent's mounts. You declare specialists; the model decides when to start one with the built-in `spawn_specialist` (`block=true` waits and returns the specialist's output; `block=false` starts it in the background). Tools cannot start specialists.

```go
func agentWithSpecialists(model tacklr.Model) tacklr.AgentOptions {
	return tacklr.AgentOptions{
		Model: model,
		Specialists: []*tacklr.Specialist{{
			Name:         "reviewer",
			Description:  "Reviews a diff and reports problems.",
			Instructions: "Review the change. Report only real problems.",
			Model:        model,
		}},
	}
}
```

A job worker is plain Go work a tool starts with `Schedule`: a shell pipeline, a long data job. Register workers on the worker (`worker.Config.Jobs`):

```go
func jobWorkers() map[string]worker.JobHandler {
	return map[string]worker.JobHandler{
		"pipeline": func(ctx context.Context, task string) (string, error) {
			return "deployed: " + task, nil
		},
	}
}
```

A child's id comes from its call, so a call that runs again names the same child, and the runtime starts it once. `list_children` and `cancel_child` let the model see and stop jobs.

### Brain: Store, Graph and Engine

`brain.Engine` is the knowledge engine the agent's tools call. It sits over a `brain.Store` (records, parts, hybrid search, saved session messages) and, with `brain.WithGraph`, a `brain.Graph` (nodes, typed edges, link search, neighborhood walks). Each backend embeds and ranks its own data; the engine fuses the lists by reciprocal rank. `brain.NewMemoryStore` is in memory; `brain/postgres` and `brain/helixgraph` are the production backends.

```go
func memoryBrain(ctx context.Context) (*brain.Engine, error) {
	note := brain.KindSpec{Kind: "Note", Description: "A durable note", IsParent: true}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(note))
	if err != nil {
		return nil, err
	}
	ns, err := brain.ParseNamespace("org", "acme")
	if err != nil {
		return nil, err
	}
	_, err = eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{Kind: "Note", Title: "Release day", Content: "We release on Tuesdays."})
	return eng, err
}
```

The agent uses five tools:

| Tool | What it does |
|------|--------------|
| `search` | Ranked results across records, parts, links and this session's collapsed messages. With `within`, searches inside one record. With no query, lists a kind. |
| `open` | Reads one record in full (by id or indexed file path), with its kind's fields and first children. |
| `related` | A record's children, parent and siblings, and its graph neighbors. |
| `save` | Creates or updates a record of a kind in `BrainWriteKinds`. |
| `link` | Creates or removes a typed edge with a note (needs a graph). |

Every call is scoped to `SearchNamespace`, and results are filtered by `SearchPrincipals` against each object's readers. Details: [docs/knowledge.md](docs/knowledge.md).

### VFS: Provider, Tree and At

`vfs.Tree` builds the agent's `/workspace` from members. `vfs.At(name, open)` mounts a provider at `/workspace/<name>`. Built-in providers: `Local`, `S3`, `Blob`, `Drive`, `Graph`, `Git`. A custom backend implements `vfs.Provider` (`Validate`, `Stat`, `OpenFile`, `ReadDir`, `Remove`, `MkdirAll`).

```go
func workspace() vfs.OpenVFS {
	return vfs.Tree(
		vfs.At("work", vfs.Local("/srv/tacklr/work")),
		vfs.At("docs", vfs.Local("/srv/tacklr/docs")).ReadOnly(),
		vfs.At("scratch", vfs.Local("/srv/tacklr/scratch")).NotIndexed(),
	)
}

func agentWithWorkspace(model tacklr.Model) tacklr.AgentOptions {
	return tacklr.AgentOptions{
		Model:        model,
		OpenVFS:      workspace(),
		OverflowPath: "/workspace/scratch/overflow",
	}
}
```

`ReadOnly` refuses writes. `NotIndexed` makes `index_file` refuse that mount. `OverflowPath` must be on a writable mount. Writes persist when they happen; file bytes are never in a checkpoint. Details: [docs/vfs.md](docs/vfs.md).

### session.Client

`session.Client` is the host API. Protocols and autonomous hosts call it; `temporal.Client` (package `session/temporal`) implements it. That package imports no harness or worker code.

```go
type Client interface {
	CreateSession(ctx context.Context, req CreateSession) (ID, error)
	Close(ctx context.Context, sessionID ID) error
	Status(ctx context.Context, id ID) (Status, error)
	Head(ctx context.Context, sessionID ID) (Seq, error)
	Subscribe(ctx context.Context, sessionID ID, after Seq) (Subscription, error)

	Prompt(ctx context.Context, sessionID ID, msg Prompt) error
	Resume(ctx context.Context, sessionID ID, resume Resume) error
	Cancel(ctx context.Context, sessionID ID) error

	Jobs(ctx context.Context, parent ID) ([]Status, error)
}
```

A prompt while a turn runs or is parked steers it; it does not cancel the model call. `Cancel` stops the turn and its jobs; the session stays open. A `Subscription`'s `Err` says why its events stopped (nil when the host closed it). A failure event carries a `FailKind` (refused, max tokens, cancelled), so `ev.Error` is the matching sentinel on the far side of the wire. A request the client rejects before it reaches a session (state JSON cannot hold, a specialist and a worker together) is `session.ErrInvalid`.

### loop.Executor

`loop.Executor` is what a durable runtime implements. The generic session loop (`loop.Run`, package `session/loop`) calls it, so every runtime runs a session the same way; a runtime only decides where and how each step runs. The Temporal worker (`session/temporal/worker`) implements it as workflow code.

```go
type Executor interface {
	Begin(kind string, promptLen, resumes int)
	End()
	Commit(base session.Revision, in tacklr.CommitIn) (session.Revision, error)
	Infer() (tacklr.Reply, error)
	Tools(runs []tacklr.ToolRun) []ToolResult
	Emit(session.Event)
	Job(name, task string) (string, error)

	Next(block bool) session.Wake

	Start(tacklr.JobStart) error
	Cancel(id session.ID)
}
```

The contract:

- **One non-deterministic action is one step.** `Infer` is one model request, each `ToolRun` is one tool call, `Job` is one worker body. The runtime records each completed step and replays its result; neither the harness nor the snapshot records which steps ran.
- **A step may run more than once** before the runtime records it (a worker dies before it reports). `Infer` and `Tool` steps change nothing a later step reads, so that is safe.
- **`Commit` is the only snapshot writer, and it is idempotent.** `base` is the revision the previous `Commit` returned; a `Commit` that finds the snapshot past `base` already saved and applies nothing again.
- **`Tools` runs its runs at once** (Temporal: one activity each) and returns a result per run, in run order.
- **`Next` is the one mailbox read:** prompts, resumes, cancels, closes and job ends in arrival order (`block` waits for one). One session function decides what each does, given the state: a prompt starts a turn when idle and steers it when it runs, waits on jobs, or is parked; a job's end becomes its waiting call's result or, for a background job, an inbox message.
- **The session owns each job's lifecycle** (`Turn.Jobs`): it starts a job by its deterministic id, cancels it by id, and turns its end into a result. The runtime only runs it: `Start` starts it (an id already started is not started again), `Cancel` stops it, and its result comes back once, as a job-end wake.

The session's identity, MCP servers and children are the runtime's: it holds them and builds each step's harness from them. A worker keeps that harness for the session (its `HarnessCache`), and the runtime routes the session's steps to it. Details: [docs/session.md](docs/session.md).

### Store, EventLog and SecretStorage

The runtime keeps three kinds of session data apart. The client and the worker must use the same ones:

| Store | Holds | In memory |
|-------|-------|-----------|
| `session.Store` | The session's checkpoint, as opaque bytes (the window of complete rounds, plan, user state, permission decisions, search cursors), and each tool call's `Outcome` (its result text and record), so the durable runtime's history does not hold tool results | `session.NewMemoryStore` |
| `session.EventLog` | The client event stream (fallback when Workflow Streams are off) | `session.NewMemoryEventLog` (the default) |
| `session.SecretStorage` | VFS tokens. Never in a checkpoint or workflow history | `session.NewMemorySecretStorage` |

The in-memory stores work for one process only. Production passes shared stores. `Store.Save` is compare-and-swap on a `Revision`, and `Revision` lets a worker learn whether the committed state it holds is current without loading the checkpoint:

```go
type checkpointRow struct {
	cp       []byte
	rev      session.Revision
	outcomes map[string]session.Outcome
}

type mapStore struct{ rows map[session.ID]checkpointRow }

func (m *mapStore) Save(ctx context.Context, id session.ID, cp []byte, expected session.Revision) (session.Revision, error) {
	row := m.rows[id]
	if row.rev != expected {
		return "", session.ErrStaleCheckpoint
	}
	row.cp, row.rev = cp, session.Revision(fmt.Sprint(len(row.rev)+1))
	m.rows[id] = row
	return row.rev, nil
}

func (m *mapStore) Load(ctx context.Context, id session.ID) ([]byte, session.Revision, error) {
	row, ok := m.rows[id]
	if !ok {
		return nil, "", session.ErrNotFound
	}
	return row.cp, row.rev, nil
}

func (m *mapStore) Revision(ctx context.Context, id session.ID) (session.Revision, error) {
	return m.rows[id].rev, nil
}

func (m *mapStore) PutOutcome(ctx context.Context, id session.ID, callKey string, o session.Outcome) error {
	row := m.rows[id]
	if row.outcomes == nil {
		row.outcomes = map[string]session.Outcome{}
	}
	row.outcomes[callKey] = o
	m.rows[id] = row
	return nil
}

func (m *mapStore) Outcomes(ctx context.Context, id session.ID, callKeys []string) (map[string]session.Outcome, error) {
	out := map[string]session.Outcome{}
	for _, k := range callKeys {
		if o, ok := m.rows[id].outcomes[k]; ok {
			out[k] = o
		}
	}
	return out, nil
}

func (m *mapStore) Delete(ctx context.Context, id session.ID) error {
	delete(m.rows, id)
	return nil
}

func workerConfig(agent tacklr.AgentOptions, store session.Store, secrets session.SecretStorage) worker.Config {
	return worker.Config{Agent: agent, Store: store, Secrets: secrets, Jobs: jobWorkers()}
}
```

`Close` deletes the session's checkpoint and secrets; the session's worker deletes its saved messages from the brain as it closes.

### server.Protocol

A `server.Protocol` maps a wire protocol onto a `session.Client`. ACP (`server/acp`) is the built-in one.

```go
func serveACP(ctx context.Context, sessions session.Client, agent tacklr.AgentOptions) error {
	return server.NewServer(sessions, agent, acp.New(nil)).ServeHTTP(ctx, "127.0.0.1:8080")
}
```

ACP's `_tacklr/vfs/bind` maps onto `Prompt.Auth`, so VFS tokens never reach workflow history.

---

## What else you can wire in

| Piece | What it does | Where to read |
|-------|--------------|---------------|
| Planning | `create_plan`, todos, hand-off on `complete_todo` | [`tacklr`](https://pkg.go.dev/github.com/ryanaldo34/tacklr) |
| MCP | External tool servers (`AgentOptions.MCPConfigs`, `session.MCPServer`) | [`session`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/session) |
| Skills | `SKILL.md` catalogs from `SkillsPath`; the model reads them only through `read_skill` | [`skills`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/skills) |
| Web | `web_search` and `web_fetch` (`web.WebSearch`, `web.WebFetch`, Exa client) | [`web`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/web) |
| Email | `read_inbox` and permission-gated `send_email` (Gmail, Outlook) | [`email`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/email) |
| File indexing | `index_file` / `unindex` put workspace files into the brain (`vfsindex`) | [docs/knowledge.md](docs/knowledge.md) |
| Telemetry | `telemetry.Init`: OTLP traces, metrics and logs; spans for model requests and tool calls | [`telemetry`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/telemetry) |

---

## Documentation

| Doc | What it covers |
|-----|----------------|
| [docs/session.md](docs/session.md) | `session.Client`, `loop.Executor`, the Temporal client and worker, rounds, HITL, children, auth |
| [docs/tools.md](docs/tools.md) | Tools, `CallContext`, clients through constructor closures |
| [docs/vfs.md](docs/vfs.md) | Mounts, content IR, providers, FUSE |
| [docs/knowledge.md](docs/knowledge.md) | The brain: search, graph, kinds, session history, tools |
| [docs/fuse-vfs-run-command.md](docs/fuse-vfs-run-command.md) | How `run_command` and the FUSE projection fit |
| [AGENTS.md](AGENTS.md) | Goals, coding standards, how we test |

---

## Packages

| Package | Role |
|---------|------|
| `tacklr` | The harness: `Harness`, `Model`, rounds, tools, `CallContext`, window, plan, specialists, interrupts, checkpoints, VFS/brain/MCP wiring |
| `message` | Conversation data: `Message`, content parts, `ToolCall` |
| `session` | The client contract: `Client`, `Store`, `Event`, `Status`, `EventLog`, `SecretStorage`, `MCPServer` |
| `session/loop` | The generic session loop: `loop.Run`, `Executor`, `Turn` |
| `session/temporal` | The Temporal client (`temporal.Client`, `Dial`) |
| `session/temporal/worker` | The Temporal worker: the session workflow, its activities, its harness cache, `JobHandler` |
| `brain` | Knowledge engine, `Store` and `Graph` interfaces, in-memory store |
| `brain/postgres` | Postgres `brain.Store` |
| `brain/helixgraph` | Helix `brain.Graph` |
| `vfs` | Virtual filesystem, mounts, providers, content IR, FUSE |
| `vfs/mount` | The mount bindings and credentials a client sends |
| `vfsindex` | Workspace files into the brain (`index_file`) |
| `openai` | OpenAI-compatible `Model` |
| `server` | Protocol host over a `session.Client`; `server/acp` is ACP |
| `skills` | Skill loading from `SkillsPath` |
| `web` | Web search and fetch tools |
| `email` | Inbox and send tools; `email/gmail` and `email/outlook` |
| `telemetry` | OpenTelemetry setup and instruments |

---

## Contributing

This repo is a Go module. It requires **Go 1.27** and **Linux**: build, lint, and test on a Linux host or VM. Start with [`AGENTS.md`](AGENTS.md) for goals and coding standards. Short version:

- Prefer the standard library. Third-party packages are a last resort.
- Prefer small, explicit pieces over hidden abstractions.
- Tests are outcome-oriented integration tests. Assert what should happen, not that a private helper ran. Avoid duplicate coverage of the same return path.

```bash
make cover   # go test -race over ./... plus the coverage gate; needs Linux, FUSE, user namespaces and Docker
make vet
make lint
```

The suite starts its own containers (Postgres, Helix, Temporal, S3, Azurite) through Testcontainers. Plain `go test ./...` runs everything.

Where to look:

| Area | Start here |
|------|------------|
| Turn scheduler, steps | `session/turn.go`, `session/session.go` |
| Steps in the harness: infer, tool, commit, collapse | `durable_steps.go`, `round.go`, `model_tasks.go` |
| A running call | `call.go`, `tool_runner.go`, `tools.go` |
| Messages, checkpoints | `message.go`, `checkpoint.go` |
| Specialists, children | `subagents.go`, `session/temporal/child.go` |
| Temporal runtime | `session/temporal/workflow.go`, `session/temporal/activities.go` |
| VFS | `vfs/`, `docs/vfs.md` |
| Knowledge | `brain/`, `brain/postgres/`, `brain/helixgraph/`, `docs/knowledge.md` |
| Protocols | `server/`, `server/acp/` |

Issues and pull requests are welcome. Match the surrounding code: `gofmt`, `go vet`, `golangci-lint`.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
