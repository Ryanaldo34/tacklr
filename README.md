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

**Sessions live in the cloud.** Hosts call `session.Runtime`. A durable runtime (Temporal today, others later) runs the session, so it outlives any one process. A human-in-the-loop question parks the session until `Resume`.

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
- **Collapse.** When a commit puts the window over 85% of its size, everything after the first message and the plan collapses into one summary. The collapsed messages are saved, and `search` finds them. A round still too large has its biggest results trimmed; with `AgentOptions.OverflowPath` each full result is written to the workspace first.

```text
prompt → Commit → Infer → Tool, Tool, Tool (at once) → Commit → Infer → … → complete
                                   └ park → Resume → Tool (the parked call) → Commit
```

The planning built-ins (`create_plan`, `list_plan`, `edit_plan`, `complete_todo`) belong to the harness. Your tools cannot rewrite the plan.

---

## Get started

A host builds an `AgentOptions`, opens a durable runtime, and talks to sessions through `session.Runtime`.

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
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/web"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The model: any tacklr.InferenceStrategy. This one speaks the OpenAI Responses API.
	model := openai.NewOpenAIInferenceStrategy(&http.Client{Timeout: 2 * time.Minute}).
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

	// The durable runtime: Temporal. Production passes shared Snapshots and Secrets.
	c, err := temporal.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_ADDRESS")})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	rt := temporal.Open(c, temporal.Config{Agent: agent})
	w := rt.StartWorker()
	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
	defer w.Stop()

	// The host API: create a session, follow its events, prompt it.
	id, err := rt.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		log.Fatal(err)
	}
	head, err := rt.Head(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, head)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	if err := rt.Prompt(ctx, id, session.Prompt{Text: "What changed in Go 1.27? Save what you find."}); err != nil {
		log.Fatal(err)
	}
	for ev := range sub.Events() {
		switch ev.Type {
		case tacklr.StreamEventMessage:
			fmt.Print(ev.Content)
		case tacklr.StreamEventInterrupt:
			fmt.Println("\nwaiting for the user:", string(ev.Data))
			return
		case tacklr.StreamEventComplete, tacklr.StreamEventError:
			return
		}
	}
}
```

`weatherTool` is the tool in [Tools and HarnessRuntime](#tools-and-harnessruntime). With a `Brain`, the agent gets `search`, `open`, `related` and `save` (and `link` when the engine has a graph). With `OpenVFS`, it gets `read`, `write`, `write_document`, `write_spreadsheet`, `run_command`, and `index_file`/`unindex` when a brain is set too.

---

## Key interfaces

### InferenceStrategy and ModelRequest

The model is an interface. `Invoke` and `CountTokens` take one `ModelRequest` with everything for that request, so a strategy holds no per-request state and one value is safe to share across sessions. The harness builds every request in one place, so a token count measures exactly what the request sends.

```go
type ModelRequest struct {
	Messages     []*Message
	Tools        []*Tool
	SystemPrompt string
	CacheKey     string // the session id, for provider prompt caching
}

type InferenceStrategy interface {
	Invoke(ctx context.Context, req ModelRequest) (chan LLMResponseChunk, error)
	CountTokens(ctx context.Context, req ModelRequest) (int, error)
	MaxContextWindow() (int, error)
	ModelTelemetryIdentity() telemetry.ModelIdentity
	SupportsMIME(mimeType string) bool
}
```

A minimal strategy:

```go
type echoModel struct{}

func (echoModel) Invoke(ctx context.Context, req tacklr.ModelRequest) (chan tacklr.LLMResponseChunk, error) {
	ch := make(chan tacklr.LLMResponseChunk, 1)
	last := req.Messages[len(req.Messages)-1]
	ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "you said: " + last.Content, IsComplete: true}
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

`openai.NewOpenAIInferenceStrategy` is the built-in client for OpenAI-compatible endpoints.

### Tools and HarnessRuntime

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

A handler that also takes `*tacklr.HarnessRuntime` gets the running call. The harness makes one for each call; a tool never makes one.

| Method | What it does |
|--------|--------------|
| `StateGet`, `StateSet`, `StateDelete` | Session key-values. A call reads its own writes first; its writes apply when its round commits. Hosts set values on `CreateSession.State`, `Prompt.State`, `Resume.State`. |
| `Park(intr)` | Asks the user. The call parks; after `Resume` it runs again and `Park` returns the answered interrupt. |
| `Schedule(ctx, JobRequest)` | Starts a Runtime job worker. With `Wait`, the job's output is the call's result; without it, the output arrives later as a message. |
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
		Handler: func(ctx context.Context, args deployArgs, rt *tacklr.HarnessRuntime) (string, error) {
			// Ask the user before a production deploy. The call parks here; after
			// Resume it runs again and Park returns the answered interrupt.
			answered, err := rt.Park(&tacklr.UserSelectionInterrupt{
				Question: "Deploy " + args.Service + "?",
				Options:  []tacklr.UserChoice{{Title: "Deploy"}, {Title: "Cancel"}},
			})
			if err != nil {
				return "", err
			}
			if answered.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title != "Deploy" {
				return "deploy cancelled by the user", nil
			}
			env, _ := rt.StateGet("env") // host state from CreateSession/Prompt.State
			rt.EmitUpdate("starting the pipeline")
			// A Runtime job worker; with Wait the job's output is this call's result.
			_, err = rt.Schedule(ctx, tacklr.JobRequest{
				Name: "pipeline",
				Task: fmt.Sprintf(`{"service":%q,"env":%q,"key":%q}`, args.Service, env, rt.CurrentToolCallID()),
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

The client gets a `StreamEventInterrupt` whose `MessageID` is the call id and whose `Data` is the serialized interrupt. It answers with `Runtime.Resume(ctx, id, session.Resume{Responses: map[string][]byte{callID: answer}})`.

### ToolInterceptor, OnCall and AllowTools

A `ToolInterceptor` wraps every call. It can short-circuit, or change the `ToolOutcome` that `next` returns (its `Output` and its window `Effect`). An `OnCallFunc` runs before one tool's handler and may return an interrupt to park on; `ToolPermissionOnCall` asks for permission and remembers allow-always and reject-always. `AllowTools` grants named tools up front.

```go
func auditInterceptor(ctx context.Context, inv tacklr.ToolInvocation, next tacklr.ToolCallFunc) (tacklr.ToolOutcome, error) {
	out, err := next(ctx, inv)
	slog.InfoContext(ctx, "tool call", "tool", inv.Tool.Name(), "call", inv.Runtime.CurrentToolCallID(), "error", err)
	return out, err
}

func gatedTool() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:    "drop_table",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, // asks once; allow-always is remembered
		Handler: func(ctx context.Context) (string, error) { return "dropped", nil },
	})
}

func agentWithPolicy(model tacklr.InferenceStrategy) tacklr.AgentOptions {
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
func agentWithSpecialists(model tacklr.InferenceStrategy) tacklr.AgentOptions {
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

A Runtime job worker is plain Go work a tool starts with `Schedule`: a shell pipeline, a long data job. Register workers on the runtime:

```go
func jobWorkers() map[string]session.JobHandler {
	return map[string]session.JobHandler{
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

func agentWithWorkspace(model tacklr.InferenceStrategy) tacklr.AgentOptions {
	return tacklr.AgentOptions{
		Model:        model,
		OpenVFS:      workspace(),
		OverflowPath: "/workspace/scratch/overflow",
	}
}
```

`ReadOnly` refuses writes. `NotIndexed` makes `index_file` refuse that mount. `OverflowPath` must be on a writable mount. Writes persist when they happen; file bytes are never in a checkpoint. Details: [docs/vfs.md](docs/vfs.md).

### session.Runtime

`session.Runtime` is the host API. Protocols and autonomous hosts call it; `session/temporal.Runtime` implements it.

```go
type Runtime interface {
	CreateSession(ctx context.Context, req CreateSession) (SessionID, error)
	Close(ctx context.Context, sessionID SessionID) error
	Status(ctx context.Context, id SessionID) (SessionStatus, error)
	Head(ctx context.Context, sessionID SessionID) (Seq, error)
	Subscribe(ctx context.Context, sessionID SessionID, after Seq) (Subscription, error)

	Prompt(ctx context.Context, sessionID SessionID, msg Prompt) error
	Resume(ctx context.Context, sessionID SessionID, resume Resume) error
	Cancel(ctx context.Context, sessionID SessionID) error

	Children(ctx context.Context, parent SessionID) ([]SessionID, error)
	Jobs(ctx context.Context, parent SessionID) ([]SessionStatus, error)
}
```

A prompt while a turn runs or is parked steers it; it does not cancel the model call. `Cancel` stops the turn and its children; the session stays open.

### session.Driver

`session.Driver` is what a durable runtime implements. The generic turn scheduler (`session.Turn.Run`) calls it, so every runtime runs a turn the same way; a runtime only decides where and how each step runs.

```go
type Driver interface {
	Begin(kind string, promptLen, resumes int)
	End()
	Commit(base Revision, in tacklr.CommitIn) (Revision, error)
	Infer() (tacklr.Reply, error)
	Tools(runs []tacklr.ToolRun) []ToolResult
	Emit(tacklr.StreamEvent)
	Job(name, task string) (string, error)

	Prompts() []PromptIn
	Ready() []*tacklr.Message
	Recv() Wake
	Wait() Wake

	Children() []SessionID
	Start(tacklr.CallOutcome) error
	Await(SessionID) (output string, failed bool, err error)
	CancelAll()
}
```

The contract:

- **One non-deterministic action is one step.** `Infer` is one model request, each `ToolRun` is one tool call, `Job` is one worker body. The runtime records each completed step and replays its result; neither the harness nor the snapshot records which steps ran.
- **A step may run more than once** before the runtime records it (a worker dies before it reports). `Infer` and `Tool` steps change nothing a later step reads, so that is safe.
- **`Commit` is the only snapshot writer, and it is idempotent.** `base` is the revision the previous `Commit` returned; a `Commit` that finds the snapshot past `base` already saved and applies nothing again.
- **`Tools` runs its runs at once** (Temporal: one activity each) and returns a result per run, in run order.
- **`Start` starts a child by its deterministic id;** starting the same id twice starts it once.

The session's identity, MCP servers and children are the runtime's: it holds them and builds each step's world from them. Details: [docs/session.md](docs/session.md).

### SnapshotStore, EventLog and SecretStorage

The runtime keeps three kinds of session data apart:

| Store | Holds | Default |
|-------|-------|---------|
| `session.SnapshotStore` | `tacklr.SessionCheckpoint`: the window of complete rounds, plan, user state, permission decisions, search cursors | in memory |
| `session.EventLog` | The client event stream (fallback when Workflow Streams are off) | in memory |
| `session.SecretStorage` | VFS tokens. Never in a snapshot or workflow history | in memory |

The in-memory stores work for one process only. Production passes shared stores. `SnapshotStore.Save` is compare-and-swap on a `Revision`:

```go
type checkpointRow struct {
	cp  tacklr.SessionCheckpoint
	rev session.Revision
}

type mapSnapshots struct{ rows map[session.SessionID]checkpointRow }

func (m *mapSnapshots) Save(ctx context.Context, id session.SessionID, cp tacklr.SessionCheckpoint, expected session.Revision) (session.Revision, error) {
	if m.rows[id].rev != expected {
		return "", session.ErrStaleCheckpoint
	}
	next := session.Revision(fmt.Sprint(len(m.rows[id].rev) + 1))
	m.rows[id] = checkpointRow{cp: cp, rev: next}
	return next, nil
}

func (m *mapSnapshots) Load(ctx context.Context, id session.SessionID) (tacklr.SessionCheckpoint, session.Revision, error) {
	row, ok := m.rows[id]
	if !ok {
		return tacklr.SessionCheckpoint{}, "", session.ErrSessionNotFound
	}
	return row.cp, row.rev, nil
}

func (m *mapSnapshots) Delete(ctx context.Context, id session.SessionID) error {
	delete(m.rows, id)
	return nil
}

func runtimeConfig(agent tacklr.AgentOptions) temporal.Config {
	return temporal.Config{
		Agent:     agent,
		Snapshots: &mapSnapshots{rows: map[session.SessionID]checkpointRow{}},
		Secrets:   session.NewMemorySecretStorage(),
		Jobs:      jobWorkers(),
	}
}
```

`Close` deletes the session's snapshot and secrets.

### server.Protocol

A `server.Protocol` maps a wire protocol onto `session.Runtime`. ACP (`server/acp`) is the built-in one.

```go
func serveACP(ctx context.Context, rt session.Runtime, agent tacklr.AgentOptions) error {
	return server.NewServer(rt, agent, acp.New(nil)).ServeHTTP(ctx, "127.0.0.1:8080")
}
```

ACP's `_tacklr/vfs/bind` maps onto `Prompt.Auth`, so VFS tokens never reach workflow history.

---

## What else you can wire in

| Piece | What it does | Where to read |
|-------|--------------|---------------|
| Planning | `create_plan`, todos, hand-off on `complete_todo` | [`tacklr`](https://pkg.go.dev/github.com/ryanaldo34/tacklr) |
| MCP | External tool servers (`AgentOptions.MCPConfigs`) | [`mcp`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/mcp) |
| Skills | `SKILL.md` catalogs from `SkillsPath`; the model reads them only through `read_skill` | [`skills`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/skills) |
| Web | `web_search` and `web_fetch` (`web.WebSearch`, `web.WebFetch`, Exa client) | [`web`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/web) |
| Email | `read_inbox` and permission-gated `send_email` (Gmail, Outlook) | [`email`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/email) |
| File indexing | `index_file` / `unindex` put workspace files into the brain (`vfsindex`) | [docs/knowledge.md](docs/knowledge.md) |
| Telemetry | `telemetry.Init`: OTLP traces, metrics and logs; spans for model requests and tool calls | [`telemetry`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/telemetry) |

---

## Documentation

| Doc | What it covers |
|-----|----------------|
| [docs/session.md](docs/session.md) | `session.Runtime`, `session.Driver`, the Temporal runtime, rounds, HITL, children, auth |
| [docs/tools.md](docs/tools.md) | Tools, `HarnessRuntime`, clients through constructor closures |
| [docs/vfs.md](docs/vfs.md) | Mounts, content IR, providers, FUSE |
| [docs/knowledge.md](docs/knowledge.md) | The brain: search, graph, kinds, session history, tools |
| [docs/fuse-vfs-run-command.md](docs/fuse-vfs-run-command.md) | How `run_command` and the FUSE projection fit |
| [AGENTS.md](AGENTS.md) | Goals, coding standards, how we test |

---

## Packages

| Package | Role |
|---------|------|
| `tacklr` | The harness: turns, rounds, tools, `HarnessRuntime`, plan, specialists, interrupts, checkpoints |
| `session` | `Runtime`, `Driver`, the generic turn scheduler, `SnapshotStore`, `EventLog`, `SecretStorage` |
| `session/temporal` | The Temporal runtime |
| `brain` | Knowledge engine, `Store` and `Graph` interfaces, in-memory store |
| `brain/postgres` | Postgres `brain.Store` |
| `brain/helixgraph` | Helix `brain.Graph` |
| `vfs` | Virtual filesystem, mounts, providers, content IR, FUSE |
| `vfsindex` | Workspace files into the brain (`index_file`) |
| `openai` | OpenAI-compatible `InferenceStrategy` |
| `server` | Protocol host over `session.Runtime`; `server/acp` is ACP |
| `mcp` | MCP config types |
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
