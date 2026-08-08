# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

**A Go framework for running AI agents in real products** — not a one-off chat demo.

```bash
go get github.com/ryanaldo34/tacklr
```

---

## What problem does it solve?

Most agent demos look like this:

1. Send the whole chat history to the model  
2. Call tools  
3. Append more messages  
4. Repeat until the context window is full, then “summarize everything”

That wastes tokens, confuses the model with old noise, and is hard to run in editors, APIs, or long-lived services.

**Tacklr is opinionated about the agent loop:**

| Problem | What Tacklr does |
|---------|------------------|
| Context fills with junk | Keeps a **plan** and **todos**; when a step finishes, it builds a **handoff** (clean context for the next step) |
| Tools and models mixed into one blob | **Clear layers**: model I/O, agent loop, client protocol |
| Hard to cancel or ask the user a question | **Turns**, **cancel**, and **interrupts** (pause for input, then resume) |
| State dies when the process exits | **Checkpoints** you can save and reload |
| Wiring into IDEs / HTTP | Same registry over **ACP** (e.g. Zed) or **SSE** |
| Naive RAG dumps stale chunks into the window | Optional **knowledge base (brain)**: temporal + graph retrieval, dual-store design, agent tools only when wired |

It is a **framework** (it defines how agents *should* run), not a loose bag of helpers.

---

## How it works (big picture)

Think of three layers:

```text
  Your model API (OpenAI-compatible, Azure, …)
            │
            ▼  tokens / tool calls from the model
     ┌──────────────┐
     │   harness    │  the agent loop: tools, plan, handoff, cancel
     │   (tacklr)   │
     └──────┬───────┘
            ▼  StreamEvent (shared event types)
     ┌──────────────┐
     │    server    │  Registry + protocol (ACP, SSE, …)
     └──────────────┘
            │
            ▼  editor / HTTP client
```

1. **Inference** — talks to the model only (parse the stream into chunks).  
2. **Harness** — owns the turn: tools, plan builtins, context, save/load.  
3. **Server** — maps that to a client protocol (stdio ACP, SSE, …).

You can use the **harness alone** in a Go program, or put a **registry** in front for multi-agent HTTP/ACP.

### One turn

A **turn** is one user prompt (or one resume after an interrupt) until the agent finishes, errors, or waits for the user:

```text
User prompt
    → model may call tools (search, create_plan, complete_todo, …)
    → results go back into context
    → model continues until done / interrupt / cancel
```

Built-in plan tools drive a simple lifecycle:

```text
create_plan  →  do work with tools  →  complete_todo  →  handoff  →  next work
```

---

## Quick start

### Minimal agent

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/inference"
	"github.com/ryanaldo34/tacklr/stores"
)

func main() {
	ctx := context.Background()

	model := inference.NewOpenAIInferenceStrategy(&http.Client{Timeout: 2 * time.Minute})
	model.WithURL(os.Getenv("OPENAI_BASE_URL")). // e.g. https://api.openai.com/v1
		WithApiKey(os.Getenv("OPENAI_API_KEY")).
		WithModel(os.Getenv("OPENAI_MODEL"))

	agent := tacklr.NewAgent(ctx, tacklr.AgentOptions{
		Config: tacklr.Config{
			MaxWindowSize: 8192,
			SystemPrompt:  "You are a concise assistant.",
		},
		Model: model,
		Store: stores.NewInMemoryStore(),
	})
	defer agent.Close()

	events, err := agent.Run(ctx, "Say hello in one short sentence.")
	if err != nil {
		panic(err)
	}
	for ev := range events {
		switch ev.Type {
		case tacklr.StreamEventMessage:
			fmt.Print(ev.Content)
		case tacklr.StreamEventError:
			fmt.Println("error:", ev.Error, ev.Content)
		case tacklr.StreamEventComplete:
			fmt.Println()
		}
	}
}
```

### Your own tool

Tools are normal Go functions. Optional `HarnessRuntime` is for **your** state, progress pings, and interrupts — not for changing the framework plan.

```go
type SearchArgs struct {
	Query string `json:"query" desc:"The search query"`
	Limit int    `json:"limit,omitempty" desc:"Max results"`
}

tool := tacklr.NewTool(tacklr.ToolConfig{
	Name:        "search_web",
	Description: "Search the web for information.",
	Handler: func(ctx context.Context, args SearchArgs, rt tacklr.HarnessRuntime) (string, error) {
		// rt.StateGet / StateSet  — small DI bag for your tool
		// rt.EmitUpdate(...)      — progress to the client
		// rt.RaiseInterrupt(...)  — ask the user and wait
		return doSearch(ctx, args.Query, args.Limit)
	},
})
```

Handler shapes supported: with or without args, with or without `HarnessRuntime`. JSON schema comes from struct tags (`json`, `desc`, `enum`).

### Serve over ACP or SSE

Two stores matter for ACP:

| Store | Role |
|-------|------|
| `stores.BaseStore` on the **Registry** | Agent harness checkpoints (conversation, plan, tools) |
| `server.ProtocolWireStore` on the **ACP protocol** | Wire session envelope (`session/new` / `session/load`: cwd, mcp, config) |

You can implement either interface against your own DB (Redis, SQLite, `database/sql`, …). Built-in Postgres helpers use `*pgx.Conn`.

**Short-hand (recommended):**

```go
store := stores.NewInMemoryStore() // or stores.NewPostgresStore(conn)
reg := server.NewRegistry(store, "my-agent")
reg.Register("my-agent", server.AgentSpec{
	Name: "Demo",
	Config: tacklr.Config{
		MaxWindowSize: 8192,
		SystemPrompt:  "You are a helpful assistant.",
	},
	Model: model,
	Tools: []*tacklr.Tool{tool},
})

// In-process ACP (memory wire store) — one line
srv := server.NewACPServer(reg)

// Editor / stdio (Zed, etc.)
_ = srv.ServeStdio(ctx, os.Stdin, os.Stdout)

// HTTP: WebSocket + Streamable HTTP on /acp
// _ = srv.ServeHTTP(ctx, ":8080")
//   ws://localhost:8080/acp
//   POST/GET/DELETE https://localhost:8080/acp  (HTTP/2 recommended for Streamable)
```

**Durable wire sessions (Postgres, same connection as harness is fine):**

```go
// harness + wire schemas are separate tables on the same *pgx.Conn
harness := stores.NewPostgresStore(conn)
reg := server.NewRegistry(harness, "my-agent")
// reg.Register(...)
srv := server.NewACPServerPostgres(reg, conn)
```

**Custom wire store or multi-protocol:**

```go
// Your ProtocolWireStore (Redis, etc.)
srv := server.NewACPServerWithWire(reg, myWireStore)

// ACP + SSE on one server
srv = server.NewServer(reg, server.NewACPProtocolMemory(), server.SSE)

// Explicit Postgres protocol only
srv = server.NewServer(reg, server.NewACPProtocolPostgres(conn))
```

| Helper | Meaning |
|--------|---------|
| `NewACPServer(reg)` | ACP + memory wire store |
| `NewACPServerWithWire(reg, wire)` | ACP + your `ProtocolWireStore` |
| `NewACPServerPostgres(reg, conn)` | ACP + Postgres wire store (`*pgx.Conn`) |
| `NewACPProtocolMemory()` | Protocol only (compose with `NewServer`) |
| `NewACPProtocolPostgres(conn)` | Protocol only, Postgres wire |

Or native HTTP + SSE (non-ACP wire):

```go
srv := server.NewServer(reg, server.SSE)
_ = srv.ServeHTTP(ctx, ":8080")
```

```bash
# SSE prompt (native SSE protocol, not ACP)
curl -N -X POST http://localhost:8080/ \
  -H "Accept: text/event-stream" \
  -d '{"agent_id":"my-agent","prompt":"Hello"}'
```

### Try the test server

`cmd/testserver` is a harness **showcase**: no toy host tools. The agent only gets Tacklr builtins (`create_plan`, `list_plan`, `edit_plan`, `complete_todo`, `ask_user_choice`, and `web_search` when `EXA_API_KEY` is set), plus optional skills via `SKILL_DIRECTORIES`.

By default it exports OTLP traces/metrics/logs to **`localhost:4317` (gRPC)** with `service.name=tacklr-testserver` when a collector is listening. Override with `OTEL_*` env vars, or set `OTEL_SDK_DISABLED=true` to turn exporters off.

```bash
# .env: OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_MODEL
# optional: EXA_API_KEY, SKILL_DIRECTORIES, MAX_WINDOW_SIZE, OTEL_*
go build -o bin/testserver ./cmd/testserver
./bin/testserver --stdio   # ACP stdio (Zed, etc.)
./bin/testserver           # HTTP ACP on PORT or :3000
#   WebSocket:        ws://localhost:3000/acp
#   Streamable HTTP:  POST/GET/DELETE http://localhost:3000/acp
#   Legacy unary:     POST http://localhost:3000/
# or: make testserver
```

---

## Core ideas (a bit more detail)

### Plans and handoffs

The agent is pushed to work from a **plan document** and a **todo list** (built-in tools: `create_plan`, `list_plan`, `edit_plan`, `complete_todo`).

- After **create_plan**, context is tightened around the user goal + plan.  
- After **complete_todo** (or a real plan-text edit), Tacklr runs a **handoff**: a short, structured carry-over for the next step instead of dumping the entire chat again.

That is the main “better context” idea in the project.

### Sessions and checkpoints

```go
agent := tacklr.NewAgent(ctx, opts)                      // new
agent, err := tacklr.NewAgentFromSession(ctx, id, opts) // restore
```

On save, a **Checkpointer** packages conversation window, plan, tool/user state, and pending interrupts. A **store** (in-memory or Postgres) persists it.

### Tools vs framework state

| | **Your tools** | **Built-in plan tools** |
|--|----------------|-------------------------|
| API | `HarnessRuntime` | Internal session manager (not passed to you) |
| Can | State, interrupts, progress, store | Create/edit plan, complete todos |
| Cannot | Rewrite the plan store directly | — |

This keeps product tools from breaking the planning system by accident.

### MCP, skills, and web search

- **MCP** — pass `MCPConfigs` on the agent (or via ACP session); tools are discovered and run for you.  
- **Skills** — set `Config.SkillDirectories` to folders of `SKILL.md` (default `skills.DirectoryLoader`). Inject a source-bound `AgentOptions.SkillsLoader` for object storage, including `skills.S3Loader` and `skills.BlobLoader`. A short catalog lands in the system prompt; full text loads via `read_skill` when needed.
- **Web search (Exa)** — when `EXA_API_KEY` is set in the environment (or `AgentOptions.ExaAPIKey`), the harness injects a built-in `web_search` tool (read access, token-efficient **highlights** by default). Hosts that use `.env` should load it before `NewAgent` (the test server already does). No Exa Go SDK; the harness calls Exa’s REST API.
- **Knowledge base (brain)** — optional; see [Knowledge base (brain)](#knowledge-base-brain) below.

### Public harness surface

`AgentHarness` fields are unexported. Hosts use:

- `NewAgent` / `NewAgentFromSession` + `AgentOptions` (model, store, tools, MCP, skills, interceptors, hooks, optional `Brain`)
- `SessionID()` / `BindSessionID` (registry thread binding)
- `ToolRuntime()` for interrupt helpers that need `*HarnessRuntime`
- `Messages()` / `RestoreMessages` for the conversation window
- `Run` / `ReturnFromInterrupt` / `Close`

Plan builtins return typed `BuiltinResult` effects (install plan, handoff) instead of name-keyed hooks.

### Knowledge base (brain)

Tacklr’s knowledge package is **not** “stuff the last N chunks into context.” It is a host-owned retrieval engine with:

- **Postgres** as the source of truth for full objects, parts/chunks, BM25 + dense hybrid search, filters, soft-delete, and containment (`parent_id`)
- **Helix** (optional graph backend) for first-class **entity** nodes and cross-object edges (not chunks)—text/vector indexes, topology, edge metadata
- **Dual-write** on parent `Put` / `SoftDelete` / `Link` so graph nodes stay live with the store
- **Scope** (namespace) on every hydrate so multi-tenant isolation is engine-enforced

Hosts build an `Engine`, then attach it on the agent. The harness registers knowledge tools only when the engine is set; capability-gated tools appear only when the graph backend supports them.

#### Boot sketch

```go
import (
	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// store: brain.NewPostgresStore(pool) in production, or brain.NewMemoryStore() in tests.
store, err := brain.NewPostgresStore(pool)
if err != nil { /* … */ }

g, err := helixgraph.New(helixURL) // optional graph backend
if err != nil { /* … */ }
// Required for find_objects on Helix. Prefer true when the image supports tenant indexes.
if err := g.Bootstrap(ctx, false); err != nil { /* … */ }
// Required per relation label before find_links can search edge notes on Helix.
for _, rel := range []string{"about", "has_buyer", "references"} {
	if err := g.EnsureEdgeTextIndex(ctx, rel); err != nil { /* … */ }
}

eng, err := brain.NewEngine(store,
	brain.WithEmbedder(emb),                    // optional dense channel
	brain.WithGraph(g),                         // MemoryGraph also implements searchers
	brain.WithObserver(telemetry.NewBrainObserver()), // optional OTEL
	// brain.WithExpandRecipes(...),            // optional named ExpandRequest templates
	// brain.WithReranker(...),                 // optional post-hydrate host scoring
)
if err != nil { /* … */ }
if err := eng.ApplyKinds(ctx, kindSpecs...); err != nil { /* … */ }

agent := tacklr.NewAgent(ctx, tacklr.AgentOptions{
	// … Model, Store, Config …
	Brain: eng,
	BrainWriteKinds: brain.WriteKinds{
		Discovery: "Discovery", // non-empty → save_discovery tool
		Fact:      "Fact",
		Memory:    "Memory",
	},
	SearchNamespace: &tenantNS, // optional isolation (checkpointed)
})
```

Offline / tests: `brain.NewMemoryStore()` + `brain.NewMemoryGraph()` need no Bootstrap; edge text search works in-process.

#### Agent tools (capability matrix)

| Tool | When registered |
|------|-----------------|
| `schema`, `read`, `search`, `find_exact`, `continue`, `expand` | `AgentOptions.Brain != nil` |
| `find_objects` | graph implements object text/vector search **and** is ready (`Bootstrap` on Helix) |
| `find_links` | graph implements edge text search (Helix after `EnsureEdgeTextIndex` for that label) |
| `link` | graph implements `GraphWriter` |
| `save_discovery` / `save_fact` / `save_memory` | corresponding `BrainWriteKinds` field is non-empty |

`expand` supports multi-hop (`max_hops`), direction (`out` / `in` / `both`), and mixed containment + graph labels. Large result sets page via `continue`.

#### Host GraphRAG composition (not agent tools)

Hosts can orchestrate the same path product code uses:

```text
find_objects / search → LandingIDs / LandingIDsFromPage
  → Expand / ExpandMany / ExpandByRecipe
  → optional FindLinks
  → search(scope_ids=…) for neighborhood corpus
  → optional Reranker / SortRichObjects
```

`LandingIDs` promotes part hits to first-class parent ids so expand/link always target dual-written entities. See package docs: [`brain`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/brain).

#### Observability

With `brain.WithObserver(telemetry.NewBrainObserver())`, retrieval ops emit `tacklr.brain` spans/metrics: `search`, `find_exact`, `find_objects`, `find_links`, `continue`, `expand`, `expand_many` (closed enum; degrade modes include lexical-only and containment-only).

---

## Observability (optional)

Tacklr can emit **traces** and **metrics** with OpenTelemetry. You bring the backend (Grafana Alloy/Collector, Tempo, Prometheus/Mimir, etc.). Logs are normal **slog**; use `telemetry.NewLogger` if you want `trace_id` / `span_id` on log lines for Grafana/Loki.

**Simple process** (one OTLP endpoint for traces + metrics):

```go
shutdown, err := telemetry.Init(ctx, telemetry.Config{
	ServiceName:  "my-agent",
	OTLPEndpoint: "localhost:4317", // Alloy / collector
	Insecure:     true,
})
defer shutdown(ctx)
// then NewRegistry / NewAgent — globals are used by default
```

**Library host** (you already own OTEL):

```go
reg := server.NewRegistry(store, "my-agent",
	server.WithTracerProvider(myTP),
	server.WithMeterProvider(myMP),
)
```

**Prometheus scrape** (you own `/metrics`):

```go
promReg := prometheus.NewRegistry()
mp, _ := telemetry.MeterProviderFromPrometheusRegisterer(promReg, "my-agent", "")
reg := server.NewRegistry(store, "my-agent", server.WithMeterProvider(mp))
// http.Handle("/metrics", promhttp.HandlerFor(promReg, ...))
```

With no endpoint and no injection, traces and metrics are no-ops. Prompt/tool **content** is not attached by default.

OTLP is the export path for traces, metrics, and logs. Point any collector (or vendor backend) at `OTEL_EXPORTER_OTLP_ENDPOINT`. slog can dual-write to stderr and OTLP via `telemetry.InstallDefaultWithOTLP`.

---

## Packages

| Package | Role |
|---------|------|
| `tacklr` | Agent harness, tools, plan loop, subagents |
| `brain` | Knowledge engine: store, expand, find_objects, kinds, dual-write |
| `brain/helixgraph` | HelixDB adapter (`WithGraph`); Bootstrap + edge text indexes |
| `inference` | OpenAI-compatible model client |
| `server` | Registry + ACP / SSE |
| `stores` | Session checkpoints |
| `interrupt` | Interrupt types and registry for tool pause/resume |
| `streaming` | Shared message/event types |
| `mcp` | MCP config types (public) |
| `skills` | `SKILL.md` loading (`SkillLoader` injectable; includes `S3Loader` / `BlobLoader`) |
| `telemetry` | OTEL init, metrics helpers, brain observer, log correlation |
| `internal/session` | Session manager, plan store, checkpointer, tool runtime |

---

## Develop

```bash
make test
make vet
```

### Agent harness benchmarks

Multi-turn scenarios (plan, memory/brain, multi-hop QA, domain end-state, optional web) live in `internal/agentbench` with **seed data in Go**. Runner:

```bash
# List cases (no model)
go run ./cmd/agent-bench -list
go run ./cmd/agent-bench -dry-run

# Live run (same env as testserver)
export OPENAI_BASE_URL OPENAI_API_KEY OPENAI_MODEL
# hybrid dense channel (default text-embedding-3-small; same base URL/key)
export OPENAI_EMBEDDING_MODEL=text-embedding-3-small
# optional: EXA_API_KEY for web_augmented
go run ./cmd/agent-bench -suite all -out /tmp/agent-bench.json
# lexical-only ablation: go run ./cmd/agent-bench -lexical-only ...
```

Brain is seeded and agent saves with **hybrid search** (BM25-style lexical + dense embeddings via OpenAI-compatible `/embeddings`). Not run in default CI (model cost). Cases are industry-aligned (LoCoMo-style memory, multi-hop QA, τ-bench-style domain), not official leaderboard ports.

Contribution rules and design ethos live in [`AGENTS.md`](AGENTS.md).

---

## License

See [LICENSE](LICENSE).
