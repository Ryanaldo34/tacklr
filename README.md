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

```go
store := stores.NewInMemoryStore()
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

// Editor / ACP (e.g. stdio)
srv := server.NewServer(reg, server.ACP)
_ = srv.ServeStdio(ctx, os.Stdin, os.Stdout)

// Or HTTP + SSE
// srv := server.NewServer(reg, server.SSE)
// _ = srv.ServeHTTP(ctx, ":8080")
```

```bash
# SSE prompt
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
./bin/testserver --stdio   # ACP (Zed, etc.)
./bin/testserver           # HTTP ACP (PORT or :3000)
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

### Public harness surface

`AgentHarness` fields are unexported. Hosts use:

- `NewAgent` / `NewAgentFromSession` + `AgentOptions` (model, store, tools, MCP, skills, interceptors, hooks)
- `SessionID()` / `BindSessionID` (registry thread binding)
- `ToolRuntime()` for interrupt helpers that need `*HarnessRuntime`
- `Messages()` / `RestoreMessages` for the conversation window
- `Run` / `ReturnFromInterrupt` / `Close`

Plan builtins return typed `BuiltinResult` effects (install plan, handoff) instead of name-keyed hooks.

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
| `inference` | OpenAI-compatible model client |
| `server` | Registry + ACP / SSE |
| `stores` | Session checkpoints |
| `interrupt` | Interrupt types and registry for tool pause/resume |
| `streaming` | Shared message/event types |
| `mcp` | MCP config types (public) |
| `skills` | `SKILL.md` loading (`SkillLoader` injectable) |
| `telemetry` | OTEL init, metrics helpers, log correlation |
| `internal/session` | Session manager, plan store, checkpointer, tool runtime |

---

## Develop

```bash
make test
make vet
```

Contribution rules and design ethos live in [`AGENTS.md`](AGENTS.md).

---

## License

See [LICENSE](LICENSE).
