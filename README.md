# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

**An opinionated agent harness SDK for Go** — structured context, real protocols, tools & MCP, built for production agents (not demo chat wrappers).

Tacklr defines *how* agents should run: plan → execute → hand off clean context → continue. It is a **framework**, not a grab-bag of helpers.

```bash
go get github.com/ryanaldo34/tacklr
```

---

## Why Tacklr?

| Idea | What you get |
|------|----------------|
| **Structured context** | Adaptive Case Management–style plans (`create_plan` with a plaintext plan + todos, `edit_plan`, `complete_todo`). The plan document stays in context; completing a todo or revising the plan compresses into a **handoff** for the next work—not a vague “summarize everything.” |
| **Protocol-native** | Speak **ACP** (editors like Zed) or **SSE/WS** over the same registry. Architecture is ready for **A2A** as another protocol plug-in. |
| **Clear layers** | Inference parses the model wire; the harness owns the agent loop; protocols own client streaming. Easy to test and extend. |
| **Tools & MCP** | Reflect-based tools plus MCP discovery (stdio / HTTP / SSE) without bolting on a second agent runtime. |
| **Cloud-minded Go** | Small surface, stdlib-first, sessions you can checkpoint and reload. |

**Who it’s for:** teams embedding agents in products, IDE integrations, or services that need cancellable turns, interrupts, and durable session state—not one-off scripts.

---

## Architecture

```text
  Model provider (OpenAI-compatible / Foundry / …)
            │
            ▼  LLMResponseChunk
     ┌──────────────┐
     │  inference   │  parse provider SSE only
     └──────┬───────┘
            ▼
     ┌──────────────┐
     │   harness    │  plan, tools, handoff, cancel
     │   (tacklr)   │  emits StreamEvent bus
     └──────┬───────┘
            ▼  StreamEvent (protocol-agnostic)
     ┌──────────────┐
     │    server    │  Registry + Protocol
     │  ACP · SSE   │  (A2A later: same plug)
     └──────────────┘
            │
            ▼  client wire (JSON-RPC / SSE / …)
```

| Package | Responsibility |
|---------|----------------|
| `tacklr` | Agent harness, tools, context Fit/Handoff, skills, subagents |
| `inference` | OpenAI-compatible Responses API + SSE → `LLMResponseChunk` |
| `streaming` | Shared types: `Message`, `StreamEvent`, `ToolCall` (not wire codecs) |
| `server` | `Registry`, transports, **protocols** (`ACP`, `SSE`) |
| `stores` | Session checkpoints (in-memory; Postgres available) |
| `mcp` | MCP **config** types only (discovery is internal) |
| `control` | Runtime, plan, interrupts |
| `skills` | Load `SKILL.md` catalogs |

**Invariant:** client presentation lives on `server.Protocol` (`OnStreamEvent` / `OnStreamClosed`). The harness never owns ACP/SSE framing—so a future A2A protocol is another implementation, not a harness rewrite.

---

## Quick start

### 1. Minimal agent (library)

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
		// Tools: []*tacklr.Tool{myTool}, // optional
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

### 2. Define a tool

```go
type SearchArgs struct {
	Query string `json:"query" desc:"The search query"`
	Limit int    `json:"limit,omitempty" desc:"Max results"`
}

tool := tacklr.NewTool(tacklr.ToolConfig{
	Name:        "search_web",
	Description: "Search the web for information.",
	Handler: func(ctx context.Context, args SearchArgs, rt tacklr.HarnessRuntime) (string, error) {
		// rt: plan, interrupts, progress updates
		return doSearch(ctx, args.Query, args.Limit)
	},
})
```

Handler shapes:

- `func(context.Context) (T, error)`
- `func(context.Context, Args) (T, error)`
- `func(context.Context, Args, HarnessRuntime) (T, error)`
- `func(context.Context, HarnessRuntime) (T, error)`

JSON schema is derived from the args struct (`json`, `desc`, `enum` tags).

### 3. Serve over ACP (e.g. Zed) or SSE

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

// Editor / ACP stdio
srv := server.NewServer(reg, server.ACP)
_ = srv.ServeStdio(ctx, os.Stdin, os.Stdout)

// Or HTTP + SSE
// srv := server.NewServer(reg, server.SSE)
// _ = srv.ServeHTTP(ctx, ":8080")
```

**SSE prompt:**

```bash
curl -N -X POST http://localhost:8080/ \
  -H "Accept: text/event-stream" \
  -d '{"agent_id":"my-agent","prompt":"Hello"}'
```

**Resume after interrupt:**

```bash
curl -N -X POST http://localhost:8080/resume \
  -H "Accept: text/event-stream" \
  -d '{"agent_id":"my-agent","thread_id":"<id>","responses":{"<interruptId>":{"selectionIdx":0}}}'
```

WebSocket uses the same JSON body (GET `/` or GET `/resume`).

### 4. Try the included test server

```bash
# .env: OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_MODEL
go run ./cmd/testserver          # HTTP ACP on :3000 (or PORT)
go run ./cmd/testserver --stdio  # ACP over stdio
```

---

## MCP servers

Config only in the public `mcp` package; the harness connects and discovers tools.

```go
// stdio
mcp.MCPConfig{Name: "fs", Command: "npx", Args: []string{"-y", "@modelcontextprotocol/server-filesystem"}}

// streamable HTTP
mcp.MCPConfig{Name: "api", Type: mcp.TransportHTTP, URL: "https://api.example.com/mcp",
	Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer tok"}}}
```

```go
agent := tacklr.NewAgent(ctx, tacklr.AgentOptions{
	Config:     tacklr.Config{MaxWindowSize: 8192},
	Model:      model,
	Store:      store,
	MCPConfigs: []mcp.MCPConfig{{Name: "fs", Command: "npx", Args: []string{"-y", "@modelcontextprotocol/server-filesystem"}}},
})
```

Over ACP, clients can also pass `mcpServers` on `session/new` / load / resume.

---

## Skills

Point `SkillDirectories` at folders of `SKILL.md` files. A short catalog goes into the system prompt; the model loads full text via `read_skill` when needed.

```go
agent := tacklr.NewAgent(ctx, tacklr.AgentOptions{
	Config: tacklr.Config{
		MaxWindowSize:    8192,
		SkillDirectories: []string{"./skills"},
	},
	Model: model,
	Store: store,
})
```

```markdown
---
name: testing
description: Write and validate automated tests for this project.
---

Follow the project's test conventions and run tests before claiming done.
```

---

## Sessions & context

```go
// Fresh agent
agent := tacklr.NewAgent(ctx, opts)

// Later: restore checkpoint
agent, err := tacklr.NewAgentFromSession(ctx, sessionID, opts)
```

- **Fit** — when the window is large, compress older history before adding new messages.  
- **Plan document** — `create_plan` stores the full plaintext plan and prunes the window to `[user, plan]`.  
- **Handoff** — after a successful `complete_todo` (or `edit_plan` when the plan text changes), rebuild as `[user, plan, handoff, …]` so the full draft stays separate from the process handoff.  
 
- **Cancel** — one turn context: `session/cancel` or parent cancel stops model stream, tools, and protocol pump.

---

## Observability (OTLP)

Each registry turn is one root span with only agent-lifecycle children:

```text
tacklr.turn
  event: prompt.received | resume.received
  tacklr.tool              # create_plan, work tools, complete_todo, …
  tacklr.plan.install      # plan document placed in context
  tacklr.context.handoff   # after todo complete / plan revise
  event: turn.ended
```

Streaming messages, absorb, and compress are not traced (plumbing, not milestones). slog gets `trace_id`/`span_id` for correlation but is not mirrored onto spans.

```go
import "github.com/ryanaldo34/tacklr/telemetry"

shutdown, err := telemetry.Init(ctx, telemetry.Config{
    ServiceName:  "my-agent-server",
    OTLPEndpoint: "localhost:4317", // or set OTEL_EXPORTER_OTLP_ENDPOINT
    Insecure:     true,
})
slog.SetDefault(telemetry.NewLogger(slog.NewTextHandler(os.Stderr, nil)))
defer shutdown(ctx)
```

Without an endpoint (and without `OTEL_EXPORTER_OTLP_ENDPOINT`), tracing is a no-op. Prompt/tool body text is not attached by default.

---

## Packages (import map)

```go
import "github.com/ryanaldo34/tacklr"            // harness, tools
import "github.com/ryanaldo34/tacklr/inference"  // OpenAI-compatible strategy
import "github.com/ryanaldo34/tacklr/server"     // Registry, ACP, SSE
import "github.com/ryanaldo34/tacklr/stores"     // InMemory (and Postgres)
import "github.com/ryanaldo34/tacklr/mcp"        // MCPConfig types
import "github.com/ryanaldo34/tacklr/streaming"  // shared event/message types
import "github.com/ryanaldo34/tacklr/control"    // runtime / plan / interrupts
import "github.com/ryanaldo34/tacklr/skills"     // skill loading
import "github.com/ryanaldo34/tacklr/telemetry"  // OTLP tracing + slog span bridge
```

---

## Develop

```bash
make test   # go test ./...
make vet    # go vet
```

See `AGENTS.md` for contribution ethos (context design, testing philosophy, Go standards).

---

## License

See [LICENSE](LICENSE).
