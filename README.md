# tacklr

Agent harness for building LLM-powered applications with tool use, session management, MCP support, and streaming.

## Install

```bash
go get github.com/ryanaldo34/tacklr
```

## Usage

```go
import "github.com/ryanaldo34/tacklr"

h := tacklr.NewAgent(tacklr.Config{MaxWindowSize: 8192}, model, runtime, watchdog)
events, err := h.Run(ctx, "your prompt")
```

### Subpackages

```go
import "github.com/ryanaldo34/tacklr/control"    // session/runtime types
import "github.com/ryanaldo34/tacklr/inference"  // inference strategies
import "github.com/ryanaldo34/tacklr/mcp"        // MCP server config types
import "github.com/ryanaldo34/tacklr/server"     // HTTP/WebSocket/SSE server
import "github.com/ryanaldo34/tacklr/stores"     // session stores
import "github.com/ryanaldo34/tacklr/streaming"  // streaming strategies
```

## Tools

Tools are defined using `NewTool`:

```go
import "github.com/ryanaldo34/tacklr"

type SearchArgs struct {
    Query string `json:"query" desc:"The search query"`
    Limit int    `json:"limit" desc:"Max results"`
}

tool := tacklr.NewTool(tacklr.ToolConfig{
    Name:        "search_web",
    Description: "Search the web for information.",
    Handler: func(ctx context.Context, args SearchArgs, rt tacklr.HarnessRuntime) (string, error) {
        return doSearch(ctx, args.Query, args.Limit)
    },
})
```

Valid handler signatures:
- `func(context.Context) (T, error)` — no arguments
- `func(context.Context, Args) (T, error)` — typed args struct
- `func(context.Context, Args, HarnessRuntime) (T, error)` — with runtime access
- `func(context.Context, HarnessRuntime) (T, error)` — runtime only, no args

The args struct is inspected with `reflect` at tool construction to auto-generate the JSON schema. Tags (`json`, `desc`, `enum`) control the schema output. `HarnessRuntime` is passed by value (a shallow copy with `CurrentToolCallID` set uniquely per call), giving tools access to interrupt and progress-update APIs.

MCP-discovered tools are handled internally by the harness — configure `MCPConfigs` (on `AgentSpec` or the harness directly) and the discovery + tool wrapping happens automatically.

## MCP servers

The public `mcp` package exposes only configuration types. Connection, discovery,
and client lifecycle are internal to the harness.

`mcp.MCPConfig` mirrors the ACP `mcpServers` wire shape and supports all three transports:

```go
// stdio (default): launch the server as a subprocess
mcp.MCPConfig{Name: "fs", Command: "npx", Args: []string{"-y", "@modelcontextprotocol/server-filesystem"},
    Env: []mcp.EnvVariable{{Name: "API_KEY", Value: "secret"}}}

// streamable HTTP
mcp.MCPConfig{Name: "api", Type: mcp.TransportHTTP, URL: "https://api.example.com/mcp",
    Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer tok"}}}

// SSE (deprecated by the MCP spec)
mcp.MCPConfig{Name: "events", Type: mcp.TransportSSE, URL: "https://events.example.com/mcp"}
```

Pass configs via `AgentSpec.MCPConfigs` or the harness `MCPConfigs` field. Over
ACP, clients supply the same shapes in `session/new`, `session/load`, and
`session/resume`; the harness connects and discovers tools at the start of the
next turn. The agent advertises `mcpCapabilities.http` and `mcpCapabilities.sse`
as supported (stdio is always supported).

### Usage in an agent

```go
agent := tacklr.NewAgent(tacklr.AgentOptions{
    Config: tacklr.Config{
        MaxWindowSize: 8192,
        SystemPrompt:  "You are a helpful assistant.",
    },
    Model: model,
    Store: store,
    Tools: []*tacklr.Tool{tool},
})
```

## Server

The `server` package separates domain logic (`Registry`) from transport (`Server`)
and wire format (`Protocol`). Built-in protocols: `server.ACP` (JSON-RPC) and
`server.SSE` (native SSE/WS).

```go
import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "github.com/ryanaldo34/tacklr"
    "github.com/ryanaldo34/tacklr/server"
    "github.com/ryanaldo34/tacklr/stores"
)

r := server.NewRegistry(store, "my-agent")
r.Register("my-agent", server.AgentSpec{
    Config: tacklr.Config{
        SystemPrompt:  "You are a helpful assistant.",
        MaxWindowSize: 8192,
    },
    Model: model,  // any tacklr.InferenceStrategy
    Tools: tools,
})

srv := server.NewServer(r, server.SSE) // or server.ACP

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

// HTTP (routes depend on Protocol.HTTPMode; shuts down on ctx cancel)
// go srv.ServeHTTP(ctx, ":8080")

// Stdio / NDJSON (returns on EOF or ctx cancel)
_ = srv.ServeStdio(ctx, os.Stdin, os.Stdout)
```

Domain errors unwrap to sentinels (`server.ErrAgentNotFound`, etc.) via `errors.Is`.
Wire responses use `PublicError` so internal failures become `ErrInternal`.


### SSE clients

```bash
# Prompt
curl -N -X POST http://localhost:8080/ \
  -H "Accept: text/event-stream" \
  -d '{"agent_id":"my-agent","prompt":"Hello"}'

# Resume from interrupt
curl -N -X POST http://localhost:8080/resume \
  -H "Accept: text/event-stream" \
  -d '{"agent_id":"my-agent","thread_id":"<id>","responses":{"<interruptId>":{"selectionIdx":0}}}'
```

WebSocket connections use the same JSON payload as the initial message (GET / for prompt, GET /resume for resume).

## Skills

Applications can load local `SKILL.md` directories into an agent with the
existing `NewAgent` constructor. The first `Run` adds a compact skill catalog
to the system prompt and registers `read_skill`, allowing the model to load
full instructions only when needed:

```go
agent := tacklr.NewAgent(
    tacklr.Config{
        MaxWindowSize:    8192,
        SystemPrompt:     "You are a helpful assistant.",
        SkillDirectories: []string{"./skills"},
    },
    model, store, watchdog,
)
```

Each immediate child of a configured directory must contain a `SKILL.md` file
with YAML front matter containing `name` and `description`, followed by an
instruction body:

```markdown
---
name: testing
description: Write and validate automated tests for this project.
---

Follow the project's test conventions and run the relevant tests before
reporting completion.
```

## Build

```bash
make test      # runs all tests
make vet       # runs go vet
```
```
