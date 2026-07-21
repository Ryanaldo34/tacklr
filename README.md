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
import "github.com/ryanaldo34/tacklr/mcp"        // MCP client
import "github.com/ryanaldo34/tacklr/server"     // HTTP/WebSocket/SSE server
import "github.com/ryanaldo34/tacklr/stores"     // session stores
import "github.com/ryanaldo34/tacklr/streaming"  // streaming strategies
```

## Server

The `server` package provides an HTTP/SSE/WebSocket server backed by a `Registry` of agent specs.

```go
import (
    "github.com/ryanaldo34/tacklr"
    "github.com/ryanaldo34/tacklr/server"
    "github.com/ryanaldo34/tacklr/stores"
)

store := stores.NewInMemoryStore()
r := server.NewRegistry(store)
r.Register("my-agent", server.AgentSpec{
    Config: tacklr.Config{
        SystemPrompt:  "You are a helpful assistant.",
        MaxWindowSize: 8192,
    },
    Model: model,  // any tacklr.InferenceStrategy
    Tools: tools,
})

// Serve over standard SSE/JSON protocol.
r.ListenAndServe(":8080", server.ProtocolSSE)
```

Clients send requests using the tacklr JSON format:

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
