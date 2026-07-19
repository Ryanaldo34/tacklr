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
import "github.com/ryanaldo34/tacklr/control"  // session/runtime types
import "github.com/ryanaldo34/tacklr/mcp"       // MCP client
import "github.com/ryanaldo34/tacklr/openai"    // OpenAI inference strategy
```

## Build

```bash
make build     # builds tackle-server binary
make test      # runs all tests
make vet       # runs go vet
```

## Server

The `cmd/server` directory contains an HTTP/WebSocket/SSE server that uses the tacklr harness. See `.env.example` for configuration.

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
