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
