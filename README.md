# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

Tacklr is a Linux-based, cloud-native operating and security system for AI agents. It has three parts:

- **The core (Go).** It runs the agent loop as a durable session: model requests and their API quirks, tool calls, async subagents, long-running commands, and agent swarms. It records every step, so a session can wait days for a person, survive a crash or a deploy, and continue on another machine. It builds for WebAssembly.
- **The supervisor (Go).** It runs on each sandbox host, separate from the session workers. It opens each session's workspace with the session's credentials, and starts, watches, and kills sandboxed workers for the session runtime, over mTLS.
- **The sandbox worker (Rust).** It runs untrusted, LLM-generated bash scripts in WebAssembly (Wasmer with WASIX: bash, coreutils, grep, sed, and findutils, compiled ahead of time), inside Linux namespaces, Landlock, and seccomp. The scripts reach only the session's workspace, which the supervisor serves to the worker on the same host, so only the script's output goes back to the session runtime.

## Features

- **Durable sessions.** Run sessions on [Temporal](https://temporal.io) or on Durable Objects (Cloudflare Workers), with more durable runtimes (Azure Durable Functions, Lambda durable functions) to come. Each model request and tool call runs once, even across restarts.
- **Claude and OpenAI.** Use `anthropic` for Claude (the Claude API or Microsoft Foundry), `openai` for any endpoint that serves the OpenAI Responses API, or implement `tacklr.Model` yourself.
- **Focused context.** The agent plans its work as a to-do list. Finishing a to-do replaces the history with a short hand-off, and a full window collapses into a summary.
- **People in the loop.** A tool can pause the session to ask a question or a permission, and continue when the answer comes back.
- **A sandboxed workspace.** The agent sees one `/workspace` built from local disk, S3, Azure Blob, Google Drive, or Microsoft 365, and works on it with `run_command` scripts that see only that workspace.
- **Sandboxed code execution.** `run_command` runs each bash script in its own worker process: WASIX bash with no network, new namespaces, no host file system access (Landlock), and a seccomp allow-list with no executable memory and no new processes. Documents read as their text and cannot be written.
- **Security policies (planned).** Worker runtime monitoring and configurable policies, with risky calls sent to security teams to approve or deny.
- **A knowledge store.** Hybrid search, an optional graph, and namespaces for multi-tenant data.
- **Standard protocols.** Serve clients over [ACP](https://agentclientprotocol.com), connect [MCP](https://modelcontextprotocol.io) servers, and trace every turn with OpenTelemetry.
- **Small binaries.** Cloud adapters use plain HTTP instead of vendor SDKs, and the core builds for WebAssembly.

## Install

```bash
go get github.com/ryanaldo34/tacklr
```

Tacklr needs Go 1.27 or later and runs on Linux. The sandbox worker needs Rust (the version in `sandbox/worker/rust-toolchain.toml`) and Linux 6.12 or later (Landlock ABI 6).

## Quick start

This program runs a Temporal worker and client in one process, sends one prompt, and prints the answer. Start a local Temporal server first with `temporal server start-dev` ([Temporal CLI](https://docs.temporal.io/cli)).

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/anthropic"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/worker"
)

type cityArgs struct {
	City string `json:"city" desc:"City name"`
}

func main() {
	ctx := context.Background()

	weather := tacklr.NewTool(tacklr.ToolConfig{
		Name:        "get_weather",
		Description: "Return the current weather for a city.",
		Handler: func(ctx context.Context, args cityArgs) (string, error) {
			return "Sunny and 24°C in " + args.City, nil
		},
	})
	agent := tacklr.AgentOptions{
		SystemPrompt: "You are a helpful assistant.",
		Model:        anthropic.New(nil).WithApiKey(os.Getenv("ANTHROPIC_API_KEY")).WithModel("claude-opus-5-5"),
		Tools:        []*tacklr.Tool{weather},
	}

	c, err := temporal.Dial(client.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// In-memory stores work when the worker and the client share a process.
	store, secrets := session.NewMemoryStore(), session.NewMemorySecretStorage()

	w := worker.New(c, worker.Config{Agent: agent, Store: store, Secrets: secrets})
	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
	defer w.Stop()

	sessions := temporal.Open(c, temporal.Config{Store: store, Secrets: secrets})
	id, err := sessions.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		log.Fatal(err)
	}
	events, err := sessions.Subscribe(ctx, id, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer events.Close()

	if err := sessions.Prompt(ctx, id, session.Prompt{Text: "What is the weather in Lisbon?"}); err != nil {
		log.Fatal(err)
	}
	for ev := range events.Events() {
		switch ev.Type {
		case session.EventMessage:
			fmt.Println(ev.Content)
		case session.EventComplete, session.EventError:
			return
		}
	}
}
```

```bash
ANTHROPIC_API_KEY=... go run .
```

The model calls `get_weather`, Tacklr runs it, and the model answers from the result. To use OpenAI instead, set `Model` to `openai.New(nil).WithApiKey(key).WithModel("gpt-5-mini")`.

The [getting started guide](docs/getting-started.md) continues from here: asking the user a question, adding a workspace, and splitting the program for production.

## How it works

Untrusted code reaches files and the network only through Tacklr host functions. Dashed parts are planned and not built yet.

```mermaid
flowchart LR
    client["ACP client<br/>editor or app"] <--> session["Session DO<br/>Cloudflare: journal, replay"]
    session --> model["Model API<br/>Claude, OpenAI"]
    session --> gateway["Sandbox gateway<br/>leases, exactly-once calls"]
    gateway -.-> microvm["MicroVM tier<br/>native binaries, later"]
    detector["Risk detector<br/>park or end risky calls"] -. verdicts .-> gateway
    gateway --> shell
    subgraph worker["Sandbox worker: jailed process, no network"]
        shell["Shell interpreter<br/>mvdan/sh, bash syntax"] --> kernel["Virtual kernel<br/>host calls, policy, events"]
        tools["WASM tools<br/>coreutils, grep, jq, CPython on WASI"] <--> kernel
    end
    kernel --> vfs["VFS service<br/>mounts, codecs, credentials<br/>reads Drive, M365, S3"]
    kernel --> proxy["Egress proxy<br/>full-URL allow-list<br/>adds credentials outside"]
    kernel --> detector

    classDef planned stroke-dasharray: 5 5
    class gateway,microvm,detector,shell,kernel,tools,proxy planned
```

- The **session** Durable Object orchestrates and journals each turn. It replays from its journal, so a tool call never runs twice.
- The **sandbox gateway** runs each call exactly once in a jailed **sandbox worker**, keyed by its tool call id.
- Inside the worker, the shell and every WASM tool reach files, the network and the risk detector only through the **virtual kernel**'s host calls.
- The **VFS service** reads the session's mounts with their codecs, so a Google Doc reads as text. The **egress proxy** allows only listed URLs and adds credentials outside the sandbox.
- The **risk detector** parks or ends a risky call for a person to review.

See [Concepts](docs/concepts.md) for sessions, turns, rounds, and the context window.

## Documentation

| Guide | Covers |
|---|---|
| [Getting started](docs/getting-started.md) | A first agent, questions to the user, next steps |
| [Concepts](docs/concepts.md) | Sessions, turns, rounds, plans, specialists, events |
| [Models](docs/models.md) | Claude, OpenAI and compatible endpoints, writing your own `Model` |
| [Tools](docs/tools.md) | Writing tools, permissions, built-in tools, MCP, skills |
| [Workspace](docs/workspace.md) | The agent's `/workspace` on the sandbox host, its sources, and how scripts work on it |
| [Knowledge](docs/knowledge.md) | The knowledge store: kinds, search, the graph, file indexing |
| [Sessions in production](docs/session.md) | Temporal and Durable Objects, Postgres, credentials, specialists and jobs |
| [Protocols](docs/protocol.md) | The ACP server, a turn from prompt to answer, writing a protocol |
| [Context window](docs/context.md) | How each request is sized, collapsed, and cut |
| [Telemetry](docs/telemetry.md) | Traces, logs, and metrics for every turn |

API reference: [pkg.go.dev/github.com/ryanaldo34/tacklr](https://pkg.go.dev/github.com/ryanaldo34/tacklr).

## Examples

- [examples/temporal](examples/temporal): a production layout on Temporal, with an ACP host, a worker, Postgres, and an end-to-end test.
- [examples/cloudflare](examples/cloudflare): sessions as Durable Objects on Cloudflare Workers, with the same ACP server.

## Contributing

Read [AGENTS.md](AGENTS.md) for the coding and testing standards. Tacklr supports Linux only; on a Mac, use a Linux VM.

```bash
make cover   # all tests with the race detector, and the coverage gate
make lint
make wasm    # checks that the core still builds for WebAssembly
make sandbox-packages       # fetches the pinned WASIX packages and compiles them ahead of time
make rust-lint rust-cover   # the sandbox worker: clippy, tests, and its coverage gate
```

The tests start their own Postgres, Helix, Temporal, S3, and Azure Blob containers, so they need Docker and user namespaces. The sandbox tests build the Rust worker with cargo.

## License

Apache 2.0. See [LICENSE](LICENSE).
