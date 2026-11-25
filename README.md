# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

Tacklr is a Linux-based, cloud-native operating and security system for AI agents. It has three parts:

- **The core (Go).** It runs the agent loop as a durable session: model requests and their API quirks, tool calls, async subagents, long-running commands, and agent swarms. It records every step, so a session can wait days for a person, survive a crash or a deploy, and continue on another machine. It builds for WebAssembly.
- **The supervisor (Go).** It runs on each sandbox host, separate from the session workers. It boots, watches, and kills Firecracker microVMs for the session runtime, over mTLS. It holds the session's credentials and opens its workspace, so no credential enters a VM.
- **The sandbox worker (Go).** It is the init of each microVM. It runs untrusted, LLM-generated bash scripts with native tools (bash, coreutils, git, Python) as an unprivileged user, in a VM with no network. Its `/workspace` is a 9P2000.L mount that the supervisor serves from the session's sources, so only the script's output goes back to the session runtime.

## Features

- **Durable sessions.** Run sessions on [Temporal](https://temporal.io) or on Durable Objects (Cloudflare Workers), with more durable runtimes (Azure Durable Functions, Lambda durable functions) to come. Each model request and tool call runs once, even across restarts.
- **Claude and OpenAI.** Use `anthropic` for Claude (the Claude API or Microsoft Foundry), `openai` for any endpoint that serves the OpenAI Responses API, or implement `tacklr.Model` yourself.
- **Focused context.** The agent plans its work as a to-do list. Finishing a to-do replaces the history with a short hand-off, and a full window collapses into a summary.
- **People in the loop.** A tool can pause the session to ask a question or a permission, and continue when the answer comes back.
- **A sandboxed workspace.** The agent sees one `/workspace` built from local disk, S3, Azure Blob, Google Drive, or Microsoft 365, and works on it with `run_command` scripts that see only that workspace.
- **Sandboxed code execution.** `run_command` runs bash scripts in a Firecracker microVM: its own kernel, a read-only root file system, no network device, and Firecracker in its own namespaces on the sandbox host. Documents read as their text and cannot be written.
- **Security policies (planned).** Worker runtime monitoring and configurable policies, with risky calls sent to security teams to approve or deny.
- **A knowledge store.** Hybrid search, an optional graph, and namespaces for multi-tenant data.
- **Standard protocols.** Serve clients over [ACP](https://agentclientprotocol.com), connect [MCP](https://modelcontextprotocol.io) servers, and trace every turn with OpenTelemetry.
- **Small binaries.** Cloud adapters use plain HTTP instead of vendor SDKs, and the core builds for WebAssembly.

## Install

```bash
go get github.com/ryanaldo34/tacklr
```

Tacklr needs Go 1.27 or later and runs on Linux. A sandbox host needs `/dev/kvm` for Firecracker; `make sandbox-image` builds Firecracker, the guest kernel, and the root file system.

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

Dashed parts are planned and not built yet.

```mermaid
flowchart LR
    client["Clients<br/>ACP editors and apps"] <--> host["Hosts<br/>stateless, session.Client"]
    host <--> runtime["Durable runtime<br/>Temporal, Durable Objects"]
    subgraph sessionworker["Session worker"]
        core["Agent core<br/>agent loop, tools, plans,<br/>specialists, context window"]
    end
    runtime <--> core
    core --> models["Model APIs<br/>Claude, OpenAI"]
    core <--> stores["Sessions and knowledge<br/>Postgres"]
    core --> otel["Observability<br/>OpenTelemetry"]
    core <-- "run_command over mTLS:<br/>bindings, output, token refresh" --> supervisor
    subgraph sandboxhost["Sandbox host"]
        supervisor["Supervisor<br/>delegated tokens,<br/>the session's tree"]
        subgraph vm["Firecracker microVM: no network, no credentials"]
            worker["Worker (init)<br/>bash, git, Python<br/>as the agent user"]
        end
        supervisor <-- "control frames, vsock" --> worker
        supervisor <-- "/workspace: 9P2000.L, vsock" --> worker
    end
    supervisor <--> sources["Sources<br/>S3, Azure Blob, Google Drive,<br/>Microsoft 365, disk"]
    scheduler["Sandbox scheduler<br/>warm pools, snapshots"] -.-> supervisor
    policy["Policy and review<br/>risky calls to people"] -.-> supervisor
    worker -.-> proxy["Egress proxy<br/>allow-list, adds credentials"]

    classDef planned stroke-dasharray: 5 5
    class scheduler,policy,proxy planned
```

- **Hosts** serve clients and keep no session state. The **durable runtime** runs each session on a **session worker**, and every model request and tool call runs once, even across crashes.
- The **agent core** calls the model, runs tools, and saves sessions and knowledge. Each turn is one trace.
- `run_command` goes to a **supervisor** on a separate sandbox host. The supervisor opens the session's tree with the turn's bindings, boots a **microVM**, and refreshes a binding's token through the session runtime when it nears expiry.
- The **worker** is the VM's init. It mounts `/workspace` over 9P from the supervisor and runs the script with native tools. The VM holds no credential and has no network: only the script's output leaves it.

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
make wasm           # checks that the core still builds for WebAssembly
make sandbox-image  # Firecracker, the guest kernel, and the root file system of the sandbox VMs
```

The tests start their own Postgres, Helix, Temporal, S3, and Azure Blob containers, so they need Docker and user namespaces. The sandbox tests boot Firecracker microVMs, so they need `/dev/kvm`, and build the sandbox image themselves.

## License

Apache 2.0. See [LICENSE](LICENSE).
