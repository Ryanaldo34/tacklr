# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

Tacklr is an opinionated Go SDK for building agent harnesses. It is a framework: it says how a turn should run, how tools execute, how context is structured around a plan, and how a session survives interrupts and restarts.

You bring a model, tools, and the storage you already use. Tacklr sits in the middle of that stack and keeps the run deterministic from the harness’s point of view — even though the model is not.

```bash
go get github.com/ryanaldo34/tacklr
```

---

## Why it exists

Work spans many model calls, tools hit real systems, and the window fills with things that no longer matter. Tacklr is built around four ideas that show up in every design choice.

**Context is structured around the current work.** The harness runs planning cycles (Adaptive Case Management): the agent writes a plan, works a to-do, and on `complete_todo` the window is rebuilt as a hand-off for what comes next. Unused history does not stay in the prompt just because it happened earlier. Specialists are the same idea at a larger grain — a nested session that returns only what the parent asked for.

**The agent’s world is bounded.** A virtual filesystem gives one path API over the mounts you attach: local disk, S3, Google Drive and Docs, Microsoft Graph, and knowledge objects. The agent sees `/work/notes.md`, not a host path or a bucket key. Credentials live on the turn (`Prompt.Auth` / `Resume.Auth`), not in checkpoints.

**Sessions are meant to live in the cloud.** The same harness runs in-process or behind `durable.Runtime` (a goroutine wait loop, or Temporal). Human-in-the-loop parks a session until `Resume`. JSON-RPC protocols (ACP is the native one) map to that Runtime; autonomous hosts call it directly.

**Knowledge is queried, not stuffed into the window.** The optional brain is a host-owned store: first-class objects as Markdown files, hybrid search, an optional graph for relationships, and namespaces so retrieval stays scoped. The agent asks when it needs a fact.

Those four are the ethos. If a change fights them, it does not belong.

---

## A turn

One `Run` (or `Prompt` / `Resume`) is a turn: infer, run tools, maybe park for the user, then complete, error, or cancel.

```text
create_plan → tools → complete_todo → handoff → next work
```

A model round may emit several tool calls. The harness does not infer again until every call in that batch has a result, or a call is parked. Planning builtins (`create_plan`, `list_plan`, `edit_plan`, `complete_todo`) are harness-owned. Your tools cannot rewrite the plan store.

---

## Get started

A model client and a prompt are enough to run a turn.

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
)

func main() {
	ctx := context.Background()

	model := inference.NewOpenAIInferenceStrategy(&http.Client{Timeout: 2 * time.Minute})
	model.WithURL(os.Getenv("OPENAI_BASE_URL")).
		WithApiKey(os.Getenv("OPENAI_API_KEY")).
		WithModel(os.Getenv("OPENAI_MODEL"))

	agent, err := tacklr.NewAgent(ctx, tacklr.AgentOptions{
		Config: tacklr.Config{
			MaxWindowSize: 8192,
			SystemPrompt:  "You are a concise assistant.",
		},
		Model: model,
	})
	if err != nil {
		panic(err)
	}
	defer agent.Close()

	events, err := agent.Run(ctx, "Outline three steps to organize a weekly operations review.")
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

Importing `tacklr` registers built-in interrupts, Word/Excel codecs, and the durable driver adapter. You register VFS backends and brain kinds yourself.

### Tools

Tools are ordinary Go functions. `HarnessRuntime` is for host state, progress, and interrupts.

```go
type SearchArgs struct {
	Query string `json:"query" desc:"The search query"`
	Limit int    `json:"limit,omitempty" desc:"Max results"`
}

tool := tacklr.NewTool(tacklr.ToolConfig{
	Name:        "search_records",
	Description: "Search operational records.",
	Handler: func(ctx context.Context, args SearchArgs, rt tacklr.HarnessRuntime) (string, error) {
		return doSearch(ctx, args.Query, args.Limit)
	},
})
```

Construct with `NewTool(ToolConfig{...})`. After construction, read metadata through getters (`Name()`, `Access()`, and the rest).

### Checkpoints

```go
agent, _ := tacklr.NewAgent(ctx, opts)
cp, _ := agent.Checkpoint()
restored, _ := tacklr.NewAgent(ctx, opts)
_ = restored.RestoreCheckpoint(*cp)
```

A checkpoint is conversation, plan, tool/user state, and pending interrupts. Embed (`NewAgent`) keeps that in process memory unless you snapshot it. `durable.Runtime` writes the same blob to SnapshotStore. Package [`stores`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/stores) is the blob type, not an I/O driver. Checkpoints store mount recipes, not file bytes or tokens. VFS writes persist as they happen.

### Sessions (optional)

For a long-lived wait loop — park, resume, child sessions — use `durable.Runtime` instead of holding one `AgentHarness` yourself:

```go
cat := durable.NewCatalog("agent")
cat.Register("agent", durable.AgentSpec{Options: opts})
rt := inprocess.New(cat, inprocess.WithProjection(vfs.DirectProjection{}))
id, _ := rt.CreateSession(ctx, durable.CreateSession{AgentID: "agent"})
_ = rt.Prompt(ctx, id, durable.Prompt{Text: prompt, Auth: auth})
sub, _ := rt.Subscribe(ctx, id, 0)
```

Temporal is the same `Runtime` interface with a worker. See [docs/durable.md](docs/durable.md).

### Specialists

Register nested agents on `AgentOptions.Specialists`. The model gets `spawn_specialist`, `list_children`, `get_child`, and `cancel_child`. A child is a nested session with the parent’s MCP, mounts, and auth, overlaid with the specialist’s model, tools, and instructions. `block=false` starts the child and returns; `get_child(block=true)` waits. Parent park does not stop children. Cancel (including the original Prompt context) and Close do.

```go
opts.Specialists = []*tacklr.Specialist{{
	Name:         "researcher",
	Description:  "Looks things up and returns a short brief.",
	Model:        model,
	Instructions: "Research the task. Return only the brief.",
}}
```

---

## What you can wire in

| Piece | What it does | Where to read |
|-------|----------------|---------------|
| Planning | `create_plan`, todos, hand-off on complete | this README · [`tacklr`](https://pkg.go.dev/github.com/ryanaldo34/tacklr) |
| Interrupts | Park a tool, collect structured input, `Resume` | [`interrupt`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/interrupt) · [docs/durable.md](docs/durable.md) |
| Specialists | Nested sessions (`spawn_specialist` and children) | [docs/durable.md](docs/durable.md) |
| VFS | Mounts and content IR; file tools `read` / `write` / `run_command` | [docs/vfs.md](docs/vfs.md) |
| Brain | Host-owned knowledge: Engrams, search, optional graph | [docs/knowledge.md](docs/knowledge.md) |
| MCP | External tool servers | [`mcp`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/mcp) |
| Skills | `SKILL.md` catalogs from VFS mounts | [`skills`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/skills) |
| Web | Search and fetch when Exa is configured | [`tacklr`](https://pkg.go.dev/github.com/ryanaldo34/tacklr) |
| Server | `Protocol` over Runtime; ACP is the native option | [`server`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/server) |
| Telemetry | One `tacklr.turn` span per prompt or resume; OTLP | [`telemetry`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/telemetry) |

When VFS is wired, the harness injects file tools over virtual paths only. `run_command` requires permission by default. Live names and grep go through `run_command` (`ls` / `fd` / `rg`). With Brain + VFS + a search namespace, the harness mounts `/engram` and injects knowledge tools. Details: [docs/vfs.md](docs/vfs.md) and [docs/knowledge.md](docs/knowledge.md).

---

## Documentation

| Doc | What it covers |
|-----|----------------|
| [docs/durable.md](docs/durable.md) | Runtime: embed, in-process, Temporal; HITL; children; auth |
| [docs/vfs.md](docs/vfs.md) | Mounts, content IR, providers, FUSE |
| [docs/knowledge.md](docs/knowledge.md) | Brain: Engrams, search, graph, tools |
| [docs/fuse-vfs-run-command.md](docs/fuse-vfs-run-command.md) | How `run_command` and the FUSE projection fit |
| [pkg.go.dev/tacklr](https://pkg.go.dev/github.com/ryanaldo34/tacklr) | Harness, tools, types |
| [AGENTS.md](AGENTS.md) | Goals, coding standards, how we test |

---

## Packages

| Package | Role |
|---------|------|
| `tacklr` | Harness, tools, plan loop, specialists |
| `vfs` | Virtual filesystem, mounts, content IR |
| `vfsindex` | Optional mount → brain ingest |
| `brain` | Knowledge engine |
| `brain/helixgraph` | Optional graph adapter |
| `inference` | OpenAI-compatible model client |
| `server` | Protocol host over Runtime |
| `durable` | Session Runtime (in-process or Temporal) |
| `stores` | Checkpoint blob (`SessionCheckpoint`) |
| `interrupt` | Pause / resume types |
| `streaming` | Shared messages and events |
| `mcp` | MCP config types |
| `skills` | Skill loading from VFS mounts |
| `telemetry` | OpenTelemetry helpers |

---

## Contributing

This repo is a Go module. Start with [`AGENTS.md`](AGENTS.md) for goals and coding standards. Short version:

- Prefer the standard library. Third-party packages are a last resort.
- Prefer small, explicit pieces over hidden abstractions.
- Tests are outcome-oriented integration tests. Assert what should happen, not that a private helper ran. Avoid duplicate coverage of the same return path.

```bash
make test-short   # no Docker
make test         # includes brain Postgres + Helix (Docker)
make vet
make lint
```

Where to look:

| Area | Start here |
|------|------------|
| Turn loop, tools, plan | `agent.go`, `agent_run.go`, `tools.go` |
| Specialists / children | `subagents.go`, `jobs.go`, `durable/child.go`, `durable/inprocess/` |
| Runtime | `durable/runtime.go`, `docs/durable.md` |
| VFS | `vfs/`, `docs/vfs.md` |
| Knowledge | `brain/`, `docs/knowledge.md` |
| ACP / protocols | `server/` |

Issues and pull requests are welcome. Match the surrounding code: `gofmt`, `go vet`, `golangci-lint`.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
