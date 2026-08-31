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

**The agent’s world is bounded.** A virtual filesystem gives one path API over the mounts you attach: local disk, S3, Azure Blob, Google Drive and Docs, Microsoft Graph, and knowledge objects. The agent sees `/workspace/work/notes.md`, not a host path or a bucket key. Credentials live on the turn (`Prompt.Auth` / `Resume.Auth`), not in checkpoints or Temporal event history.

**Sessions are meant to live in the cloud.** Hosts call `durable.Runtime` (in-process goroutine wait loop, or Temporal). Human-in-the-loop parks a session until `Resume`. JSON-RPC protocols (ACP is the native one) map to that Runtime; autonomous hosts call it directly.

**Knowledge is queried, not stuffed into the window.** The optional brain is a host-owned store: first-class objects as Markdown files, hybrid search, an optional graph for relationships, and namespaces so retrieval stays scoped. The agent asks when it needs a fact.

Those four are the ethos. If a change fights them, it does not belong.

---

## A turn

One `Prompt` or `Resume` is a turn: infer, run tools, maybe park for the user, then complete, error, or cancel.

```text
create_plan → tools → complete_todo → handoff → next work
```

A model round may emit several tool calls. The harness does not infer again until every call in that batch has a result, or a call is parked. Planning builtins (`create_plan`, `list_plan`, `edit_plan`, `complete_todo`) are harness-owned. Your tools cannot rewrite the plan store.

---

## Get started

This is a host: a model, a brain, a `/workspace` tree, a durable runtime, and ACP on HTTP. The protocol never talks to Temporal (or the in-process loop) in their own dialect — it consumes `tacklr.StreamEvent` from `Runtime`.

Session data is three frozen planes. Do not mix them:

| Plane | You pass | Holds |
|-------|----------|--------|
| **SnapshotStore** | `Config.Snapshots` | Window, plan, parked interrupt, `userState`, VFS recipes, session identity |
| **Wait loop** | `inprocess.New` or `temporal.New` + `NewWorker` | Scheduler: leftover Temporal tool calls, MCP Durable topology, child futures, Status |
| **SecretStorage** | `temporal.Config.Secrets` (required; same instance on New and NewWorker) | VFS tokens. Not in snapshots. Not in Temporal history |

In-process keeps work-item tokens in RAM for the turn. Temporal puts them in `Secrets` before signaling. `Prompt.Auth` / `Resume.Auth` is still how the host hands tokens in.

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName:  "tacklr-host",
		OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Insecure:     true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	model := builtins.NewOpenAIInferenceStrategy(&http.Client{Timeout: 2 * time.Minute})
	model.WithURL(os.Getenv("OPENAI_BASE_URL")).
		WithApiKey(os.Getenv("OPENAI_API_KEY")).
		WithModel(os.Getenv("OPENAI_MODEL"))

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	kinds := []brain.KindSpec{
		{Kind: "Discovery", Description: "Research finding", IsParent: true},
		{Kind: "Fact", Description: "Verified fact", IsParent: true},
		{Kind: "Memory", Description: "Durable memory", IsParent: true},
	}
	store, err := postgres.New(pool)
	if err != nil {
		log.Fatal(err)
	}
	if err := store.Setup(ctx, kinds...); err != nil {
		log.Fatal(err)
	}
	g, err := helixgraph.New(os.Getenv("HELIX_URL"))
	if err != nil {
		log.Fatal(err)
	}
	if err := g.Bootstrap(ctx, false); err != nil {
		log.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithLexicalOnly())
	if err != nil {
		log.Fatal(err)
	}
	if err := eng.LoadKindsFromStore(ctx); err != nil {
		log.Fatal(err)
	}
	ns, err := brain.ParseNamespace("org", "acme")
	if err != nil {
		log.Fatal(err)
	}

	jail := filepath.Join(os.TempDir(), "tacklr-workspace")
	if err := os.MkdirAll(filepath.Join(jail, "skills"), 0o750); err != nil {
		log.Fatal(err)
	}
	exa := builtins.NewExa(os.Getenv("EXA_API_KEY"))

	cat := durable.NewCatalog("agent")
	cat.Register("agent", durable.AgentSpec{
		Name: "Agent",
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{
				MaxWindowSize: 8192,
				SystemPrompt:  "You are a concise assistant.",
			},
			Model:           model,
			Brain:           eng,
			SearchNamespace: ns,
			BrainWriteKinds: brain.WriteKinds{
				Discovery: "Discovery",
				Fact:      "Fact",
				Memory:    "Memory",
			},
			Tools: []*tacklr.Tool{
				builtins.WebSearch(exa),
				builtins.WebFetch(exa),
			},
		},
		OpenVFS:    openVFS(jail, eng, ns),
		OpenSkills: vfs.Tree(vfs.At("skills", vfs.Union(builtins.Local(filepath.Join(jail, "skills"))))),
	})

	snaps := inprocess.NewMemorySnapshot()
	rt := inprocess.New(inprocess.Config{
		Catalog:    cat,
		Snapshots:  snaps,
		Projection: vfs.DirectProjection{},
	})
	srv := server.NewServer(rt, cat, server.NewACPProtocol(nil)).AllowAnonymousNetwork()
	log.Printf("ACP on http://127.0.0.1:8080/acp")
	if err := srv.ServeHTTP(ctx, "127.0.0.1:8080"); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func openVFS(jail string, eng *brain.Engine, ns brain.Namespace) vfs.OpenVFS {
	return func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
		members := []vfs.Member{
			vfs.At("work", builtins.Local(jail)),
			vfs.At("engram", brain.Open(eng, brain.Scope{Namespace: ns})),
			vfs.At("memory", builtins.Memory()),
		}
		if b, ok := vfs.BindingByName(req.Bindings, "drive"); ok && strings.TrimSpace(b.Auth.Token) != "" {
			h := vfs.NewTokenHolder(b.Auth)
			api, err := builtins.NewGoogleDrive(ctx, h)
			if err != nil {
				return nil, err
			}
			members = append(members, vfs.At("drive", builtins.Drive(api)))
		}
		if b, ok := vfs.BindingByName(req.Bindings, "sharepoint"); ok && strings.TrimSpace(b.Auth.Token) != "" {
			h := vfs.NewTokenHolder(b.Auth)
			api, err := builtins.NewGraph(h, "", nil)
			if err != nil {
				return nil, err
			}
			members = append(members, vfs.At("sharepoint", builtins.Graph(api, h, b.Params[vfs.ParamAccount])))
		}
		return vfs.Tree(members...)(ctx, sessionID, req)
	}
}
```

Same Catalog and SnapshotStore on Temporal. `Secrets` is required and must be one instance both processes can `Get`:

```go
import (
	"go.temporal.io/sdk/client"

	tacklrtemporal "github.com/ryanaldo34/tacklr/durable/temporal"
)

c, err := tacklrtemporal.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_HOST")})
if err != nil {
	log.Fatal(err)
}
secrets := durable.NewMemorySecretStorage() // production: Redis / Postgres / Vault
cfg := tacklrtemporal.Config{
	Catalog:    cat,
	Snapshots:  snaps,
	Secrets:    secrets,
	Projection: vfs.DirectProjection{},
}
w := tacklrtemporal.NewWorker(c, cfg)
if err := w.Start(); err != nil {
	log.Fatal(err)
}
defer w.Stop()
rt := tacklrtemporal.New(c, cfg)
```

ACP `_tacklr/vfs/bind` still maps onto `Prompt.Auth`. The worker never sees those tokens in workflow history.

Importing `tacklr` registers built-in interrupts, Word/Excel codecs, and the durable driver adapter. The agent sees `/workspace/work`, `/workspace/engram`. Skills load from `OpenSkills` and reach the model only through `read_skill`. A Drive or SharePoint bind on the prompt adds `/workspace/drive` or `/workspace/sharepoint` for that turn. Tests pass a fake `DriveAPI` / `GraphAPI` into the same `builtins.Drive` / `builtins.Graph` constructors. `WithLexicalOnly` is the explicit no-embedder choice; production hosts pass `brain.WithEmbedder`.

`telemetry.Init` installs the process-wide OpenTelemetry providers. With `OTLPEndpoint` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) it exports traces, metrics, and logs over OTLP (gRPC by default, or HTTP). Without an endpoint it still installs Temporal’s ReplaySafe tracer so workflow replay does not leak spans. Each Prompt or Resume is one `tacklr.turn` span; inference, tools, hand-off, and compress nest under it. `postgres.Store` Query/Exec spans join that same trace. Hosts must not start `tacklr.*` spans themselves. Metrics include turn duration and count, tool calls, model tokens, interrupts, hand-offs, compress, sessions, and checkpoints. Call `Init` before `durable/temporal.Dial`. Details: [`telemetry`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/telemetry).

### Tools

Tools are ordinary Go functions. Give a tool a client by closing over it in the constructor. That is the dependency injection. Tests pass a fake into the same constructor.

`HarnessRuntime` is park, progress, children, and session key-values (`StateGet`). Put facts like the current user on `CreateSession.State` (also `Prompt.State` / `Resume.State`). Close over clients in the constructor: `NewSearchRecordsTool(liveStore)` in production, `NewSearchRecordsTool(fakeStore)` in tests. Construct with `NewTool(ToolConfig{...})`. After construction, read metadata through getters (`Name()`, `Access()`, and the rest).

Built-in tools that need a client use the same pattern. You construct them and put them on `AgentOptions.Tools`:

| You construct | Closed into |
|---------------|-------------|
| `builtins.ReadInbox` / `builtins.SendEmail` | `read_inbox`, `send_email` |
| `builtins.WebSearch` / `builtins.WebFetch` | `web_search`, `web_fetch` |
| `MountSession` | `read`, `write`, `write_document`, `write_spreadsheet`, `run_command` |
| `SkillsSession` (`OpenSkills`) | `read_skill` |
| `Brain` | knowledge tools (`search`, `save_*`, …) |
| index bridge (from Brain + VFS) | `index_file`, `unindex` |

Put optional builtins on `AgentOptions.Tools`. Swap the fake the same way: `Tools: []*tacklr.Tool{builtins.ReadInbox(fakeMail)}`, `Brain: testEngine`, a temp `MountSession`. Details: [docs/tools.md](docs/tools.md).

### Session data

`durable.Runtime` is the host API (the snippets above). `TurnManager` is the per-turn infer/tools/checkpoint object the runtime constructs; hosts do not call it.

| Plane | Canonical store |
|-------|-----------------|
| Conversation, plan, parked interrupt, `userState`, VFS recipes, identity | `durable.SnapshotStore` (`tacklr.SessionCheckpoint` plus `Snapshot.Mounts`) |
| Leftover unstarted Temporal tool calls, MCP Durable topology, child futures, Status | Wait loop (in-process goroutine or Temporal workflow replay) |
| VFS tokens | `durable.SecretStorage` on Temporal; process RAM on in-process |

`CreateSession.State` / `Prompt.State` / `Resume.State` merge into checkpoint `userState`. They are not a second Temporal copy of that map. File bytes are never snapshotted; VFS writes persist as they happen. Close deletes the snapshot and the secret bag. Details: [docs/durable.md](docs/durable.md).

### Specialists

Register nested agents on `AgentOptions.Specialists`. Tools start them through `HarnessRuntime`: `SpawnChild`, `Children`, `AwaitChild`, `CancelChild`. The stock tools `spawn_specialist`, `list_children`, `get_child`, and `cancel_child` call those methods; host tools can too. A child is a nested session with the parent’s MCP Durable topology and mount recipes, overlaid with the specialist’s model, tools, and instructions. Tokens come from `SecretStorage` (child, then parent). `block=false` starts the child and returns; `get_child(block=true)` waits. Parent park does not stop children. Cancel (including the original Prompt context) and Close do.

---

## What you can wire in

| Piece | What it does | Where to read |
|-------|----------------|---------------|
| Planning | `create_plan`, todos, hand-off on complete | this README · [`tacklr`](https://pkg.go.dev/github.com/ryanaldo34/tacklr) |
| Interrupts | Park a tool, collect structured input, `Resume` | [`interrupt`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/interrupt) · [docs/durable.md](docs/durable.md) |
| Specialists | Nested sessions (`spawn_specialist` and children) | [docs/durable.md](docs/durable.md) |
| VFS | Mounts and content IR; file tools `read` / `write` / `run_command` | [docs/vfs.md](docs/vfs.md) |
| Brain | Host-owned knowledge: Engrams, search, optional graph | [docs/knowledge.md](docs/knowledge.md) |
| Host tools | Your functions; close over clients in the constructor | [docs/tools.md](docs/tools.md) |
| MCP | External tool servers | [`mcp`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/mcp) |
| Skills | `SKILL.md` catalogs from `OpenSkills`; the model reads them only through `read_skill` | [`skills`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/skills) |
| Model | `tacklr.InferenceStrategy`; OpenAI-compatible client is `builtins.NewOpenAIInferenceStrategy` | [`tacklr`](https://pkg.go.dev/github.com/ryanaldo34/tacklr) · [`builtins`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/builtins) |
| Web | `web_search` and `web_fetch` via `builtins.WebSearch` / `builtins.WebFetch` | [`builtins`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/builtins) |
| Email | `read_inbox` and permission-gated `send_email` via `builtins.ReadInbox` / `builtins.SendEmail` | [`builtins`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/builtins) |
| Server | `Protocol` over Runtime; ACP is the native option | [`server`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/server) |
| Telemetry | `telemetry.Init`: OTLP traces/metrics/logs; one `tacklr.turn` span per prompt or resume | [`telemetry`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/telemetry) |

When VFS is wired, the harness injects file tools over virtual paths only. `run_command` requires permission by default. Live names and grep go through `run_command` (`ls` / `fd` / `rg`). With Brain + VFS + a search namespace, knowledge tools attach to `/workspace/engram`. Details: [docs/vfs.md](docs/vfs.md) and [docs/knowledge.md](docs/knowledge.md).

---

## Documentation

| Doc | What it covers |
|-----|----------------|
| [docs/durable.md](docs/durable.md) | Runtime: in-process, Temporal; three data planes; HITL; children; auth |
| [docs/tools.md](docs/tools.md) | Tool clients: constructor closures, tests, builtins |
| [docs/vfs.md](docs/vfs.md) | Mounts, content IR, providers, FUSE |
| [docs/knowledge.md](docs/knowledge.md) | Brain: Engrams, search, graph, tools |
| [docs/fuse-vfs-run-command.md](docs/fuse-vfs-run-command.md) | How `run_command` and the FUSE projection fit |
| [pkg.go.dev/tacklr](https://pkg.go.dev/github.com/ryanaldo34/tacklr) | Harness, tools, types |
| [AGENTS.md](AGENTS.md) | Goals, coding standards, how we test |

---

## Packages

| Package | Role |
|---------|------|
| `tacklr` | Harness, tools, plan loop, specialists, messages, checkpoints |
| `builtins` | Optional tools (email, Exa), VFS constructors, OpenAI model client |
| `vfs` | Virtual filesystem, mounts, content IR |
| `vfsindex` | Optional mount → brain ingest |
| `brain` | Knowledge engine, store/graph interfaces, in-memory backends |
| `brain/postgres` | Optional Postgres `brain.Store` |
| `brain/helixgraph` | Optional Helix graph adapter |
| `server` | Protocol host over Runtime |
| `durable` | Session Runtime, SnapshotStore, SecretStorage |
| `interrupt` | Pause / resume types |
| `mcp` | MCP config types |
| `skills` | Skill loading from the host-only `OpenSkills` tree |
| `telemetry` | OpenTelemetry helpers |

---

## Contributing

This repo is a Go module. It requires **Go 1.27**. Start with [`AGENTS.md`](AGENTS.md) for goals and coding standards. Short version:

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
| Messages / checkpoints | `message.go`, `checkpoint.go` |
| Specialists / children | `subagents.go`, `durable/child.go`, `durable/inprocess/` |
| Runtime | `durable/runtime.go`, `docs/durable.md` |
| VFS | `vfs/`, `docs/vfs.md` |
| Model client | `builtins/openai.go` (`tacklr.InferenceStrategy`) |
| Knowledge | `brain/`, `brain/postgres/`, `brain/helixgraph/`, `docs/knowledge.md` |
| ACP / protocols | `server/` |

Issues and pull requests are welcome. Match the surrounding code: `gofmt`, `go vet`, `golangci-lint`.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
