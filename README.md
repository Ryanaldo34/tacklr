# Tacklr

[![CI](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml/badge.svg)](https://github.com/Ryanaldo34/tacklr/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Ryanaldo34/tacklr/main/docs/badges/coverage.json)](https://github.com/Ryanaldo34/tacklr/blob/main/.testcoverage.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryanaldo34/tacklr.svg)](https://pkg.go.dev/github.com/ryanaldo34/tacklr)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/go.mod)
[![License](https://img.shields.io/github/license/Ryanaldo34/tacklr)](https://github.com/Ryanaldo34/tacklr/blob/main/LICENSE)

**Open-source agent harness for Go.** Tacklr is a framework, not a toolbox of optional helpers. It defines how agents should run—how work is planned, how context is structured, how tools execute under scope, and how state survives—so agents stay efficient, secure, and operable.

```bash
go get github.com/ryanaldo34/tacklr
```

---

## Why Tacklr exists

Most agent harnesses share the same weaknesses.

They **struggle to structure context**. History piles up; when the window fills, the answer is usually a blunt summarize-or-trim pass that loses the thread of the task. They are **not token-efficient**: every turn re-pays for noise that no longer matters. They are **not secure by construction**—access control is left to prompts, host glue, or hope. They often **perform poorly** under real load because the loop, state, and I/O were never designed as infrastructure. And they are **sloppily architected**: model code, tool code, and product code tangled together, hard to test and harder to operate.

Tacklr exists to fix that class of problem. An agent is a **system** you run—with explicit planning, deliberate context handoffs, security as a platform concern, and a clean separation between model I/O, the turn loop, storage, and knowledge—not a chat transcript you hope finishes well.

---

## Ethos

### Structured context and token efficiency

Context should serve the **current task**, not archive every token the model has ever seen. Tacklr drives work through **plans and todos**. When a step completes, the harness performs a **handoff**: it rebuilds a focused context for what comes next. Irrelevant history does not keep a permanent seat in the window. That is how you preserve quality and keep spend predictable.

### Security baked in

Unscoped tools and unbounded environments are how agents cause damage. Security is not a host afterthought. Tacklr’s direction is a **virtual execution environment and filesystem**: one controlled interface over the resources an agent may touch—object storage, local paths, and external drives—without the agent needing to know or escape that boundary. Scope is a property of the platform, not a line in the system prompt.

Hosts wire mounts on a session-owned VFS; content is also available as a line-addressable **document IR** (read, edit lines, write back). See [Virtual filesystem (`vfs`)](docs/vfs.md).

### Cloud-native performance and operability

Agents belong in infrastructure: durable state, horizontal scale, inspectable runs. Tacklr is built in Go with **checkpointed sessions**, pluggable stores, and a harness that does not assume a single process or a single client. The same core backs interactive products or fleets of workers without rewriting the loop—and without the ad-hoc process sprawl that tanks performance in looser stacks.

### Determinism where it matters

Language models are probabilistic; the harness around them does not have to be. Tacklr separates model I/O from the turn loop, uses explicit tool results and plan effects, and persists **checkpoints** (conversation, plan, interrupts, knowledge scope) so runs resume instead of improvising after a crash. Clear architecture makes behavior testable, observable, and operable.

### Knowledge that stays alive

Static RAG dumps age in the window. Tacklr’s optional **brain** is a host-owned knowledge engine: hybrid and graph-aware retrieval, dual-store design, and tools that appear only when an engine is wired. Memory is queried when needed—not frozen forever as transcript sludge.

---

## What you can build

With the harness as the center of gravity:

- **Operators** that plan multi-step work, complete todos, and hand off cleanly between stages  
- **Product agents** with typed tools, user interrupts (ask and wait), cancel, and resume  
- **Long-lived sessions** restored from checkpoints after restart or redeploy  
- **Multi-agent systems** via a registry of agent specs sharing infrastructure  
- **Knowledge-backed agents** when you attach a brain engine (search, expand, save structured memory)  
- **Observable systems** with optional OpenTelemetry on turns, tools, and retrieval  

You bring the model endpoint, business tools, and hosting. Tacklr owns the loop structure.

---

## How the framework is shaped

```text
  Model (OpenAI-compatible inference strategy)
            │
            ▼
     ┌──────────────┐
     │   harness    │  turns · tools · plan · handoff · checkpoints
     │   (tacklr)   │
     └──────┬───────┘
            ▼  stream events
     ┌──────────────┐
     │  host layer  │  your product, workers, or protocol adapters
     └──────────────┘
```

| Piece | Role |
|-------|------|
| **Inference strategy** | Talks to the model only; parses the stream |
| **Harness** | Owns a turn: tools, plan builtins, context, save/load |
| **Store** | Persists checkpoints across process boundaries |
| **Brain (optional)** | Knowledge engine and related tools |
| **Registry (optional)** | Multi-agent hosting and protocol-facing wiring |

A **turn** is one prompt (or resume after interrupt) until completion, error, cancel, or a deliberate wait for the user. Planning follows a simple lifecycle:

```text
create_plan → execute with tools → complete_todo → handoff → next work
```

---

## Quick start

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
	model.WithURL(os.Getenv("OPENAI_BASE_URL")).
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

### Tools

Tools are ordinary Go functions. Optional `HarnessRuntime` supports host state, progress updates, and interrupts—not direct mutation of the plan store.

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

### Sessions

```go
agent := tacklr.NewAgent(ctx, opts)
agent, err := tacklr.NewAgentFromSession(ctx, sessionID, opts)
```

Checkpoints include the conversation window, plan, tool and user state, and pending interrupts. Use an in-memory store for ephemeral runs or Postgres for durable ones.

### Host tools vs plan system

| | Your tools | Plan builtins |
|--|------------|----------------|
| API | `HarnessRuntime` | Internal session (not exposed on host tools) |
| May | State, interrupts, progress | Plan document and todos |
| May not | Rewrite the plan store | — |

That boundary keeps product logic from accidentally breaking planning.

---

## Capabilities you can wire in

- **Planning** — `create_plan`, `list_plan`, `edit_plan`, `complete_todo` with install and handoff effects  
- **Interrupts** — pause a tool for structured user input, then resume the turn  
- **MCP** — attach external tool servers via config  
- **Skills** — load `SKILL.md` catalogs (directories or object storage loaders)  
- **Web** — optional search/fetch when an Exa API key is configured  
- **Brain** — knowledge tools when `AgentOptions.Brain` is set  

### Knowledge (brain)

Not “paste the last N chunks into context.” The brain is a host-owned engine:

- **Postgres** as source of truth for objects, hybrid search, filters, soft-delete  
- **Optional graph backend** for entities and links  
- **Namespaces** for isolation  
- Tools such as `search`, `read`, `expand`, `find_objects`, and `find_links` when capabilities allow  

See package docs: [`brain`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/brain).

### Observability

Optional OpenTelemetry on turns, tools, and retrieval. Bring your own collector or inject tracer/meter providers. With nothing configured, telemetry is a no-op.

---

## Packages

| Package | Role |
|---------|------|
| `tacklr` | Harness, tools, plan loop, subagents |
| `vfs` | Virtual filesystem, mounts, content IR ([docs](docs/vfs.md)) |
| `vfsindex` | Optional mount → brain ingest bridge (VFS and brain stay independent) |
| `brain` | Knowledge engine |
| `brain/helixgraph` | Optional graph adapter |
| `inference` | OpenAI-compatible model client |
| `server` | Multi-agent registry and protocol adapters |
| `stores` | Session checkpoints |
| `interrupt` | Pause/resume types |
| `streaming` | Shared messages and events |
| `mcp` | MCP config types |
| `skills` | Skill loading |
| `telemetry` | OTEL helpers |

---

## Roadmap: agent operating system

Tacklr’s long-term direction is an **agent OS**: a closed virtual world (filesystem + execution + policy) so agents stay efficient, scoped, and operable. Determinism here means **the world the agent can touch is explicit, mediated, and replayable**—not that the model always samples the same tokens.

**Principles we bake in:**

| Principle | Meaning |
|-----------|---------|
| **Closed world** | Only mounted virtual paths exist; backends stay opaque |
| **Single content truth** | Document IR is the agent-facing file; codecs own storage format on flush |
| **Mediated ops** | Shell, scripts, and tools act through policy—not ambient host authority |
| **Stable edits** | Content `rev` (hash) for optimistic concurrency on writes |
| **Few doors** | One edit surface across file types and mounts; discovery can use path tools or a restricted shell |
| **Checkpointed truth** | Sync IR, then persist session state—not mystery process memory |
| **Observe then enforce** | Gates on operations *during* a tool call, not only the tool name at the start |

### Feature status

| Area | Status | Notes |
|------|--------|--------|
| Planning + handoffs (ACM) | **Shipped** | Plans, todos, context rebuild on complete |
| Checkpoints / stores | **Shipped** | Conversation, plan, interrupts; Postgres or in-memory |
| Inference strategy | **Shipped** | OpenAI-compatible stream client |
| Brain (hybrid + graph hooks) | **Shipped** | Optional; tools only when wired |
| VFS mounts (local, S3) | **Shipped** | Session `MountSession`, host-owned factories |
| Content IR (text) | **Shipped** | `TextDocument`, line IR, write-back cache |
| Dirty session overlay | **Shipped** | `Stat` / `ReadDir` / `Open` / `ReadFile` / `Remove` see write-back before Sync |
| Content `rev` + edit tools | **Shipped** | `read_lines`, `replace_lines`, `replace_text`, `write`, tree tools when VFS is set |
| Progressive line windows | **Shipped** | `ReadLines` / `LineWindow` for large text without full IR when needed |
| Mount → brain index (`vfsindex`) | **Shipped** | Optional; SyncScheduler; async scheduler later |
| Unified `read` / `replace` for all media | **Planned** | One tool family; codecs for plain + rich docs |
| Rich document IR (WYSIWYG) | **Planned** | Blocks/runs + style metadata; Word and similar via codecs |
| FUSE projection of `MountSession` | **Planned** | Host `rg` / `fd` / `ls` on session-visible tree; macOS FSKit/macFUSE, Linux FUSE |
| Custom agent shell | **Planned** | Real shell process attached to runtime; VFS-backed builtins; no raw host bash as the main path |
| Sandboxed Python / JS | **Planned** | Guests with `tacklr.fs` / `tacklr.http` only—scraping and mini-programs under policy |
| Capability broker | **Planned** | Mid-flight allow / deny / ask on `fs` · `net` · `proc` · runtime ops |
| Linux eBPF / cgroup backstop | **Eventually** | Observe + enforce outside the runtime; correlate to tool sessions |
| Materialize tree (non-FUSE) | **Eventually** | Export session view for tools without FUSE where useful |

Local **testserver** mounts a local jail at `/tmp/tacklr` when VFS is enabled so agents can exercise mounts and file tools end-to-end.

### Target architecture

How the pieces fit when the agent OS is fully formed. Today’s harness already covers the top of the diagram (turns, plan, tools, VFS IR); lower layers are the roadmap above.

```mermaid
flowchart TB
  subgraph harness [Agent harness]
    Plan[Plan / todos / handoff]
    Tools[Hard tools: read · replace · write]
    Ctx[Structured context]
    CP[Checkpoint + SyncAll]
  end

  subgraph runtime [Agent runtime]
    Shell[Custom agent shell]
    Py[Python guest]
    JS[JS guest]
    Broker[Capability broker<br/>allow · deny · ask · limit]
  end

  subgraph vfs_layer [Virtual filesystem]
    MS[MountSession]
    IR[Content IR<br/>text lines · rich blocks + styles]
    Overlay[Dirty write-back overlay]
    Codec[Codecs bytes ↔ IR]
  end

  subgraph project [Host projection optional]
    FUSE[FUSE / FSKit mount]
    HostCLIs[rg · fd · ls · find]
  end

  subgraph backends [Opaque backends]
    Local[Local jail]
    S3[S3 / object]
    Other[Drive / Docs / …]
  end

  subgraph kernel [Linux backstop eventually]
    eBPF[eBPF / cgroup / seccomp]
  end

  Plan --> Tools
  Plan --> Ctx
  Tools --> Broker
  Shell --> Broker
  Py --> Broker
  JS --> Broker
  Broker --> MS
  MS --> Overlay
  Overlay --> IR
  IR --> Codec
  Codec --> Local
  Codec --> S3
  Codec --> Other
  MS --> FUSE
  FUSE --> HostCLIs
  HostCLIs -.->|allowlisted exec| Broker
  CP --> MS
  eBPF -.->|observe / enforce| Broker
  eBPF -.->|jail| backends
```

### Deterministic control loop (during a tool call)

```mermaid
sequenceDiagram
  participant Agent
  participant Harness
  participant Broker as Capability broker
  participant VFS as MountSession + IR
  participant Backend

  Agent->>Harness: tool / shell / python session
  Harness->>Broker: session open
  loop Each privileged op
    Agent->>Broker: fs.read / net.http / proc.exec / …
    Broker->>Broker: policy allow · deny · ask
    alt allowed
      Broker->>VFS: session-visible I/O
      VFS->>Backend: on Sync / write-through only
      Broker-->>Agent: result
    else deny or human gate
      Broker-->>Harness: block / interrupt
    end
  end
  Harness->>VFS: SyncAll on checkpoint
  VFS->>Backend: codecs encode durable bytes
```

### Intended agent-facing surface (target)

| Role | Mechanism |
|------|-----------|
| Discover paths / search text | Restricted shell and/or `rg`·`fd` on FUSE; optional `list` / `stat` |
| Read IR (+ `rev`) | Single **`read`** (lines or rich document JSON) |
| Edit IR | **`replace`** / **`write`** with `rev` (plain + WYSIWYG) |
| Tree mutate | Shell under policy, or thin tools if no shell |
| Scripts / scrape | Sandboxed **python** / **js** with runtime APIs only |
| Knowledge | Brain tools when an engine is attached |

Deep VFS detail: [docs/vfs.md](docs/vfs.md).

---

## Develop

```bash
make test
make vet
```

Contribution rules: [`AGENTS.md`](AGENTS.md).

---

## License

See [LICENSE](LICENSE).
