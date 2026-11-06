# Tacklr on Temporal

This example runs Tacklr as it is meant to run in production: two programs, each scaled on its own, with the session state in Postgres.

```
editor ──ACP over WebSocket──► host ──start · signal · query──► Temporal ──steps──► worker ──► model
                                 │ ◄──────── live deltas (HTTP) ─────────────────────── │
                                 └──────────────► Postgres ◄─────────────────────────────┘
                                      sessions · sealed credentials · events · ACP wire
```

- **The host** (`cmd/host`) serves ACP at `/acp` behind one bearer token. It is a `session.Client` (package `session/temporal`) under the protocol server (`server`, `server/acp`), set up in package [`examples/host`](../host), which the Cloudflare example serves too. It keeps no session state, so you can run as many as you need behind a load balancer. It does not link the agent, the workspace or the worker. A turn belongs to its session, not to the editor's connection: an editor that disconnects and loads the session again follows the turn, and `session/cancel` cancels it. [docs/protocol.md](../../docs/protocol.md) follows a turn through the host.
- **The worker** (`cmd/worker`) runs the sessions: the session workflow, each model request and tool call as an activity, the agent (package `agent`) and each session's workspace. It serves a live endpoint, where a host streams the deltas of a model call while it runs. Deltas are never stored. Run as many workers as you need: a session's steps go to the worker that holds it, and any other worker continues it from Postgres.
- **Postgres** holds the checkpoints, the tool-call results, the users' credentials (sealed with `SEAL_KEY`), the session events, and the ACP wire state (each session's cwd and owner), so any host can load any session.

## Files

| Path | What it is |
|---|---|
| `agent/` | The agent: a `lookup` tool, a `researcher` specialist, MCP servers that the client names, and the workspace. Each session gets its own directory, `/workspace/work`, under `WORKSPACE_DIR` (`vfs.SessionLocal`). A specialist gets its own directory too. |
| `../host/` | The ACP server with bearer authentication (package `examples/host`). The Cloudflare example serves the same server. |
| `cmd/internal/stack/` | What both programs read from the environment and open: Temporal, the Postgres stores, the model client and the live key. |
| `cmd/host/`, `cmd/worker/` | The two programs. |
| `example_test.go` | The end-to-end test. |

## Run it

You need Linux, Go 1.27, Docker and the [Temporal CLI](https://docs.temporal.io/cli).

```sh
temporal server start-dev &
docker run -d --name tacklr-pg -e POSTGRES_PASSWORD=tacklr -p 5432:5432 pgvector/pgvector:0.8.7-pg17

export DATABASE_URL=postgres://postgres:tacklr@localhost:5432/postgres
export SEAL_KEY=$(openssl rand -base64 32) LIVE_KEY=$(openssl rand -base64 32)
export OPENAI_API_KEY=... MODEL=gpt-5-mini

WORKSPACE_DIR=$PWD/work go run ./examples/temporal/cmd/worker &
API_TOKEN=dev-token go run ./examples/temporal/cmd/host
```

Point an ACP client at `ws://localhost:8080/acp` with the header `Authorization: Bearer dev-token`. Ask "What is the status of v2?" (the agent calls `lookup`), then "Write a note about it" (the agent makes a plan, because write tools wait for one, and its write asks you first).

| Variable | Program | Meaning |
|---|---|---|
| `DATABASE_URL` | both | Postgres. The programs create their tables on start. |
| `SEAL_KEY` | both | 32 bytes in base64. It seals the credentials users send. |
| `LIVE_KEY` | both | base64. It signs the hosts' requests for live deltas. |
| `OPENAI_API_KEY`, `MODEL`, `OPENAI_BASE_URL` | both | The model. The host uses it only to refuse content the model cannot read. |
| `TEMPORAL_ADDRESS`, `TASK_QUEUE` | both | Default `localhost:7233` and `tacklr`. |
| `API_TOKEN`, `ADDR` | host | The bearer token, and the address (default `:8080`). |
| `WORKSPACE_DIR` | worker | The directory that holds each session's `/workspace/work`. |
| `LIVE_URL`, `LIVE_ADDR` | worker | How hosts reach the live endpoint (default `http://localhost:8081`), and where it listens (default `:8081`). |
| `FUSE_ROOT` | worker | Optional. A directory for FUSE. It gives the agent `run_command`, which runs in a sandbox that sees only its session's workspace. |
| `MAX_WINDOW` | worker | Optional. The context window in tokens. The default is the model's. |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` | both | Optional. Where traces, metrics and logs go (OTLP over gRPC by default, or `http`). |

Both programs send their telemetry to `OTEL_EXPORTER_OTLP_ENDPOINT` when it is set (for example a local Grafana LGTM stack): one trace per turn, from the client's request through the host, Temporal, the worker's steps, the model, the MCP servers and Postgres. Their logs go to stderr and, with the trace and span of each record, to the same endpoint. See [docs/telemetry.md](../../docs/telemetry.md).

## The end-to-end test

`go test ./examples/temporal` runs the worker and the host as the programs wire them, on Temporal and Postgres in Docker (the same containers as the repository's other live tests), with a scripted model behind a real HTTP endpoint. An ACP client on a WebSocket tests these outcomes:

1. A question runs the agent's tool on the worker, and the answer streams back through the host.
2. A note: the agent makes a plan, its write asks the operator, and once the operator allows it, the note is in the session's own directory under `WORKSPACE_DIR`.
3. A cancel: the operator cancels the turn while the write asks, and answers the ask with `cancelled`. The prompt ends with `stopReason` `cancelled`, and the session answers the next question.
4. A wrong token gets 401 at the upgrade, and a connection with no token cannot open a session.
