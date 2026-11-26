# Tacklr on Temporal

This example runs Tacklr as it is meant to run in production: two programs, each scaled on its own, with the session state in Postgres.

```
your program ──HTTP · SSE──► backend ──start · signal · query──► Temporal ──steps──► worker ──► model
                               │ ◄──────── live deltas (HTTP) ──────────────────────── │
                               └──────────────► Postgres ◄──────────────────────────────┘
                                    sessions · sealed credentials · events
```

- **The backend** (`cmd/backend`) serves the small HTTP API of package [`examples/backend`](../backend) behind one bearer token: create a session, prompt, resume, cancel, and follow its events as server-sent events. It is a `session.Client` (package `session/temporal`) under that API; your own backend has your own routes and users over the same client. It keeps no session state, so you can run as many as you need behind a load balancer, and it does not link the agent, the workspace or the worker. A turn belongs to its session, not to a request: a program that stops following the events and follows them again sees the turn go on.
- **The worker** (`cmd/worker`) runs the sessions: the session workflow, each model request and tool call as an activity, the agent (package `agent`) and each session's workspace. It serves a live endpoint, where a backend streams the deltas of a model call while it runs. Deltas are never stored. Run as many workers as you need: a session's steps go to the worker that holds it, and any other worker continues it from Postgres.
- **Postgres** holds the checkpoints, the tool-call results, the users' credentials (sealed with `SEAL_KEY`) and the session events, so any backend can serve any session.

## Files

| Path | What it is |
|---|---|
| `agent/` | The agent: a `lookup` tool, a `researcher` specialist, MCP servers that a session's creator names, and the workspace tree (`agent.Tree`). Each session gets its own directory, `/workspace/work`, under the supervisor's `WORKSPACE_DIR` (`vfs.SessionLocal`). A specialist gets its own directory too. |
| `../backend/` | The HTTP API with bearer authentication (package `examples/backend`). The Cloudflare example serves the same one. |
| `cmd/internal/stack/` | What both programs read from the environment and open: Temporal, the Postgres stores and the live key, and the worker's model client. |
| `cmd/backend/`, `cmd/worker/` | The two programs. |
| `cmd/supervisor/` | Optional. The sandbox supervisor, for a sandbox host: the agent's `run_command` runs there in Firecracker microVMs, over the same tree (`agent.Tree`). |
| `example_test.go` | The end-to-end test. |

## Run it

You need Linux, Go 1.27, Docker and the [Temporal CLI](https://docs.temporal.io/cli).

```sh
temporal server start-dev &
docker run -d --name tacklr-pg -e POSTGRES_PASSWORD=tacklr -p 5432:5432 pgvector/pgvector:0.8.7-pg17

export DATABASE_URL=postgres://postgres:tacklr@localhost:5432/postgres
export SEAL_KEY=$(openssl rand -base64 32) LIVE_KEY=$(openssl rand -base64 32)
export ANTHROPIC_API_KEY=... MODEL=claude-opus-5-5   # or OPENAI_API_KEY=... MODEL=gpt-5-mini

go run ./examples/temporal/cmd/worker &
API_TOKEN=dev-token go run ./examples/temporal/cmd/backend
```

Drive a session with curl:

```sh
auth='Authorization: Bearer dev-token'
id=$(curl -s -H "$auth" -X POST localhost:8080/sessions | jq -r .id)
curl -sN -H "$auth" localhost:8080/sessions/$id/events &          # follow its events
curl -s -H "$auth" -d '{"text":"What is the status of v2?"}' localhost:8080/sessions/$id/prompt
```

The agent calls `lookup` and answers. Ask it to "Write a note about it": the agent makes a plan, and its `run_command` waits for permission (a `yield` event); answer with `POST /sessions/$id/resume` and `{"responses":{"<the event's MessageID>":{"optionId":"allow-once"}}}`. `run_command` needs the supervisor (`cmd/supervisor`) and the `SANDBOX_*` settings below.

| Variable | Program | Meaning |
|---|---|---|
| `DATABASE_URL` | both | Postgres. The programs create their tables on start. |
| `SEAL_KEY` | both | 32 bytes in base64. It seals the credentials users send. |
| `LIVE_KEY` | both | base64. It signs the backends' requests for live deltas. |
| `MODEL`, and `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` | worker | The model: a Claude model with `ANTHROPIC_API_KEY`, else an OpenAI-compatible one. `ANTHROPIC_BASE_URL` and `OPENAI_BASE_URL` set the endpoint. |
| `TEMPORAL_ADDRESS`, `TASK_QUEUE` | both | Default `localhost:7233` and `tacklr`. |
| `API_TOKEN`, `ADDR` | backend | The bearer token, and the address (default `:8080`). |
| `LIVE_URL`, `LIVE_ADDR` | worker | How backends reach the live endpoint (default `http://localhost:8081`), and where it listens (default `:8081`). |
| `MAX_WINDOW` | worker | Optional. The context window in tokens. The default is the model's. |
| `SANDBOX_ADDR`, `SANDBOX_CERT`, `SANDBOX_KEY`, `SANDBOX_CA` | worker | Optional. The sandbox supervisor's address, and PEM files of the worker's client certificate, its key and the supervisor's roots. They give the agent `run_command`. |
| `SUPERVISOR_ADDR`, `SUPERVISOR_CERT`, `SUPERVISOR_KEY`, `SUPERVISOR_CLIENT_CA` | supervisor | Where it serves mTLS (default `:7443`), its certificate and key, and the roots that verify the workers. |
| `FIRECRACKER`, `GUEST_KERNEL`, `GUEST_ROOTFS`, `WORKSPACE_DIR` | supervisor | Firecracker, the guest kernel and the root file system (`make sandbox-image`), and the directory of each session's `/workspace/work`. The sandbox host needs `/dev/kvm`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` | both | Optional. Where traces, metrics and logs go (OTLP over gRPC by default, or `http`). |

Both programs send their telemetry to `OTEL_EXPORTER_OTLP_ENDPOINT` when it is set (for example a local Grafana LGTM stack): one trace per turn, from the caller's request (its `traceparent`) through the backend, Temporal, the worker's steps, the model, the MCP servers and Postgres. Their logs go to stderr and, with the trace and span of each record, to the same endpoint. See [docs/telemetry.md](../../docs/telemetry.md).

## The end-to-end test

`go test ./examples/temporal` runs the worker and the backend as the programs wire them, on Temporal and Postgres in Docker (the same containers as the repository's other live tests), with a scripted model behind a real HTTP endpoint and real sandbox microVMs. A program that drives a session through the backend tests these outcomes:

1. A question runs the agent's tool on the worker, and the answer streams back through the backend.
2. A note: the agent makes a plan, its `run_command` waits for permission, and once the operator allows it, the sandbox writes the note into the session's own directory under `WORKSPACE_DIR`.
3. A cancel: the operator cancels the turn while the write waits. The turn ends with no note, and the session answers the next question.

`examples/backend`'s own tests cover its routes and refusals, such as a request without the token.
