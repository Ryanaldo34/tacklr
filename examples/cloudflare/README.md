# Tacklr on Cloudflare Workers

This example Worker runs each Tacklr session as a SQLite-backed Durable Object, and serves the small HTTP API of [`examples/backend`](../backend) over them, behind one bearer token (package `session/durableobject/cloudflare`). It is the same backend the [Temporal example](../temporal) runs: create a session, prompt, resume, cancel, and follow its events as server-sent events.

```
your program ──HTTP · SSE──► Worker (backend) ──object protocol──► session object ──► model
                                                                   (one per session: its journal
                                                                    and events in SQLite)
```

## Files

- `main.go`: the Go program (`GOOS=js GOARCH=wasm`). It calls `cloudflare.Serve` with:
  - the agent, with a model from the env (Claude with `ANTHROPIC_API_KEY`, else OpenAI-compatible);
  - three tools: `lookup`, `report` (a 3 MB result) and `deploy` (asks for permission);
  - a `researcher` specialist, which runs in its own object;
  - the backend (`backend.New`) as its `Handler`, over the session objects' `durableobject.Client`.
- `worker/`: the Worker:
  - `build.sh` builds the program into `build/`.
  - `src/index.js` is the Worker entry. It imports `shim.js` from the bridge package and exports the `Session` class.
  - `wrangler.jsonc` is the config: the `SESSIONS` binding.
  - `e2e/` is the end-to-end test.

## Run it

You need Go 1.27, Bun and Node 22.

```sh
cd examples/cloudflare/worker
bun install
cp .dev.vars.example .dev.vars   # set API_TOKEN, SEAL_KEY (openssl rand -base64 32), MODEL, and a model key
bun run dev
```

Drive a session with curl, as in the [Temporal example](../temporal#run-it), at `localhost:8787` with the header `Authorization: Bearer $API_TOKEN`. Ask it to deploy: the agent calls `deploy`, which waits for your permission.

To deploy, set the same names as secrets (`wrangler secret put API_TOKEN`, and the same for the others), then run `bun run deploy`. The `limits.cpu_ms` setting needs Workers Paid.

A deploy closes the event streams. A turn belongs to its session, not to a request: a program that follows the events again (from `?after=0`, or from now) sees the session's turn, and a turn parked on a permission still waits for its answer. `POST /sessions/{id}/cancel` cancels the turn.

## The end-to-end test

`make cloudflare-e2e` (or `bun test e2e` in `worker/`) does these steps:
1. It builds the program.
2. It starts a scripted model (`e2e/model.ts`).
3. It runs the Worker under `wrangler dev` (workerd), with its state in a temporary directory.
4. As a program that drives sessions through the backend, it tests these outcomes:
   - a prompt streams its answer in deltas as the model writes it;
   - a 3 MB tool result reaches the model and the program whole (its stored rows are split into parts);
   - a turn is one trace from the program's `traceparent`, through the session's and the specialist's objects (the fake model server is the OTLP endpoint too);
   - a specialist runs in its own object, and its result reaches the parent's call;
   - a turn parked on a permission is still parked after `wrangler dev` restarts (an eviction), and finishes once allowed;
   - a cancel while the agent asks for permission ends the turn;
   - a request without the token gets 401.

The make target then checks the program's size against the 24 MiB budget (`scripts/wasm-size.sh`).

## Telemetry

A turn is one trace, as on Temporal: from the caller's `traceparent`, through the Worker, the session's object and each specialist's object, to the model and the MCP servers. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (and `OTEL_EXPORTER_OTLP_HEADERS` for an API key) to send the spans there, as OTLP/HTTP in JSON. See [docs/telemetry.md](../../docs/telemetry.md#cloudflare-workers).

## Not measured yet

These need a real deploy. Local workerd does not apply them:
- The subrequest limit (50 for each invocation on the Free plan). It is not known how Workers counts a session's model requests after the request that started them returned, or the Worker's requests to the session objects.
- Memory in production (128 MB for each isolate).
- Cold start.
