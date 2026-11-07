# Tacklr on Cloudflare Workers

This example Worker runs each Tacklr session as a SQLite-backed Durable Object, and serves ACP over WebSocket, behind one bearer token, from a connection object for each socket (package `session/durableobject/cloudflare`). The ACP server is the same one the [Temporal example](../temporal)'s host runs: package [`examples/host`](../host). [docs/protocol.md](../../docs/protocol.md) follows a turn through it.

```
editor ──ACP over WebSocket──► Worker ──► connection object ──object protocol──► session object ──► model
                                          (one per socket:           (one per session: its journal,
                                           server.Connection)         events and ACP wire state in SQLite)
```

## Files

- `main.go`: the Go program (`GOOS=js GOARCH=wasm`). It calls `cloudflare.Serve` with:
  - the agent, with a model from the env (Claude with `ANTHROPIC_API_KEY`, else OpenAI-compatible);
  - three tools: `lookup`, `report` (a 3 MB result) and `deploy` (asks for permission);
  - a `researcher` specialist, which runs in its own object;
  - the ACP server (`host.New`), whose wire store is the session objects (`durableobject.Client.WireStore`).
- `worker/`: the Worker:
  - `build.sh` builds the program into `build/`.
  - `src/index.js` is the Worker entry. It imports `shim.js` from the bridge package and exports the `Session` and `Connection` classes.
  - `wrangler.jsonc` is the config: the `SESSIONS` and `CONNECTIONS` bindings.
  - `e2e/` is the end-to-end test.

## Run it

You need Go 1.27, Bun and Node 22.

```sh
cd examples/cloudflare/worker
bun install
cp .dev.vars.example .dev.vars   # set API_TOKEN, SEAL_KEY (openssl rand -base64 32), MODEL, and a model key
bun run dev
```

Point an ACP client at `ws://localhost:8787/acp` with the header `Authorization: Bearer $API_TOKEN`. Ask it to deploy: the agent calls `deploy`, which asks you first.

To deploy, set the same names as secrets (`wrangler secret put API_TOKEN`, and the same for the others), then run `bun run deploy`. The `limits.cpu_ms` setting needs Workers Paid.

A connection object uses the standard WebSocket API, so it stays in memory while its socket is open. A deploy closes the sockets. A turn belongs to its session, not to the connection: a client that connects again and loads its session follows the session's turn, and a turn parked on a permission asks again. `session/cancel`, or a `cancelled` answer to a permission, cancels the turn.

## The end-to-end test

`make cloudflare-e2e` (or `bun test e2e` in `worker/`) does these steps:
1. It builds the program.
2. It starts a scripted model (`e2e/model.ts`).
3. It runs the Worker under `wrangler dev` (workerd), with its state in a temporary directory.
4. As an ACP client on a WebSocket, it tests these outcomes:
   - a prompt streams its answer in chunks as the model writes it;
   - a 3 MB tool result reaches the model and the client whole (its stored rows are split into parts);
   - a turn is one trace from the client's `traceparent`, through the session's and the specialist's objects (the fake model server is the OTLP endpoint too);
   - a specialist runs in its own object, and its result reaches the parent's call;
   - a turn parked on a permission asks again after `wrangler dev` restarts (an eviction), and finishes once allowed;
   - a cancel while the agent asks for permission ends the prompt with `stopReason` `cancelled`;
   - a wrong token gets 401 at the upgrade, and a connection with no token cannot make a session.

The make target then checks the program's size against the 24 MiB budget (`scripts/wasm-size.sh`).

## Telemetry

A turn is one trace, as on Temporal: from the client's `traceparent`, through the connection object, the session's object and each specialist's object, to the model and the MCP servers. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (and `OTEL_EXPORTER_OTLP_HEADERS` for an API key) to send the spans there, as OTLP/HTTP in JSON. See [docs/telemetry.md](../../docs/telemetry.md#cloudflare-workers).

## Not measured yet

These need a real deploy. Local workerd does not apply them:
- The subrequest limit (50 for each invocation on the Free plan). It is not known how Workers counts a session's model requests after the request that started them returned, or a connection object's requests to the session objects.
- Memory in production (128 MB for each isolate).
- Cold start.
