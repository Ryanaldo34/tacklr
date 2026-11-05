# Tacklr on Cloudflare Workers

This example Worker runs each Tacklr session as a SQLite-backed Durable Object (package `session/durableobject/cloudflare`). Its front door is a small HTTP API behind one bearer token (package `api`). The API is the example's own; ACP is the protocol for real clients.

## Files

- `main.go`: the Go program (`GOOS=js GOARCH=wasm`). It calls `cloudflare.Serve` with:
  - the agent, with an OpenAI model from the env;
  - three tools: `lookup`, `report` (a 3 MB result) and `deploy` (asks for permission);
  - a `researcher` specialist.
- `api/`: the HTTP API, in plain Go and tested on Linux.
- `worker/`: the Worker:
  - `build.sh` builds the program into `build/`.
  - `src/index.js` is the Worker entry. It imports `shim.js` from the bridge package.
  - `wrangler.jsonc` is the config.
  - `e2e/` is the end-to-end test.

## Run it

You need Go 1.27, Bun and Node 22.

```sh
cd examples/cloudflare/worker
bun install
cp .dev.vars.example .dev.vars   # set API_TOKEN, SEAL_KEY (openssl rand -base64 32), OPENAI_API_KEY
bun run dev
```

Then, with `TOKEN` set to your `API_TOKEN`:

```sh
curl -s -X POST localhost:8787/sessions -H "authorization: Bearer $TOKEN"     # {"id":"..."}
curl -s -N "localhost:8787/sessions/$ID/events?after=0" -H "authorization: Bearer $TOKEN" &
curl -s -X POST localhost:8787/sessions/$ID/prompt -H "authorization: Bearer $TOKEN" -d '{"text":"hello"}'
```

To deploy, set the same names as secrets (`wrangler secret put API_TOKEN`, and the same for the others), then run `bun run deploy`. The `limits.cpu_ms` setting needs Workers Paid.

## The API

| Request | Answer |
|---|---|
| `POST /sessions` with `{"id"?, "specialist"?}` | 201 `{"id"}` |
| `POST /sessions/{id}/prompt` with `{"text"}` | 202 |
| `POST /sessions/{id}/resume` with `{"responses": {"<interrupt id>": <answer>}}` | 202 |
| `POST /sessions/{id}/cancel` | 202 |
| `DELETE /sessions/{id}` | 204 |
| `GET /sessions/{id}` | the status |
| `GET /sessions/{id}/jobs` | its jobs' statuses (a specialist is a job) |
| `GET /sessions/{id}/events?after=N` | NDJSON events: deltas live, then each stored event |

A child session's id has slashes, so escape it in a URL.

## The end-to-end test

`make cloudflare-e2e` (or `bun test e2e` in `worker/`) does these steps:
1. It builds the program.
2. It starts a scripted model (`e2e/model.ts`).
3. It runs the Worker under `wrangler dev` (workerd), with its state in a temporary directory.
4. It tests these outcomes:
   - deltas stream live, and the log keeps the whole message;
   - a 3 MB tool result reaches the model whole (its stored rows are split into parts);
   - a specialist parks in its own object and is resumed by its id;
   - a parked session resumes after `wrangler dev` restarts (an eviction);
   - a bad token gets 401.

The make target then checks the program's size against the 24 MiB budget (`scripts/wasm-size.sh`).

## Not measured yet

These need a real deploy. Local workerd does not apply them:
- The subrequest limit (50 for each invocation on the Free plan). It is not known how Workers counts a session's model requests after the request that started them returned.
- Memory in production (128 MB for each isolate).
- Cold start.
