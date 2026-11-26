// End-to-end test of the Cloudflare bridge: the example Worker runs in
// workerd (wrangler dev) against a scripted model, and the test is a
// program that drives sessions through the Worker's backend API
// (examples/backend), following each session's events as server-sent
// events. A restart of wrangler dev is an eviction: only each object's
// SQLite storage survives it, and the event streams close.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Subprocess } from "bun";
import { startModel } from "./model";

const root = join(import.meta.dir, "..");
const port = 8787 + Math.floor(Math.random() * 1000);
const base = `http://localhost:${port}`;
const token = "e2e-token";
const state = mkdtempSync(join(tmpdir(), "tacklr-e2e-"));
const model = startModel();
let wrangler: Subprocess | undefined;

async function start() {
  const vars = {
    API_TOKEN: token,
    SEAL_KEY: Buffer.alloc(32, 7).toString("base64"),
    OPENAI_API_KEY: "e2e",
    OPENAI_BASE_URL: model.url,
    MODEL: "gpt-5-mini",
    OTEL_EXPORTER_OTLP_ENDPOINT: model.url,
    OTEL_SERVICE_NAME: "tacklr-edge",
  };
  wrangler = Bun.spawn(
    ["bunx", "wrangler", "dev", "--port", String(port), "--persist-to", state, ...Object.entries(vars).flatMap(([k, v]) => ["--var", `${k}:${v}`])],
    { cwd: root, stdout: "inherit", stderr: "inherit" },
  );
  for (let i = 0; i < 600; i++) {
    try {
      await fetch(base);
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error("wrangler dev did not start");
}

async function stop() {
  wrangler?.kill();
  await wrangler?.exited;
}

beforeAll(async () => {
  const build = Bun.spawnSync(["./build.sh"], { cwd: root, stdout: "inherit", stderr: "inherit" });
  if (build.exitCode !== 0) throw new Error("build failed");
  await start();
}, 300_000);

afterAll(async () => {
  await stop();
  model.stop();
  rmSync(state, { recursive: true, force: true });
});

type Event = { Type: string; MessageID?: string; Content?: string; delta?: boolean };

// Session is one session of the backend, and the events it follows.
class Session {
  private queue: Event[] = [];
  private wake?: () => void;
  private reader?: ReadableStreamDefaultReader<Uint8Array>;
  ended = false;

  private constructor(readonly id: string) {}

  // create makes a session and follows its events.
  static async create(): Promise<Session> {
    const r = await api("POST", "/sessions");
    expect(r.status).toBe(201);
    const s = new Session((await r.json()).id);
    await s.follow();
    return s;
  }

  // follow reads the session's events after after (from now when it is
  // undefined).
  async follow(after?: number) {
    const query = after === undefined ? "" : `?after=${after}`;
    const r = await api("GET", `/sessions/${this.id}/events${query}`);
    expect(r.status).toBe(200);
    this.reader = r.body!.getReader();
    this.ended = false;
    const reader = this.reader;
    (async () => {
      const dec = new TextDecoder();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read().catch(() => ({ value: undefined, done: true }));
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let at;
        while ((at = buf.indexOf("\n\n")) >= 0) {
          const line = buf.slice(0, at);
          buf = buf.slice(at + 2);
          if (line.startsWith("data: ")) this.queue.push(JSON.parse(line.slice(6)));
        }
        this.wake?.();
      }
      this.ended = true;
      this.wake?.();
    })();
  }

  close() {
    this.reader?.cancel().catch(() => {});
  }

  async event(): Promise<Event> {
    for (let i = 0; i < 600 && this.queue.length === 0; i++) {
      if (this.ended) throw new Error("the event stream ended");
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        setTimeout(resolve, 100);
      });
    }
    const e = this.queue.shift();
    if (!e) throw new Error("no event in 60 seconds");
    return e;
  }

  // until reads events until the first that done accepts, and returns it
  // and the events before it.
  async until(done: (e: Event) => boolean): Promise<{ last: Event; before: Event[] }> {
    const before: Event[] = [];
    for (;;) {
      const e = await this.event();
      if (done(e)) return { last: e, before };
      before.push(e);
    }
  }

  // turn reads events until the turn ends or waits on the user.
  turn() {
    return this.until((e) => ["complete", "error", "yield"].includes(e.Type));
  }

  async prompt(text: string, headers: Record<string, string> = {}) {
    const r = await api("POST", `/sessions/${this.id}/prompt`, { text }, headers);
    expect(r.status).toBe(202);
  }

  async resume(messageID: string, answer: unknown) {
    const r = await api("POST", `/sessions/${this.id}/resume`, { responses: { [messageID]: answer } });
    expect(r.status).toBe(202);
  }
}

// api sends a request to the Worker with the token (bearer: another one).
function api(method: string, path: string, body?: unknown, headers: Record<string, string> = {}, bearer = token) {
  return fetch(`${base}${path}`, {
    method,
    headers: { authorization: `Bearer ${bearer}`, "content-type": "application/json", ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

const deltas = (events: Event[]) => events.filter((e) => e.Type === "message" && e.delta).map((e) => e.Content);
const said = (events: Event[]) =>
  events
    .filter((e) => e.Type === "message" && !e.delta)
    .map((e) => e.Content)
    .join("");
const result = (events: Event[]) => events.find((e) => e.Type === "tool_result")?.Content;

const allow = { optionId: "allow-once" };

test("a prompt streams its answer in deltas as the model writes it", async () => {
  const s = await Session.create();
  await s.prompt("hello");
  const { last, before } = await s.turn();
  s.close();

  expect(last.Type).toBe("complete");
  expect(deltas(before).length).toBeGreaterThan(1);
  expect(said(before)).toBe("Hello from the edge");
}, 60_000);

test("a tool's 3 MB result reaches the model and the program whole", async () => {
  const s = await Session.create();
  await s.prompt("report");
  const { last, before } = await s.turn();
  s.close();

  expect(last.Type).toBe("complete");
  expect(result(before)?.length).toBe(3 << 20);
  expect(model.outputs).toContain(3 << 20);
}, 60_000);

test("a turn is one trace from the program's traceparent, through the session's and the specialist's objects", async () => {
  const s = await Session.create();
  const traceId = crypto.getRandomValues(new Uint8Array(16)).reduce((acc, b) => acc + b.toString(16).padStart(2, "0"), "");
  const clientSpan = "00f067aa0ba902b7";
  await s.prompt("research", { traceparent: `00-${traceId}-${clientSpan}-01` });
  const { last } = await s.turn();
  s.close();
  const want = ["invoke_agent edge", "execute_tool spawn_specialist", "invoke_agent researcher", "execute_tool lookup"];
  let spans = model.spans.filter((sp) => sp.traceId === traceId);
  for (let i = 0; i < 100 && !want.every((n) => spans.some((sp) => sp.name === n)); i++) {
    await Bun.sleep(100);
    spans = model.spans.filter((sp) => sp.traceId === traceId);
  }

  expect(last.Type).toBe("complete");
  expect(spans.map((sp) => sp.name)).toEqual(expect.arrayContaining([...want, expect.stringMatching(/^chat /)]));
  const ids = new Set(spans.map((sp) => sp.spanId));
  expect(spans.filter((sp) => !ids.has(sp.parentSpanId ?? "") && sp.parentSpanId !== clientSpan)).toEqual([]);
}, 60_000);

test("a specialist runs in its own object and its result reaches the parent's call", async () => {
  const s = await Session.create();
  await s.prompt("research");
  const { last, before } = await s.turn();
  s.close();

  expect(last.Type).toBe("complete");
  expect(result(before)).toBe("done: 42");
}, 60_000);

test("a turn parked on a permission is still parked after an eviction, and finishes once allowed", async () => {
  const s = await Session.create();
  await s.prompt("deploy");
  const asked = await s.turn();
  s.close();
  await stop();
  await start();

  await s.follow(0);
  const again = await s.until((e) => e.Type === "yield");
  await s.resume(again.last.MessageID!, allow);
  const { before } = await s.until((e) => e.Type === "message" && !e.delta && e.Content === "done: deployed");
  s.close();

  expect(asked.last.Type).toBe("yield");
  expect(again.last.MessageID).toBe(asked.last.MessageID);
  expect(result(before)).toBe("deployed");
}, 120_000);

test("a cancel while the agent asks for permission ends the turn", async () => {
  const s = await Session.create();
  await s.prompt("deploy");
  await s.turn();
  const r = await api("POST", `/sessions/${s.id}/cancel`);
  const { last } = await s.turn();
  s.close();

  expect(r.status).toBe(202);
  expect(last.Type).not.toBe("yield");
}, 60_000);

test("the Worker refuses a request without the token", async () => {
  const wrong = await api("POST", "/sessions", undefined, {}, "wrong");
  const anonymous = await fetch(`${base}/sessions`, { method: "POST" });

  expect(wrong.status).toBe(401);
  expect(anonymous.status).toBe(401);
});
