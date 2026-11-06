// End-to-end test of the Cloudflare bridge: the example Worker runs in
// workerd (wrangler dev) against a scripted model, and the test is an ACP
// client on a WebSocket, as an editor is. A restart of wrangler dev is an
// eviction: only each object's SQLite storage survives it, and the
// client's socket closes.
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

type Frame = { id?: number; method?: string; params?: any; result?: any; error?: { code: number; message: string } };

// Client is an ACP client on one WebSocket: it reads the frames in order.
class Client {
  private queue: Frame[] = [];
  private wake?: () => void;
  private next = 0;
  closed = false;

  private constructor(private ws: WebSocket) {
    ws.addEventListener("message", (e) => {
      this.queue.push(JSON.parse(String(e.data)));
      this.wake?.();
    });
    ws.addEventListener("close", () => {
      this.closed = true;
      this.wake?.();
    });
  }

  // open connects with bearer (none when it is empty) and says hello.
  static async open(bearer = token): Promise<Client> {
    const headers: Record<string, string> = bearer ? { authorization: `Bearer ${bearer}` } : {};
    const ws = new WebSocket(`ws://localhost:${port}/acp`, { headers } as any);
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => resolve(), { once: true });
      ws.addEventListener("error", () => reject(new Error("the upgrade was refused")), { once: true });
    });
    const c = new Client(ws);
    await c.call("initialize", { protocolVersion: 1 });
    return c;
  }

  async frame(): Promise<Frame> {
    for (let i = 0; i < 600 && this.queue.length === 0; i++) {
      if (this.closed) throw new Error("the socket closed");
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        setTimeout(resolve, 100);
      });
    }
    const f = this.queue.shift();
    if (!f) throw new Error("no frame in 60 seconds");
    return f;
  }

  send(frame: unknown) {
    this.ws.send(JSON.stringify(frame));
  }

  // request sends method and returns its id.
  request(method: string, params: unknown): number {
    this.send({ jsonrpc: "2.0", id: ++this.next, method, params });
    return this.next;
  }

  // until reads frames until the first that done accepts, and returns it
  // and the frames before it.
  async until(done: (f: Frame) => boolean): Promise<{ last: Frame; before: Frame[] }> {
    const before: Frame[] = [];
    for (;;) {
      const f = await this.frame();
      if (done(f)) return { last: f, before };
      before.push(f);
    }
  }

  async call(method: string, params: unknown) {
    const id = this.request(method, params);
    return (await this.until((f) => f.id === id && !f.method)).last;
  }

  async newSession(): Promise<string> {
    const r = await this.call("session/new", { cwd: "/edge" });
    expect(r.error).toBeUndefined();
    return r.result.sessionId;
  }

  prompt(sid: string, text: string): number {
    return this.request("session/prompt", { sessionId: sid, prompt: [{ type: "text", text }] });
  }

  close() {
    this.ws.close();
  }
}

// updates is the session/update payloads among frames.
const updates = (frames: Frame[]) => frames.filter((f) => f.method === "session/update").map((f) => f.params.update);
const text = (frames: Frame[]) =>
  updates(frames)
    .filter((u) => u.sessionUpdate === "agent_message_chunk")
    .map((u) => u.content.text);
const output = (frames: Frame[], status = "completed") =>
  updates(frames).find((u) => u.sessionUpdate === "tool_call_update" && u.status === status)?.rawOutput?.output;

const allow = { outcome: { outcome: "selected", optionId: "allow-once" } };

test("a prompt streams its answer in chunks as the model writes it", async () => {
  const c = await Client.open();
  const sid = await c.newSession();
  const id = c.prompt(sid, "hello");
  const { last, before } = await c.until((f) => f.id === id);
  c.close();

  expect(last.result?.stopReason).toBe("end_turn");
  expect(text(before).length).toBeGreaterThan(1);
  expect(text(before).join("")).toBe("Hello from the edge");
}, 60_000);

test("a tool's 3 MB result reaches the model and the client whole", async () => {
  const c = await Client.open();
  const id = c.prompt(await c.newSession(), "report");
  const { last, before } = await c.until((f) => f.id === id);
  c.close();

  expect(last.result?.stopReason).toBe("end_turn");
  expect(output(before)?.length).toBe(3 << 20);
  expect(model.outputs).toContain(3 << 20);
}, 60_000);

test("a turn is one trace from the client's traceparent, through the session's and the specialist's objects", async () => {
  const c = await Client.open();
  const sid = await c.newSession();
  const traceId = crypto.getRandomValues(new Uint8Array(16)).reduce((s, b) => s + b.toString(16).padStart(2, "0"), "");
  const clientSpan = "00f067aa0ba902b7";
  const id = c.request("session/prompt", {
    sessionId: sid,
    prompt: [{ type: "text", text: "research" }],
    _meta: { traceparent: `00-${traceId}-${clientSpan}-01` },
  });
  const { last } = await c.until((f) => f.id === id && !f.method);
  c.close();
  const want = ["session/prompt", "invoke_agent edge", "execute_tool spawn_specialist", "invoke_agent researcher", "execute_tool lookup"];
  let spans = model.spans.filter((s) => s.traceId === traceId);
  for (let i = 0; i < 100 && !want.every((n) => spans.some((s) => s.name === n)); i++) {
    await Bun.sleep(100);
    spans = model.spans.filter((s) => s.traceId === traceId);
  }

  expect(last.result?.stopReason).toBe("end_turn");
  expect(spans.map((s) => s.name)).toEqual(expect.arrayContaining([...want, expect.stringMatching(/^chat /)]));
  const ids = new Set(spans.map((s) => s.spanId));
  expect(spans.filter((s) => !ids.has(s.parentSpanId ?? "") && s.parentSpanId !== clientSpan)).toEqual([]);
}, 60_000);

test("a specialist runs in its own object and its result reaches the parent's call", async () => {
  const c = await Client.open();
  const id = c.prompt(await c.newSession(), "research");
  const { last, before } = await c.until((f) => f.id === id);
  c.close();

  expect(last.result?.stopReason).toBe("end_turn");
  expect(output(before)).toBe("done: 42");
}, 60_000);

test("a turn parked on a permission asks again after an eviction, and finishes once allowed", async () => {
  const first = await Client.open();
  const sid = await first.newSession();
  first.prompt(sid, "deploy");
  await first.until((f) => f.method === "session/request_permission");
  await stop();
  await start();

  const second = await Client.open();
  const loaded = second.request("session/load", { sessionId: sid, cwd: "/edge" });
  const asked = await second.until((f) => f.method === "session/request_permission");
  second.send({ jsonrpc: "2.0", id: asked.last.id, result: allow });
  const { before } = await second.until((f) => text([f]).includes("done: deployed"));
  second.close();

  expect([...asked.before, ...before].some((f) => f.id === loaded && f.result?.sessionId === sid)).toBe(true);
  expect(asked.last.params.sessionId).toBe(sid);
  expect(output(before)).toBe("deployed");
}, 120_000);

test("a cancel while the agent asks for permission ends the prompt cancelled", async () => {
  const c = await Client.open();
  const sid = await c.newSession();
  const id = c.prompt(sid, "deploy");
  const asked = await c.until((f) => f.method === "session/request_permission");
  c.send({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: sid } });
  c.send({ jsonrpc: "2.0", id: asked.last.id, result: { outcome: { outcome: "cancelled" } } });
  const { last } = await c.until((f) => f.id === id && !f.method);
  c.close();

  expect(last.result?.stopReason).toBe("cancelled");
}, 60_000);

test("the Worker refuses a client without the token", async () => {
  const wrong = Client.open("wrong").then(
    () => "opened",
    (err: Error) => err.message,
  );
  const anonymous = await Client.open("");
  const refused = await anonymous.call("session/new", { cwd: "/edge" });
  anonymous.close();
  const plain = await fetch(`${base}/acp`);

  expect(await wrong).toContain("refused");
  expect(refused.error?.message).toBe("authentication required");
  expect(plain.status).toBe(426);
});
