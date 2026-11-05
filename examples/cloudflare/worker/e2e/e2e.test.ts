// End-to-end test of the Cloudflare bridge: the example Worker runs in
// workerd (wrangler dev) against a scripted model, and the test drives
// sessions over the example's API. A restart of wrangler dev is an
// eviction: only each object's SQLite storage survives it.
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

async function api(method: string, path: string, body?: unknown, auth = token) {
  return fetch(base + path, {
    method,
    headers: { authorization: `Bearer ${auth}`, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

type Event = { Type: string; Content?: string; MessageID?: string; delta?: boolean };

// events reads a session's events from after until one of type until, and
// returns them all.
async function events(id: string, until: string, after = 0): Promise<Event[]> {
  const res = await api("GET", `/sessions/${encodeURIComponent(id)}/events?after=${after}`);
  expect(res.status).toBe(200);
  const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
  const got: Event[] = [];
  let buf = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) throw new Error(`stream ended after ${JSON.stringify(got.map((e) => e.Type))}`);
    buf += value;
    let nl: number;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const ev = JSON.parse(buf.slice(0, nl)) as Event;
      buf = buf.slice(nl + 1);
      got.push(ev);
      if (ev.Type === until) {
        await reader.cancel();
        return got;
      }
    }
  }
}

async function session(): Promise<string> {
  const res = await api("POST", "/sessions");
  expect(res.status).toBe(201);
  return (await res.json()).id;
}

async function prompt(id: string, text: string) {
  expect((await api("POST", `/sessions/${encodeURIComponent(id)}/prompt`, { text })).status).toBe(202);
}

const allow = { optionId: "allow-once" };

test("a prompt streams its deltas live and the log keeps the whole message", async () => {
  const id = await session();
  const live = events(id, "complete");
  await prompt(id, "hello");
  const seen = await live;
  const logged = await events(id, "complete");

  const deltas = seen.filter((e) => e.delta).map((e) => e.Content).join("");
  const messages = (evs: Event[]) => evs.filter((e) => e.Type === "message" && !e.delta).map((e) => e.Content);
  expect(deltas).toBe("Hello from the edge");
  expect(messages(seen)).toEqual(["Hello from the edge"]);
  expect(logged.some((e) => e.delta)).toBe(false);
  expect(messages(logged)).toEqual(["Hello from the edge"]);
}, 60_000);

test("a tool's 3 MB result reaches the model whole and stays in the session", async () => {
  const id = await session();
  await prompt(id, "report");
  const evs = await events(id, "complete");
  const st = await (await api("GET", `/sessions/${encodeURIComponent(id)}`)).json();

  expect(evs.find((e) => e.Type === "tool_result")?.Content?.length).toBe(3 << 20);
  expect(model.outputs).toContain(3 << 20);
  expect(st.Result).toBe(`done: ${"r".repeat(40)}`);
}, 60_000);

test("a specialist parks in its own object and its result reaches the parent call", async () => {
  const id = await session();
  await prompt(id, "research");
  let child: { ID: string; Waiting: boolean } | undefined;
  for (let i = 0; i < 200 && !child?.Waiting; i++) {
    await Bun.sleep(100);
    child = (await (await api("GET", `/sessions/${encodeURIComponent(id)}/jobs`)).json())[0];
  }
  expect(child?.Waiting).toBe(true);
  const park = (await events(child!.ID, "yield")).at(-1)!;
  expect((await api("POST", `/sessions/${encodeURIComponent(child!.ID)}/resume`, { responses: { [park.MessageID!]: allow } })).status).toBe(202);
  const evs = await events(id, "complete");

  expect(evs.find((e) => e.Type === "tool_result")?.Content).toBe("done: deployed");
}, 60_000);

test("a parked session resumes after its objects were evicted", async () => {
  const id = await session();
  await prompt(id, "deploy");
  const park = (await events(id, "yield")).at(-1)!;
  await stop();
  await start();
  expect((await api("POST", `/sessions/${encodeURIComponent(id)}/resume`, { responses: { [park.MessageID!]: allow } })).status).toBe(202);
  const evs = await events(id, "complete");

  expect(evs.find((e) => e.Type === "tool_result")?.Content).toBe("deployed");
}, 120_000);

test("a request without the token is refused", async () => {
  expect((await api("GET", "/sessions/any", undefined, "wrong")).status).toBe(401);
});
