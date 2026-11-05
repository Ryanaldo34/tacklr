// The JavaScript half of package cloudflare. A host's Worker entry imports
// Go's wasm_exec.js, its compiled program and this file:
//
//   import "./wasm_exec.js";
//   import wasm from "./tacklr.wasm";
//   import { tacklr } from "./shim.js";
//
//   const app = tacklr(wasm);
//   export class Session extends app.Session {}
//   export default { fetch: app.fetch };
//
// The program starts once per isolate and serves every object in it. Each
// object keeps its storage in one SQLite table; a value larger than a row
// can hold is split into parts in the same transaction.

import { DurableObject } from "cloudflare:workers";

// PART is the size of one stored part of a value: a SQLite row of a
// Durable Object holds at most 2 MB.
const PART = 1 << 20;

const TABLE = `CREATE TABLE IF NOT EXISTS tacklr_kv (
  k TEXT NOT NULL,
  part INTEGER NOT NULL,
  v BLOB NOT NULL,
  PRIMARY KEY (k, part)
) WITHOUT ROWID`;

// instances numbers each object instance of the isolate, so the program
// can tell an object the platform reset from the one it served before.
let instances = 0;

export function tacklr(wasm) {
  let started;
  const program = () =>
    (started ??= (async () => {
      const go = new Go();
      const instance = await WebAssembly.instantiate(wasm, go.importObject);
      go.run(instance).catch((err) => console.error("tacklr: the Go program ended", err));
      if (!globalThis.__tacklr) {
        throw new Error("tacklr: the Go program did not call cloudflare.Serve");
      }
      return globalThis.__tacklr;
    })());

  class Session extends DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      ctx.storage.sql.exec(TABLE);
      this.host = new Host(ctx);
    }

    async fetch(request) {
      const go = await program();
      return toResponse(await go.object(this.host, this.env, await fromRequest(request)));
    }

    async alarm() {
      const go = await program();
      await go.alarm(this.host, this.env);
    }
  }

  return {
    Session,
    async fetch(request, env) {
      const go = await program();
      return toResponse(await go.front(env, await fromRequest(request)));
    },
  };
}

// Host is one object's storage and alarm, as the program calls them.
class Host {
  constructor(ctx) {
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.id = ctx.id.toString();
    this.instance = ++instances;
  }

  // get is the value at k, or null.
  get(k) {
    const rows = this.sql.exec("SELECT v FROM tacklr_kv WHERE k = ? ORDER BY part", k).toArray();
    return rows.length === 0 ? null : join(rows.map((r) => r.v));
  }

  // scan is the [key, value] pairs whose keys start with prefix and sort
  // after after, in key order: at most limit keys, or all when limit is 0.
  scan(prefix, after, limit) {
    let keys = "SELECT DISTINCT k FROM tacklr_kv WHERE k >= ? AND substr(k, 1, length(?)) = ?";
    const args = [prefix, prefix, prefix];
    const end = prefixEnd(prefix);
    if (end !== null) {
      keys += " AND k < ?";
      args.push(end);
    }
    if (after !== "") {
      keys += " AND k > ?";
      args.push(after);
    }
    keys += " ORDER BY k";
    if (limit > 0) {
      keys += " LIMIT ?";
      args.push(limit);
    }
    const rows = this.sql.exec(`SELECT k, v FROM tacklr_kv WHERE k IN (${keys}) ORDER BY k, part`, ...args).toArray();
    const out = [];
    for (const r of rows) {
      const last = out[out.length - 1];
      if (last && last[0] === r.k) {
        last[1].push(r.v);
      } else {
        out.push([r.k, [r.v]]);
      }
    }
    return out.map(([k, parts]) => [k, join(parts)]);
  }

  // write applies ops ({key, value} or {key, delete}) in one transaction.
  write(ops) {
    this.ctx.storage.transactionSync(() => {
      for (const op of ops) {
        this.sql.exec("DELETE FROM tacklr_kv WHERE k = ?", op.key);
        if (op.delete) continue;
        let part = 0;
        do {
          const chunk = op.value.slice(part * PART, (part + 1) * PART);
          this.sql.exec("INSERT INTO tacklr_kv (k, part, v) VALUES (?, ?, ?)", op.key, part, chunk.buffer);
          part++;
        } while (part * PART < op.value.length);
      }
    });
  }

  setAlarm(ms) {
    return this.ctx.storage.setAlarm(ms);
  }

  deleteAlarm() {
    return this.ctx.storage.deleteAlarm();
  }
}

// join is the parts (ArrayBuffers) as one Uint8Array.
function join(parts) {
  if (parts.length === 1) return new Uint8Array(parts[0]);
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
  let at = 0;
  for (const p of parts) {
    out.set(new Uint8Array(p), at);
    at += p.byteLength;
  }
  return out;
}

// prefixEnd is the first string after every string that starts with
// prefix, when its last character allows one; scan also checks the prefix,
// so null only makes the range open.
function prefixEnd(prefix) {
  if (prefix === "") return null;
  const last = prefix.charCodeAt(prefix.length - 1);
  if (last >= 0x7f) return null;
  return prefix.slice(0, -1) + String.fromCharCode(last + 1);
}

// fromRequest is request as the program reads it.
async function fromRequest(request) {
  return {
    method: request.method,
    url: request.url,
    headers: [...request.headers],
    body: new Uint8Array(await request.arrayBuffer()),
  };
}

// toResponse is the program's response, its body streamed as the program
// writes it.
function toResponse(r) {
  const body = new ReadableStream({
    async pull(controller) {
      const chunk = await r.read();
      if (chunk === null) controller.close();
      else controller.enqueue(chunk);
    },
    cancel() {
      r.cancel();
    },
  });
  return new Response(body, { status: r.status, headers: r.headers });
}
