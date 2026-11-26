// The example Worker: the Tacklr Go program and its JavaScript half.
import "../build/wasm_exec.js";
import wasm from "../build/tacklr.wasm";
import { tacklr } from "../../../../session/durableobject/cloudflare/shim.js";

const app = tacklr(wasm);

// Session is the Durable Object class of the SESSIONS binding: one object
// for each session.
export class Session extends app.Session {}

export default { fetch: app.fetch };
