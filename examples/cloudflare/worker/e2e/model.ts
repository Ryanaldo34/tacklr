// A scripted OpenAI Responses endpoint for the end-to-end test. It answers
// by the last thing the conversation said:
//   "hello"            -> streams a greeting in deltas
//   "lookup"/"report"  -> calls that tool
//   "deploy"           -> calls deploy, which asks for permission
//   "research"         -> starts the researcher specialist
//   "find the answer"  -> (the specialist's task) calls lookup
//   a tool's output    -> "done: " and the output (its first 40 bytes)
// It keeps the size of each tool output it got, for the test to read. It
// is the Worker's OTLP endpoint too: it keeps each span it gets
// (POST /v1/traces, OTLP's JSON encoding).

const enc = new TextEncoder();
const sse = (events: unknown[]) =>
  new Response(
    new ReadableStream({
      async start(c) {
        for (const e of events) {
          c.enqueue(enc.encode(`data: ${JSON.stringify(e)}\n\n`));
          await Bun.sleep(5);
        }
        c.close();
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );

const completed = { type: "response.completed", response: { status: "completed", usage: { input_tokens: 100, output_tokens: 10 } } };

let calls = 0;
function call(name: string, args: unknown) {
  const id = `fc_${++calls}`;
  return sse([
    { type: "response.output_item.done", item: { type: "function_call", id, call_id: id, name, arguments: JSON.stringify(args), status: "completed" } },
    completed,
  ]);
}

function say(text: string, parts = 1) {
  const id = `msg_${++calls}`;
  const size = Math.ceil(text.length / parts);
  const deltas = [];
  for (let i = 0; i < text.length; i += size) {
    deltas.push({ type: "response.output_text.delta", item_id: id, delta: text.slice(i, i + size) });
  }
  return sse([
    ...deltas,
    { type: "response.output_item.done", item: { id, type: "message", status: "completed", content: [{ type: "output_text", text }] } },
    completed,
  ]);
}

const text = (content: unknown): string =>
  typeof content === "string" ? content : Array.isArray(content) ? content.map((p) => p.text ?? "").join("") : "";

export type Span = { traceId: string; spanId: string; parentSpanId?: string; name: string };

export function startModel() {
  const outputs: number[] = [];
  const spans: Span[] = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const { pathname } = new URL(req.url);
      const body = await req.json();
      if (pathname === "/v1/traces") {
        for (const rs of body.resourceSpans ?? []) for (const ss of rs.scopeSpans ?? []) spans.push(...(ss.spans ?? []));
        return Response.json({});
      }
      if (pathname.endsWith("/input_tokens")) return Response.json({ input_tokens: 100 });
      const last = [...body.input].reverse().find((i: any) => i.type === "function_call_output" || i.role === "user");
      if (last?.type === "function_call_output") {
        const out = text(last.output);
        outputs.push(out.length);
        return say(`done: ${out.slice(0, 40)}`);
      }
      const said = text(last?.content);
      if (said.includes("hello")) return say("Hello from the edge", 3);
      if (said.includes("deploy")) return call("deploy", {});
      if (said.includes("lookup") || said.includes("find the answer")) return call("lookup", {});
      if (said.includes("report")) return call("report", {});
      if (said.includes("research")) {
        return call("spawn_specialist", { specialist: "researcher", task_description_and_context: "find the answer", block: true });
      }
      return say("I do not know that.");
    },
  });
  return { url: `http://localhost:${server.port}`, outputs, spans, stop: () => server.stop(true) };
}
