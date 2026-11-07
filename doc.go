// Package tacklr is the harness: Harness runs one session's steps (a
// model request through Model, a tool call with its CallContext, a commit
// into the window), and the root package owns tool registration, the
// window and plan, the checkpoint encoding, and the VFS, brain and MCP
// wiring. Other packages:
//   - message holds the conversation data (Message, ContentPart, ToolCall).
//   - session is the client contract (Client, Store, Event); session/orchestrator
//     decides each step of a session over an orchestrator.Runtime; session/temporal is the
//     Temporal client and session/temporal/worker its worker.
//   - brain owns knowledge retrieval; brain/postgres and brain/helixgraph
//     are the store and graph adapters.
//   - vfs owns virtual filesystem mounts and the backend constructors;
//     vfs/mount holds the mount bindings a client sends.
//   - anthropic and openai are the model clients (Claude, and OpenAI-compatible
//     endpoints); web holds optional host tools.
//   - mcp owns MCP connection configuration.
//
// Process-wide registrations (built-in interrupts, common VFS codecs) run
// in this package's init. Hosts import tacklr
// once; they do not register those defaults themselves.
//
// New APIs should use the canonical domain packages and must not add server
// transport, wire protocol, persistence backend, or provider-client details
// here.
package tacklr
