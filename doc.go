// Package tacklr is the stable harness SDK facade.
//
// The root package owns agent construction, turn execution, tool
// registration, conversation types (Message, StreamEvent, Todo), and
// the session checkpoint blob. Domain packages:
//   - brain owns knowledge retrieval and graph capabilities.
//   - vfs owns virtual filesystem mounts, sessions, and provider interfaces.
//   - builtins owns optional tool constructors (email, Exa), VFS backend
//     factories, and the OpenAI-compatible model client.
//   - mcp owns MCP connection configuration.
//
// Process-wide registrations (built-in interrupts, common VFS codecs, the
// durable driver adapter) run in this package's init. Hosts import tacklr
// once; they do not register those defaults themselves.
//
// New APIs should use the canonical domain packages and must not add server
// transport, wire protocol, persistence backend, or provider-client details
// here.
package tacklr
