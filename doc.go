// Package tacklr is the stable harness SDK facade.
//
// The root package owns agent construction, turn execution, and tool
// registration. Domain data types have canonical packages:
//   - streaming owns messages, events, tool calls, and todos.
//   - brain owns knowledge retrieval and graph capabilities.
//   - vfs owns virtual filesystem mounts, sessions, and provider interfaces.
//   - builtins owns optional tool constructors (email, Exa) and VFS backend factories.
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
