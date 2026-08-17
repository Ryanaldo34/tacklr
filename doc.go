// Package tacklr is the stable harness SDK facade.
//
// The root package owns agent construction, turn execution, and tool
// registration. Domain data types have canonical packages:
//   - streaming owns messages, events, tool calls, and todos.
//   - brain owns knowledge retrieval and graph capabilities.
//   - vfs owns virtual filesystem providers and mount sessions.
//   - mcp owns MCP connection configuration.
//
// New APIs should use the canonical domain packages and must not add server
// transport, wire protocol, persistence backend, or provider-client details
// here.
package tacklr
