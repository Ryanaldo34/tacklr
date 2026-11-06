// Package server serves a session.Client over host-defined wire protocols.
//
// Protocol is the extension point; the ACP built-in is package server/acp.
// More protocols can be served by the same Server, each at its own path:
//
//	srv := server.NewServer(rt, agent, acp.New(wire), myProtocol{})
//	_ = srv.ServeHTTP(ctx, addr) // or srv.Serve(ctx, listener)
//
// A connection is event-driven: its Handler gets each frame from the peer
// and each event of the sessions it follows, one call at a time, and never
// waits for the peer. On net/http the Server accepts each connection as a
// WebSocket and holds it in a goroutine. A platform that holds the socket
// itself (a Cloudflare Durable Object) calls Connect, then Connection.Serve
// on its socket, so every host runs the same protocol code. docs/protocol.md
// follows a turn through the server, from the client's prompt to the answer.
//
// The program on the other end of a connection is the caller's. This
// module does not ship it. A protocol parses its own credential payload and
// stores the bindings on a CredentialBag; it puts bag.Take() on Prompt or
// Resume. Runtime, harness, VFS, and Temporal do not import this package.
package server
