// Package server serves a session.Client over host-defined wire protocols.
//
// Protocol is the extension point. Implement it to own HTTP routes and WebSocket,
// stream framing, and HITL resume. The ACP built-in is package server/acp.
// More protocols can be mounted on the same Server:
//
//	srv := server.NewServer(rt, agent, acp.New(nil), myProtocol{})
//	_ = srv.ServeHTTP(ctx, addr) // or srv.Serve(ctx, listener)
//
// The program on the other end of a connection is the caller's. This module
// does not ship that program. A protocol asks it through Conn.Ask.
//
// RunTurn pumps Client.Prompt/Resume/Subscribe through Protocol.OnEvent
// and OnStreamClosed. A protocol parses its own credential payload and stores
// the bindings on session.CredentialBag. Runtime, harness, VFS, and Temporal
// do not import this package.
package server
