// Package server serves a durable.Runtime over host-defined wire protocols.
//
// Protocol is the extension point. Implement it to own HTTP/WebSocket routes,
// stream framing, and HITL resume. ACP is the native option (NewACPProtocol);
// more protocols can be mounted on the same Server:
//
//	srv := server.NewServer(rt, cat, server.NewACPProtocol(nil), myProtocol{})
//	_ = srv.ServeHTTP(ctx, addr)
//
// RunTurn pumps Runtime.Prompt/Resume/Subscribe through Protocol.OnStreamEvent
// and OnStreamClosed. Map wire credentials into durable.AuthContext on the work
// item. Kernel, harness, VFS, and Temporal do not import this package.
package server
