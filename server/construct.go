package server

import (
	"github.com/jackc/pgx/v5"
)

// NewACPServer returns a Server with ACP and an in-memory wire session store.
// Suitable for demos, single-process apps, and tests that do not need durable
// session/load across process restarts.
//
//	reg := server.NewRegistry(stores.NewInMemoryStore(), "agent")
//	reg.Register("agent", server.AgentSpec{...})
//	srv := server.NewACPServer(reg)
//	_ = srv.ServeStdio(ctx, os.Stdin, os.Stdout)
//	// or: _ = srv.ServeHTTP(ctx, ":8080")  // /acp WebSocket + Streamable HTTP
func NewACPServer(reg *Registry) *Server {
	return NewACPServerWithWire(reg, NewMemoryWireStore())
}

// NewACPServerWithWire returns a Server with ACP using the given wire store for
// protocol session envelopes (session/new, session/load). The registry's
// BaseStore remains the harness checkpoint store (agent conversation state).
//
//	wire := server.NewPostgresWireStore(conn, "acp")
//	srv := server.NewACPServerWithWire(reg, wire)
//
// Or any custom ProtocolWireStore (Redis, SQLite, …).
func NewACPServerWithWire(reg *Registry, wire ProtocolWireStore) *Server {
	return NewServer(reg, NewACPProtocol(wire))
}

// NewACPProtocolMemory is shorthand for NewACPProtocol(NewMemoryWireStore()).
// Use when mounting ACP alongside other protocols:
//
//	server.NewServer(reg, server.NewACPProtocolMemory(), server.SSE)
func NewACPProtocolMemory() Protocol {
	return NewACPProtocol(NewMemoryWireStore())
}

// NewACPProtocolPostgres is shorthand for ACP with a Postgres wire store on conn.
// protocolKey is "acp". Requires table public.protocol_wire_session
// (see stores/testdata/session_schema.sql).
//
//	server.NewServer(reg, server.NewACPProtocolPostgres(conn))
func NewACPProtocolPostgres(conn *pgx.Conn) Protocol {
	return NewACPProtocol(NewPostgresWireStore(conn, "acp"))
}

// NewACPServerPostgres returns a Server with ACP backed by a Postgres wire store.
// The registry should already use a harness store (e.g. stores.NewPostgresStore(conn)
// or InMemoryStore). Wire and harness schemas are separate; sharing *pgx.Conn is fine.
//
//	harness := stores.NewPostgresStore(conn)
//	reg := server.NewRegistry(harness, "agent")
//	// reg.Register(...)
//	srv := server.NewACPServerPostgres(reg, conn)
func NewACPServerPostgres(reg *Registry, conn *pgx.Conn) *Server {
	return NewACPServerWithWire(reg, NewPostgresWireStore(conn, "acp"))
}
