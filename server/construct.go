package server

import (
	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/durable"
)

// NewACPServer returns a Server with ACP and an in-memory wire session store.
//
//	rt := inprocess.New(catalog)
//	srv := server.NewACPServer(rt, catalog)
func NewACPServer(rt durable.Runtime, cat durable.Catalog) *Server {
	return NewACPServerWithWire(rt, cat, NewMemoryWireStore())
}

// NewACPServerWithWire returns a Server with ACP using the given wire store.
func NewACPServerWithWire(rt durable.Runtime, cat durable.Catalog, wire ProtocolWireStore) *Server {
	return NewServer(rt, cat, NewACPProtocol(wire))
}

// NewACPProtocolMemory is shorthand for NewACPProtocol(NewMemoryWireStore()).
func NewACPProtocolMemory() Protocol {
	return NewACPProtocol(NewMemoryWireStore())
}

// NewACPProtocolPostgres is shorthand for ACP with a Postgres wire store on conn.
func NewACPProtocolPostgres(conn *pgx.Conn) Protocol {
	return NewACPProtocol(NewPostgresWireStore(conn, "acp"))
}

// NewACPServerPostgres returns a Server with ACP backed by a Postgres wire store.
func NewACPServerPostgres(rt durable.Runtime, cat durable.Catalog, conn *pgx.Conn) *Server {
	return NewACPServerWithWire(rt, cat, NewPostgresWireStore(conn, "acp"))
}
