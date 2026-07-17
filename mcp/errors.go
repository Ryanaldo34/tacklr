package mcp

import "errors"

var (
	// ErrMCPAuthRequired is returned when an MCP server config requires
	// authentication but no AuthToken or OAuthURL is provided.
	ErrMCPAuthRequired = errors.New("mcp auth required")

	// ErrMCPTransportNotSupported is returned when an MCP server config
	// specifies an unsupported transport type.
	ErrMCPTransportNotSupported = errors.New("mcp transport not supported")
)
