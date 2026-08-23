// Package testhttp hosts an httptest server for official SDK adapters.
// Tests inject Server.Client so the SDK talks HTTP without a GraphAPI fake.
package testhttp

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Server is an httptest.Server. Official clients should use Client().
type Server struct {
	*httptest.Server
}

// New starts a server and registers Cleanup to close it.
func New(tb testing.TB, handler http.Handler) *Server {
	tb.Helper()
	s := &Server{Server: httptest.NewServer(handler)}
	tb.Cleanup(s.Close)
	return s
}
