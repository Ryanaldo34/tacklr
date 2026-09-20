package testhttp

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// New starts httptest.Server and closes it on cleanup.
func New(tb testing.TB, h http.Handler) *httptest.Server {
	tb.Helper()
	s := httptest.NewServer(h)
	tb.Cleanup(s.Close)
	return s
}
