package mcpruntime

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryanaldo34/tacklr/mcp"
)

func TestHeaderTransportInjectsHeaders(t *testing.T) {
	var gotAuth, gotCustom string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	transport := &headerTransport{
		base: http.DefaultTransport,
		headers: []mcp.HTTPHeader{
			{Name: "Authorization", Value: "Bearer shhh"},
			{Name: "X-Custom", Value: "value"},
		},
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	defer resp.Body.Close()

	if gotAuth != "Bearer shhh" {
		t.Errorf("authorization header = %q, want %q", gotAuth, "Bearer shhh")
	}
	if gotCustom != "value" {
		t.Errorf("custom header = %q, want %q", gotCustom, "value")
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHeaderTransportCapturesForbiddenBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("insufficient authentication scopes"))
	}))
	defer ts.Close()

	transport := &headerTransport{base: http.DefaultTransport}

	req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err == nil {
		defer resp.Body.Close()
		t.Fatalf("expected error for 403, got nil")
	}

	var httpErr *httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected *httpError, got %T: %v", err, err)
	}
	if httpErr.Status != http.StatusForbidden {
		t.Errorf("status = %d, want %d", httpErr.Status, http.StatusForbidden)
	}
	if httpErr.Body != "insufficient authentication scopes" {
		t.Errorf("body = %q, want %q", httpErr.Body, "insufficient authentication scopes")
	}
}

func TestHeaderTransportCapturesInternalServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer ts.Close()

	transport := &headerTransport{base: http.DefaultTransport}

	req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	_, err = transport.RoundTrip(req)
	if err == nil {
		t.Fatalf("expected error for 500")
	}

	var httpErr *httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected *httpError, got %T: %v", err, err)
	}
	if httpErr.Status != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", httpErr.Status, http.StatusInternalServerError)
	}
	if httpErr.Body != "boom" {
		t.Errorf("body = %q, want %q", httpErr.Body, "boom")
	}
}

func TestHeaderTransportPassesThroughSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello"))
	}))
	defer ts.Close()

	transport := &headerTransport{base: http.DefaultTransport}

	req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello" {
		t.Errorf("body = %q, want %q", string(body), "hello")
	}
}

func TestHeaderTransportPropagatesRoundTripError(t *testing.T) {
	transport := &headerTransport{base: http.DefaultTransport}

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:1/", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	_, err = transport.RoundTrip(req)
	if err == nil {
		t.Fatalf("expected connection error")
	}
	var httpErr *httpError
	if errors.As(err, &httpErr) {
		t.Errorf("did not expect *httpError for transport failure, got %v", err)
	}
}

func TestBuildHTTPClientAlwaysWrapsTransport(t *testing.T) {
	cfg := mcp.MCPConfig{Name: "plain"}
	client := buildHTTPClient(cfg)
	if client.Transport == nil {
		t.Fatal("expected non-nil transport")
	}
	if _, ok := client.Transport.(*headerTransport); !ok {
		t.Errorf("expected *headerTransport, got %T", client.Transport)
	}
}

func TestBuildHTTPClientWrapsWithHeaders(t *testing.T) {
	cfg := mcp.MCPConfig{Name: "plain", Headers: []mcp.HTTPHeader{{Name: "X-Foo", Value: "bar"}}}
	client := buildHTTPClient(cfg)
	transport, ok := client.Transport.(*headerTransport)
	if !ok {
		t.Fatalf("expected *headerTransport, got %T", client.Transport)
	}
	if len(transport.headers) != 1 || transport.headers[0].Name != "X-Foo" || transport.headers[0].Value != "bar" {
		t.Errorf("expected header X-Foo=bar, got %v", transport.headers)
	}
}
