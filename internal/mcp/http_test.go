package mcpruntime

import (
	"errors"
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

func TestHeaderTransportCapturesHTTPErrors(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{http.StatusForbidden, "insufficient authentication scopes"},
		{http.StatusInternalServerError, "boom"},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(ts.Close)
			req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = (&headerTransport{base: http.DefaultTransport}).RoundTrip(req)
			var httpErr *httpError
			if !errors.As(err, &httpErr) || httpErr.Status != tc.status || httpErr.Body != tc.body {
				t.Fatalf("err = %v", err)
			}
		})
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
