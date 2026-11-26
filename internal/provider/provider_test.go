package provider_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/provider"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/session"
)

func TestPost_sendsTheBodyAsJSONWithTheCallersHeader(t *testing.T) {
	t.Parallel()
	var method, contentType, key, body string
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		method, contentType, key, body = r.Method, r.Header.Get("Content-Type"), r.Header.Get("X-Api-Key"), string(raw)
	}))

	resp, err := provider.Post(t.Context(), srv.Client(), srv.URL, http.Header{"X-Api-Key": {"key"}}, []byte(`{"a":1}`))

	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if method != http.MethodPost || contentType != "application/json" || key != "key" || body != `{"a":1}` {
		t.Errorf("got %s %q key %q body %s, want a JSON POST with the key", method, contentType, key, body)
	}
}

func TestPost_aProviderThatIsGoneIsANetworkError(t *testing.T) {
	t.Parallel()
	srv := testhttp.New(t, http.NotFoundHandler())
	srv.Close()

	_, err := provider.Post(t.Context(), srv.Client(), srv.URL, nil, nil)

	if !errors.Is(err, tacklr.ErrNetwork) {
		t.Errorf("err = %v, want a network error", err)
	}
}

func TestPost_aURLThatDoesNotParseFailsBeforeSending(t *testing.T) {
	t.Parallel()

	_, err := provider.Post(t.Context(), http.DefaultClient, "http://bad\x7f", nil, nil)

	if err == nil || errors.Is(err, tacklr.ErrNetwork) || !strings.Contains(err.Error(), "build provider request") {
		t.Errorf("err = %v, want a request that could not be built, not a network error", err)
	}
}

func TestSSE_readsEachDataPayloadAndSkipsOtherLines(t *testing.T) {
	t.Parallel()
	sse := provider.NewSSE(strings.NewReader("event: a\ndata: one\n\n: comment\ndata:two\nid: 3\n\ndata: [DONE]\n"))

	var got []string
	for data, ok := sse.Next(); ok; data, ok = sse.Next() {
		got = append(got, data)
	}

	if want := []string{"one", "two", "[DONE]"}; !slices.Equal(got, want) || sse.Err() != nil {
		t.Errorf("payloads = %q (err %v), want %q", got, sse.Err(), want)
	}
}

func TestSSE_aBodyCutOffEndsWithItsReadError(t *testing.T) {
	t.Parallel()
	sse := provider.NewSSE(io.MultiReader(strings.NewReader("data: one\n"), iotest.ErrReader(io.ErrUnexpectedEOF)))

	first, _ := sse.Next()
	_, more := sse.Next()

	if first != "one" || more || !errors.Is(sse.Err(), io.ErrUnexpectedEOF) {
		t.Errorf("first %q, more %v, err %v; want the payload, then the cut", first, more, sse.Err())
	}
}

func TestSSE_readsALineOfAMebibyte(t *testing.T) {
	t.Parallel()
	payload := strings.Repeat("x", 1<<20)
	sse := provider.NewSSE(strings.NewReader("data: " + payload + "\n"))

	got, ok := sse.Next()

	if !ok || got != payload {
		t.Errorf("read %d bytes (ok %v, err %v), want the whole line", len(got), ok, sse.Err())
	}
}

func TestChunks_sendGivesUpOnceTheContextEndsWhileNoOneReads(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	chunks := provider.NewChunks(ctx)
	for len(chunks.C) < cap(chunks.C) {
		chunks.Send(tacklr.LLMResponseChunk{})
	}
	cancel()

	sent := chunks.Send(tacklr.LLMResponseChunk{})

	if sent {
		t.Error("send to a full stream after its context ended = true, want false")
	}
}

func TestChunks_failEndsTheStreamWithACompleteErrorChunk(t *testing.T) {
	t.Parallel()
	chunks := provider.NewChunks(t.Context())
	failure := errors.New("boom")

	chunks.Fail(failure)

	got := <-chunks.C
	if got.Type != session.EventError || !got.IsComplete || got.Content != "boom" || !errors.Is(got.Error, failure) {
		t.Errorf("chunk = %+v, want a complete error chunk of the failure", got)
	}
}

func TestAPIStatusError_saysItsStatusCodeAndMessage(t *testing.T) {
	t.Parallel()
	for want, err := range map[string]*provider.APIStatusError{
		"provider HTTP 401 (auth): bad key": {Status: 401, Code: "auth", Body: " bad key\n"},
		"provider HTTP 500: down":           {Status: 500, Body: "down"},
		"provider HTTP 502: no error body":  {Status: 502},
	} {
		got := err.Error()

		if got != want || err.ProviderHTTPStatus() != err.Status || err.ProviderErrorCode() != err.Code {
			t.Errorf("error = %q (status %d, code %q), want %q", got, err.ProviderHTTPStatus(), err.ProviderErrorCode(), want)
		}
	}
}

func TestRetryable_isATimeoutARateLimitOrAServerFailure(t *testing.T) {
	t.Parallel()
	for status, want := range map[int]bool{408: true, 429: true, 500: true, 529: true, 400: false, 401: false, 404: false} {
		if got := provider.Retryable(status); got != want {
			t.Errorf("Retryable(%d) = %v, want %v", status, got, want)
		}
	}
}
