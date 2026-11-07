// Package provider holds what the model clients share and no provider's
// rules: the HTTP request, the server-sent event reader, the stream of
// response chunks, the error of an HTTP failure, and its telemetry.
package provider

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

var _ telemetry.ProviderStatus = (*APIStatusError)(nil)

var (
	// ErrIncompleteStream is a stream that closed before its terminal event.
	ErrIncompleteStream = errors.New("incomplete provider stream")
	// ErrMalformedStream is a stream event that is not valid JSON.
	ErrMalformedStream = errors.New("malformed provider stream")
)

// APIStatusError is an HTTP error answer from a model API. Find it in a
// wrapped chain with errors.As.
type APIStatusError struct {
	Status int
	Body   string
	Code   string
}

func (e *APIStatusError) Error() string {
	msg := strings.TrimSpace(e.Body)
	if msg == "" {
		msg = "no error body"
	}
	if e.Code != "" {
		return fmt.Sprintf("provider HTTP %d (%s): %s", e.Status, e.Code, msg)
	}
	return fmt.Sprintf("provider HTTP %d: %s", e.Status, msg)
}

// ProviderHTTPStatus implements telemetry.ProviderStatus.
func (e *APIStatusError) ProviderHTTPStatus() int { return e.Status }

// ProviderErrorCode implements telemetry.ProviderStatus.
func (e *APIStatusError) ProviderErrorCode() string { return e.Code }

// Retryable reports a status the durable runtime retries: a timeout, a
// rate limit, or a server failure.
func Retryable(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

// Post sends body as JSON to url with header. A transport failure wraps
// tacklr.ErrNetwork. The caller closes the response body.
func Post(ctx context.Context, client *http.Client, url string, header http.Header, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build provider request: %w", err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, tacklr.Network(fmt.Errorf("provider request failed: %w", err))
	}
	return resp, nil
}

// SSE reads the data payloads of a server-sent event stream, one line
// each. Other lines (event names, comments, blank lines) are skipped.
type SSE struct{ sc *bufio.Scanner }

// NewSSE reads r, with lines up to 2 MiB.
func NewSSE(r io.Reader) *SSE {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	return &SSE{sc: sc}
}

// Next is the next data payload, or false at the end of the stream.
func (s *SSE) Next() (string, bool) {
	const prefix = "data:"
	for s.sc.Scan() {
		if data, ok := strings.CutPrefix(s.sc.Text(), prefix); ok {
			return strings.TrimPrefix(data, " "), true
		}
	}
	return "", false
}

// Err is the read error that ended the stream, if any.
func (s *SSE) Err() error { return s.sc.Err() }

// Chunks is a stream of response chunks that stops sending once its
// context ends, so a client goroutine never blocks on a reader that left.
type Chunks struct {
	ctx context.Context
	C   chan tacklr.LLMResponseChunk
}

// NewChunks is a stream for one request on ctx.
func NewChunks(ctx context.Context) Chunks {
	return Chunks{ctx: ctx, C: make(chan tacklr.LLMResponseChunk, 10)}
}

// Send delivers chunk. It is false when the context ended first.
func (c Chunks) Send(chunk tacklr.LLMResponseChunk) bool {
	if c.ctx.Err() != nil {
		return false
	}
	select {
	case <-c.ctx.Done():
		return false
	case c.C <- chunk:
		return true
	}
}

// Fail ends the stream with err.
func (c Chunks) Fail(err error) {
	c.Send(tacklr.LLMResponseChunk{Type: session.EventError, Content: err.Error(), Error: err, IsComplete: true})
}

// EmitFailed records a failed model request as a span-correlated log
// event. inputItems summarizes the request's input for diagnosis.
func EmitFailed(ctx context.Context, err error, httpStatus int, inputItems string) {
	attrs := []attribute.KeyValue{
		attribute.String(telemetry.EventAttrInputItems, inputItems),
		attribute.String(telemetry.AttrErrorType, telemetry.ClassifyErrorClass(err, httpStatus)),
	}
	if httpStatus > 0 {
		attrs = append(attrs, attribute.Int(telemetry.AttrHTTPStatus, httpStatus))
	}
	if api, ok := errors.AsType[*APIStatusError](err); ok && api.Code != "" {
		attrs = append(attrs, attribute.String(telemetry.AttrErrorCode, api.Code))
	}
	telemetry.EmitEventSeverity(ctx, telemetry.EventProviderFailed, log.SeverityError, attrs...)
}
