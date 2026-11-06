package telemetry

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Carrier is a trace context (W3C traceparent, tracestate and baggage) as
// plain data: how a turn's trace crosses what a context cannot, such as a
// wake to a session, a step of a durable runtime, a journal, or a message
// between objects. It is nil when there is no trace.
type Carrier map[string]string

// Inject is ctx's trace context as a Carrier, by the process-wide
// propagator (export.Init installs W3C trace context and baggage). It is
// nil when ctx has no span.
func Inject(ctx context.Context) Carrier {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}
	c := Carrier{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(c))
	return c
}

// Context is ctx with c's trace context: a span started from it is a child
// of the span c carries. With no trace in c it is ctx.
func (c Carrier) Context(ctx context.Context) context.Context {
	if len(c) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(c))
}

// Link is a link to the span c carries, for a span that follows from it
// without being its child; ok is false when c carries none.
func (c Carrier) Link() (trace.Link, bool) {
	sc := trace.SpanContextFromContext(c.Context(context.Background()))
	return trace.Link{SpanContext: sc}, sc.IsValid()
}

// Transport sends each request with its context's trace context in its
// headers (W3C traceparent), so the system it reaches (a model provider or
// a gateway before one, an MCP server) continues the trace. It starts no
// span: the operation's own span (chat, execute_tool) is the client span.
// A nil base is http.DefaultTransport.
func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return propagating{base: base}
}

type propagating struct{ base http.RoundTripper }

func (p propagating) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if trace.SpanContextFromContext(ctx).IsValid() {
		req = req.Clone(ctx)
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	}
	return p.base.RoundTrip(req)
}

// FromHeaders is ctx with the trace context of an incoming request's
// headers, for a server that continues the trace of its caller.
func FromHeaders(ctx context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}
