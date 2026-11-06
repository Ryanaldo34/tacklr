package telemetry

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RPCSpan is a protocol request's span (the RPC semantic conventions,
// rpc.system jsonrpc): a request the server answers (SERVER), or one it
// sends the client (CLIENT), such as a permission it asks. End it once,
// when the response is sent or received.
type RPCSpan struct {
	ctx     context.Context
	span    trace.Span
	method  string
	server  bool
	started time.Time
	ended   bool
}

// StartRPC starts the span of a protocol request named by its method. A
// server request's parent is the client's trace context in ctx (its
// _meta.traceparent, or the connection's); session is the request's
// session (gen_ai.conversation.id), when it names one.
func StartRPC(ctx context.Context, kind trace.SpanKind, method, session string, extra ...attribute.KeyValue) (context.Context, *RPCSpan) {
	attrs := append([]attribute.KeyValue{
		attribute.String(AttrRPCSystem, RPCSystemJSONRPC),
		attribute.String(AttrRPCMethod, method),
	}, extra...)
	if session != "" {
		attrs = append(attrs, attribute.String(AttrGenAIConversationID, session))
		ctx = ContextWithSessionID(ctx, session)
	}
	ctx, span := Tracer().Start(ctx, method, trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
	return ctx, &RPCSpan{ctx: ctx, span: span, method: method, server: kind == trace.SpanKindServer, started: time.Now()}
}

// Context is the span's context: a span of the work the request does is
// its child.
func (s *RPCSpan) Context() context.Context { return s.ctx }

// End ends the request's span. code is the JSON-RPC error code of an
// error response (zero for a result); stop is a prompt's stop reason, when
// it has one. A server request records its duration.
func (s *RPCSpan) End(code int, stop string) {
	if s.ended {
		return
	}
	s.ended = true
	errType := ""
	if code != 0 {
		errType = strconv.Itoa(code)
		s.span.SetAttributes(attribute.Int(AttrJSONRPCError, code), attribute.String(AttrErrorType, errType))
		s.span.SetStatus(codes.Error, errType)
	}
	if stop != "" {
		s.span.SetAttributes(attribute.String(AttrStopReason, stop))
	}
	s.span.End()
	if s.server {
		Metrics().recordRPC(s.ctx, s.method, errType, time.Since(s.started))
	}
}
