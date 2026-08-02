package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// SpanHandler wraps a slog.Handler and, for records that carry a context with an
// active span, attaches trace_id/span_id so logs correlate with traces.
//
// It does not mirror log records as span events — that floods the turn lifecycle
// view with operational noise. Use a log backend that joins on trace_id instead.
type SpanHandler struct {
	inner slog.Handler
}

// NewSpanHandler wraps base. base must not be nil.
func NewSpanHandler(base slog.Handler) *SpanHandler {
	if base == nil {
		base = slog.DiscardHandler
	}
	return &SpanHandler{inner: base}
}

func (h *SpanHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *SpanHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	if sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *SpanHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SpanHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *SpanHandler) WithGroup(name string) slog.Handler {
	return &SpanHandler{inner: h.inner.WithGroup(name)}
}

// NewLogger builds a slog.Logger that correlates records to the active span.
func NewLogger(base slog.Handler) *slog.Logger {
	return slog.New(NewSpanHandler(base))
}

// InstallDefault wraps the current default handler (or a text stderr handler)
// with span correlation. Safe to call after Init.
func InstallDefault(base slog.Handler) {
	if base == nil {
		base = slog.Default().Handler()
	}
	slog.SetDefault(slog.New(NewSpanHandler(base)))
}
