package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
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

// MultiHandler fans out each record to every child handler (e.g. stderr + OTLP).
// Enabled is true if any child is enabled; Handle returns the first error.
type MultiHandler []slog.Handler

func (m MultiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m {
		if h != nil && h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m MultiHandler) Handle(ctx context.Context, r slog.Record) error {
	var first error
	for _, h := range m {
		if h == nil || !h.Enabled(ctx, r.Level) {
			continue
		}
		// Clone so concurrent handlers cannot race on shared Attrs slices.
		if err := h.Handle(ctx, r.Clone()); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m MultiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(MultiHandler, 0, len(m))
	for _, h := range m {
		if h == nil {
			continue
		}
		out = append(out, h.WithAttrs(attrs))
	}
	return out
}

func (m MultiHandler) WithGroup(name string) slog.Handler {
	out := make(MultiHandler, 0, len(m))
	for _, h := range m {
		if h == nil {
			continue
		}
		out = append(out, h.WithGroup(name))
	}
	return out
}

// NewLogger builds a slog.Logger that correlates records to the active span.
func NewLogger(base slog.Handler) *slog.Logger {
	return slog.New(NewSpanHandler(base))
}

// NewOTLPSlogHandler returns a slog.Handler that exports records via the global
// LoggerProvider (OTLP → collector → Loki). Call after Init so the provider is set.
// name is the instrumentation scope (typically the service name).
func NewOTLPSlogHandler(name string) slog.Handler {
	if name == "" {
		name = InstrumentationName
	}
	return otelslog.NewHandler(name)
}

// InstallDefault wraps base with span correlation. Safe to call after Init.
func InstallDefault(base slog.Handler) {
	if base == nil {
		base = slog.Default().Handler()
	}
	slog.SetDefault(slog.New(NewSpanHandler(base)))
}

// InstallDefaultWithOTLP dual-writes slog to base (e.g. stderr) and OTLP logs
// (Loki via the collector). base and otlp may be nil (otlp skipped when nil).
// Span correlation is applied once on the combined handler.
func InstallDefaultWithOTLP(base, otlp slog.Handler) {
	var handlers []slog.Handler
	if base != nil {
		handlers = append(handlers, base)
	}
	if otlp != nil {
		handlers = append(handlers, otlp)
	}
	if len(handlers) == 0 {
		handlers = append(handlers, slog.DiscardHandler)
	}
	var combined slog.Handler
	if len(handlers) == 1 {
		combined = handlers[0]
	} else {
		combined = MultiHandler(handlers)
	}
	slog.SetDefault(slog.New(NewSpanHandler(combined)))
}
