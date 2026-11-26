package pgtrace_test

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/pgtrace"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// traced is a pool whose statements are traced, and the spans it ends.
func traced(t *testing.T) (*pgxpool.Pool, *tracetest.SpanRecorder) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(pgtest.Conn(t))
	require.NoError(t, err)
	spans := tracetest.NewSpanRecorder()
	telemetry.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	t.Cleanup(func() { telemetry.SetTracerProvider(nil) })
	cfg.ConnConfig.Tracer = pgtrace.Tracer()
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool, spans
}

// remote is ctx with a sampled remote span context, as a session step has
// its turn's trace.
func remote(ctx context.Context) context.Context {
	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, Remote: true,
	}))
}

func attr(s sdktrace.ReadOnlySpan, key string) attribute.Value {
	i := slices.IndexFunc(s.Attributes(), func(kv attribute.KeyValue) bool { return string(kv.Key) == key })
	if i < 0 {
		return attribute.Value{}
	}
	return s.Attributes()[i].Value
}

func TestAStatementIsASpanOfTheTraceInItsContext(t *testing.T) {
	pool, spans := traced(t)
	ctx := remote(t.Context())

	_, err := pool.Exec(ctx, "SELECT 1")

	require.NoError(t, err)
	ended := spans.Ended()
	require.Len(t, ended, 1)
	s := ended[0]
	require.Equal(t, "SELECT", s.Name())
	require.Equal(t, trace.SpanKindClient, s.SpanKind())
	require.Equal(t, trace.TraceID{1}, s.SpanContext().TraceID())
	require.Equal(t, trace.SpanID{2}, s.Parent().SpanID())
	require.Equal(t, "postgresql", attr(s, "db.system.name").AsString())
	require.Equal(t, "SELECT 1", attr(s, "db.query.text").AsString())
	require.Equal(t, codes.Unset, s.Status().Code)
}

func TestAStatementWithNoTraceHasNoSpan(t *testing.T) {
	pool, spans := traced(t)

	_, err := pool.Exec(t.Context(), "SELECT 1")

	require.NoError(t, err)
	require.Empty(t, spans.Ended())
}

func TestAStatementUnderTheCallersSpanLeavesThatSpanOpen(t *testing.T) {
	pool, spans := traced(t)
	ctx, caller := telemetry.Tracer().Start(t.Context(), "caller")

	_, err := pool.Exec(ctx, "SELECT 1")

	require.NoError(t, err)
	require.True(t, caller.IsRecording())
	ended := spans.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, caller.SpanContext().SpanID(), ended[0].Parent().SpanID())
	caller.End()
}

func TestAFailedStatementHasItsErrorType(t *testing.T) {
	pool, spans := traced(t)
	ctx := remote(t.Context())

	_, missing := pool.Exec(ctx, "SELECT * FROM tacklr_missing_table")
	_, unencodable := pool.Exec(ctx, "SELECT $1::int", make(chan int))

	require.Error(t, missing)
	require.Error(t, unencodable)
	ended := spans.Ended()
	require.Len(t, ended, 2)
	require.Equal(t, "42P01", attr(ended[0], "error.type").AsString())
	require.Equal(t, codes.Error, ended[0].Status().Code)
	require.Equal(t, "_OTHER", attr(ended[1], "error.type").AsString())
}

func TestABatchAndACopyAreSpans(t *testing.T) {
	pool, spans := traced(t)
	ctx := remote(t.Context())
	conn, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(t.Context(), "CREATE TEMP TABLE tacklr_copied (n int)")
	require.NoError(t, err)
	batch := &pgx.Batch{}
	batch.Queue("SELECT 1")
	batch.Queue("SELECT 2")

	batchErr := conn.SendBatch(ctx, batch).Close()
	_, copyErr := conn.CopyFrom(ctx, pgx.Identifier{"tacklr_copied"}, []string{"n"}, pgx.CopyFromRows([][]any{{1}, {2}}))

	require.NoError(t, batchErr)
	require.NoError(t, copyErr)
	ended := spans.Ended()
	require.Len(t, ended, 2)
	require.Equal(t, "BATCH", ended[0].Name())
	require.Equal(t, int64(2), attr(ended[0], "db.operation.batch.size").AsInt64())
	require.Equal(t, `COPY "tacklr_copied"`, ended[1].Name())
	require.Equal(t, `"tacklr_copied"`, attr(ended[1], "db.collection.name").AsString())
}
