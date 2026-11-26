// Package pgtrace is the one Postgres tracer of Tacklr's stores: a CLIENT
// span for each statement, batch and copy, under the trace in the
// caller's context. That is a span of this process, or a remote span
// context, such as the turn's trace that a session step carries. A
// statement whose context has no trace has no span, so the stores start no
// traces of their own.
package pgtrace

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// The attributes of a statement's span (the OpenTelemetry database
// conventions).
const (
	attrSystem     = "db.system.name"
	attrOperation  = "db.operation.name"
	attrQuery      = "db.query.text"
	attrCollection = "db.collection.name"
	attrBatchSize  = "db.operation.batch.size"
	systemPostgres = "postgresql"
)

// tracer traces a connection's statements (pgx.QueryTracer), batches
// (pgx.BatchTracer) and copies (pgx.CopyFromTracer).
type tracer struct{}

// Tracer is the tracer of a pool's connections (pgx.ConnConfig.Tracer).
func Tracer() pgx.QueryTracer {
	return tracer{}
}

// spanKey holds the span a start began, so its end ends that span and no
// other.
type spanKey struct{}

func (tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	operation := operation(data.SQL)
	return start(ctx, operation, attribute.String(attrOperation, operation), attribute.String(attrQuery, data.SQL))
}

func (tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	end(ctx, data.Err)
}

func (tracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	return start(ctx, "BATCH", attribute.String(attrOperation, "BATCH"), attribute.Int(attrBatchSize, data.Batch.Len()))
}

// TraceBatchQuery adds nothing: the batch's span covers its statements.
func (tracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	end(ctx, data.Err)
}

func (tracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	table := data.TableName.Sanitize()
	return start(ctx, "COPY "+table, attribute.String(attrOperation, "COPY"), attribute.String(attrCollection, table))
}

func (tracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	end(ctx, data.Err)
}

// start begins the span name under ctx's trace, when ctx has one.
func start(ctx context.Context, name string, attrs ...attribute.KeyValue) context.Context {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	ctx, span := telemetry.Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(append(attrs, attribute.String(attrSystem, systemPostgres))...))
	return context.WithValue(ctx, spanKey{}, span)
}

// end ends the span that start began in ctx, with err's type when it
// failed: the SQLSTATE of a Postgres error, else _OTHER.
func end(ctx context.Context, err error) {
	span, ok := ctx.Value(spanKey{}).(trace.Span)
	if !ok {
		return
	}
	if err != nil {
		errorType := "_OTHER"
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
			errorType = pgErr.Code
		}
		span.SetAttributes(attribute.String(telemetry.AttrErrorType, errorType))
		span.SetStatus(codes.Error, errorType)
	}
	span.End()
}

// operation is the statement's first keyword, such as SELECT.
func operation(sql string) string {
	keyword, _, _ := strings.Cut(strings.TrimSpace(sql), " ")
	keyword, _, _ = strings.Cut(keyword, "\n")
	return strings.ToUpper(keyword)
}
