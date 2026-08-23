package brain

import (
	"context"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// tracedDB runs otelpgx on Query/Exec so SQL client spans parent under the
// caller ctx (tacklr.turn / tacklr.tool after telemetry.Init). otelpgx uses
// the global TracerProvider and skips span start when the parent is not
// recording, so Postgres does not open its own traces.
type tracedDB struct {
	inner PgxDB
	tr    *otelpgx.Tracer
}

func newTracedDB(inner PgxDB) tracedDB {
	return tracedDB{
		inner: inner,
		tr: otelpgx.NewTracer(
			otelpgx.WithTrimSQLInSpanName(),
			otelpgx.WithDisableAcquireTracer(),
		),
	}
}

func (d tracedDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctx = d.tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: args})
	tag, err := d.inner.Exec(ctx, sql, args...)
	d.tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{CommandTag: tag, Err: err})
	return tag, err
}

func (d tracedDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx = d.tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: args})
	rows, err := d.inner.Query(ctx, sql, args...)
	d.tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: err})
	return rows, err
}

func (d tracedDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx = d.tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: args})
	return tracedRow{row: d.inner.QueryRow(ctx, sql, args...), ctx: ctx, tr: d.tr}
}

type tracedRow struct {
	row pgx.Row
	ctx context.Context
	tr  *otelpgx.Tracer
}

func (r tracedRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.tr.TraceQueryEnd(r.ctx, nil, pgx.TraceQueryEndData{Err: err})
	return err
}
