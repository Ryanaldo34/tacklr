package brain

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// PgxQuerier is the subset of *pgx.Conn / *pgxpool.Pool needed by PostgresStore.
// Method signatures match pgx so adapters are zero-cost wrappers.
type PgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type pgxDBTX struct {
	q PgxQuerier
}

// AdaptPgx wraps a pgx connection or pool as DBTX for NewPostgresStore.
// pgx return types (pgx.Row / pgx.Rows) are not assignable to brain.Row/Rows
// without this adapter because Go interface method signatures must match exactly.
func AdaptPgx(q PgxQuerier) DBTX {
	if q == nil {
		return nil
	}
	return pgxDBTX{q: q}
}

func (a pgxDBTX) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return a.q.QueryRow(ctx, sql, args...)
}

func (a pgxDBTX) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return a.q.Query(ctx, sql, args...)
}
