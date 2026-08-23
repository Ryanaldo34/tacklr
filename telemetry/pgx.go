package telemetry

import (
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InstrumentPgx installs otelpgx on a pgx connection config so Query/Exec
// spans and query duration metrics parent under the current trace after Init.
// Call on *pgx.ConnConfig, including pgxpool.Config.ConnConfig (already a
// pointer — do not take its address).
//
// Span names are the SQL verb (SELECT/INSERT/…) so cardinality stays low.
// Acquire spans are off. SQL text is on db.query.text; bind parameters are not.
func InstrumentPgx(cfg *pgx.ConnConfig) {
	cfg.Tracer = otelpgx.NewTracer(
		otelpgx.WithTrimSQLInSpanName(),
		otelpgx.WithDisableAcquireTracer(),
	)
}

// RecordPgxPoolStats registers otelpgx pool gauges (acquire wait, idle, max)
// on the global MeterProvider. Query duration and error metrics come from
// InstrumentPgx; this is pool health only.
func RecordPgxPoolStats(pool *pgxpool.Pool) error {
	return otelpgx.RecordStats(pool)
}
