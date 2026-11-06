// Package pgtrace is the one Postgres tracer of Tacklr's stores: a span
// for each statement under its caller's context (otelpgx), named by the
// statement and without the pool's connection acquires.
package pgtrace

import (
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
)

// Tracer is the tracer of a pool's connections (pgx.ConnConfig.Tracer).
func Tracer() pgx.QueryTracer {
	return otelpgx.NewTracer(otelpgx.WithTrimSQLInSpanName(), otelpgx.WithDisableAcquireTracer())
}
