package telemetry

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestInstrumentPgx_setsQueryTracer: host pool/conn config carries an otelpgx
// tracer so later Query/Exec on that connection emit under the current span.
func TestInstrumentPgx_setsQueryTracer(t *testing.T) {
	cfg, err := pgx.ParseConfig("postgres://localhost:5432/brain")
	if err != nil {
		t.Fatal(err)
	}
	InstrumentPgx(cfg)
	if cfg.Tracer == nil {
		t.Fatal("expected otelpgx tracer on ConnConfig")
	}
}

// TestRecordPgxPoolStats_registers: constructing a pool (MinConns 0, no live
// server) is enough for otelpgx to attach pool gauges to the global meter.
func TestRecordPgxPoolStats_registers(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://127.0.0.1:1/brain")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := RecordPgxPoolStats(pool); err != nil {
		t.Fatal(err)
	}
}
