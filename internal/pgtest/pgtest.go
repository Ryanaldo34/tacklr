// Package pgtest runs the Postgres that live tests share: one pgvector
// container per test binary (Ryuk reaps it on exit), with the extensions
// the brain store uses installed in public, and a schema per test.
package pgtest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Image is the Postgres image of the brain store, session store, wire store
// and Temporal live tests.
const Image = "pgvector/pgvector:0.8.7-pg17"

var (
	once     sync.Once
	conn     string
	admin    *pgxpool.Pool
	startErr error
	schemas  atomic.Int64
)

// Conn is the container's connection string; it starts the container on
// first use.
func Conn(t testing.TB) string {
	t.Helper()
	start(t)
	return conn
}

// Admin is a pool on the container's default schema.
func Admin(t testing.TB) *pgxpool.Pool {
	t.Helper()
	start(t)
	return admin
}

// Schema is a schema name no other test uses; it is dropped when t ends.
func Schema(t testing.TB) string {
	t.Helper()
	start(t)
	name := fmt.Sprintf("test_%d", schemas.Add(1))
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+name+" CASCADE"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

func start(t testing.TB) {
	t.Helper()
	once.Do(func() {
		ctx := context.Background()
		ctr, err := tcpostgres.Run(ctx, Image,
			tcpostgres.WithDatabase("tacklr"),
			tcpostgres.WithUsername("tacklr"),
			tcpostgres.WithPassword("tacklr"),
			tcpostgres.BasicWaitStrategies(),
			tcpostgres.WithSQLDriver("pgx"),
		)
		if err != nil {
			startErr = err
			return
		}
		if conn, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
			startErr = err
			return
		}
		if admin, err = pgxpool.New(ctx, conn); err != nil {
			startErr = err
			return
		}
		_, startErr = admin.Exec(ctx, `
			CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA public;
			CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;
			CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;`)
	})
	if startErr != nil {
		t.Fatalf("postgres container: %v", startErr)
	}
}
