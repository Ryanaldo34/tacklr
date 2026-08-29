package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ryanaldo34/tacklr/brain/postgres"
)

func TestNew_requiresDB(t *testing.T) {
	if _, err := postgres.New(nil); err == nil {
		t.Fatal("want error")
	}
}

type execErrDB struct{}

func (execErrDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unused")
}
func (execErrDB) QueryRow(context.Context, string, ...any) pgx.Row { return execErrRow{} }
func (execErrDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("exec boom")
}

type execErrRow struct{}

func (execErrRow) Scan(...any) error { return pgx.ErrNoRows }

func TestStore_setupReportsExecError(t *testing.T) {
	store, err := postgres.New(execErrDB{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(context.Background()); err == nil {
		t.Fatal("want setup error")
	}
}
