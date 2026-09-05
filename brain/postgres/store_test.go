package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
)

func TestNew_requiresDB(t *testing.T) {
	if _, err := postgres.New(nil); err == nil {
		t.Fatal("want error")
	}
}

type failDB struct{}

func (failDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("query down")
}
func (failDB) QueryRow(context.Context, string, ...any) pgx.Row { return failRow{} }
func (failDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("exec down")
}

type failRow struct{}

func (failRow) Scan(...any) error { return pgx.ErrNoRows }

func TestStore_setupReportsExecError(t *testing.T) {
	store, err := postgres.New(failDB{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(context.Background()); err == nil {
		t.Fatal("want setup error")
	}
}

func TestStore_clientAndQueryFailures(t *testing.T) {
	store, err := postgres.New(failDB{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	scope := brain.Scope{}
	id := uuid.New()

	if got, err := store.GetMany(ctx, scope, nil); err != nil || got != nil {
		t.Fatalf("empty ids: %v %v", got, err)
	}
	if err := store.SoftDelete(ctx, scope, uuid.Nil); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("nil id: %v", err)
	}
	if err := store.Put(ctx, brain.Object{}); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid put: %v", err)
	}
	if _, err := store.GetKind(ctx, "ghost"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("missing kind: %v", err)
	}
	if _, err := store.GetMany(ctx, scope, []uuid.UUID{id}); err == nil {
		t.Fatal("want query failure")
	}
	if err := store.Put(ctx, brain.Object{ID: id, Kind: "Document", Namespace: mustNS(t, "org", "a")}); err == nil {
		t.Fatal("want exec failure")
	}
	if _, err := store.SearchLexical(ctx, scope, "q", brain.Filter{UpdatedAfter: "not-a-time"}, 5); err == nil {
		t.Fatal("want invalid filter")
	}
	if _, err := store.SearchVector(ctx, scope, []float32{1}, brain.Filter{UpdatedAfter: "not-a-time"}, 5); err == nil {
		t.Fatal("want invalid vector filter")
	}
	if _, err := store.SearchTrigram(ctx, scope, "q", brain.Filter{UpdatedAfter: "not-a-time"}, 5); err == nil {
		t.Fatal("want invalid trigram filter")
	}
	if err := store.PutKind(ctx, brain.ObjectKind{}); err == nil {
		t.Fatal("want empty kind")
	}
	if _, err := store.GetByProperty(ctx, scope, "", "v"); err == nil {
		t.Fatal("want empty property key")
	}
	if got, err := store.ListByKind(ctx, scope, "", 10); err != nil || got != nil {
		t.Fatalf("empty kind list: %v %v", got, err)
	}
	if err := store.Put(ctx, brain.Object{
		ID: id, Kind: "Document", Namespace: mustNS(t, "org", "a"),
		Properties: map[string]any{"ch": make(chan int)},
	}); err == nil {
		t.Fatal("want properties marshal error")
	}
}
