package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
)

type traceStubDB struct{}

func (traceStubDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return emptyRows{}, nil
}

func (traceStubDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return errRow{}
}

func (traceStubDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

type errRow struct{}

func (errRow) Scan(...any) error { return pgx.ErrNoRows }

type emptyRows struct{}

func (emptyRows) Close()                                       {}
func (emptyRows) Err() error                                   { return nil }
func (emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (emptyRows) Next() bool                                   { return false }
func (emptyRows) Scan(...any) error                            { return nil }
func (emptyRows) Values() ([]any, error)                       { return nil, nil }
func (emptyRows) RawValues() [][]byte                          { return nil }
func (emptyRows) Conn() *pgx.Conn                              { return nil }

func TestStore_sqlSpansParentUnderCallerSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	store, err := postgres.New(traceStubDB{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, parent := tp.Tracer("test").Start(context.Background(), "tacklr.tool")
	_, _ = store.Get(ctx, brain.Scope{}, uuid.New())
	_, _ = store.GetMany(ctx, brain.Scope{}, []uuid.UUID{uuid.New()})
	if err := store.PutKind(ctx, brain.ObjectKind{Kind: "Document"}); err != nil {
		t.Fatal(err)
	}
	parent.End()

	parentID := parent.SpanContext().SpanID()
	var children int
	for _, sp := range sr.Ended() {
		if sp.Parent().SpanID() == parentID {
			children++
		}
	}
	if children < 3 {
		t.Fatalf("got %d postgres spans under tacklr.tool, want QueryRow+Query+Exec", children)
	}
}
