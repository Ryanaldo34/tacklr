package postgres_test

import (
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
)

func TestStore_sqlSpansParentUnderTheCallersSpan(t *testing.T) {
	schema := testSchema(t)
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	store, err := postgres.Open(t.Context(), pgConn, postgres.Options{Schema: schema, EmbeddingDim: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, parent := tp.Tracer("test").Start(t.Context(), "tacklr.tool")
	scope := brain.Scope{Namespace: brain.Namespace{{Name: "org", Value: "acme"}}}
	doc := uuid.New()
	part := brain.TreePart{Object: brain.Object{ID: uuid.New(), Kind: "Chunk", ParentID: &doc, Namespace: scope.Namespace}}

	_, _ = store.Get(ctx, scope, uuid.New())
	_, _ = store.GetMany(ctx, scope, []uuid.UUID{uuid.New()})
	putErr := store.PutKind(ctx, brain.ObjectKind{Kind: "Document"})
	stageErr := store.StageParts(ctx, scope, uuid.New(), []brain.TreePart{part})
	parent.End()

	var children int
	for _, sp := range sr.Ended() {
		if sp.Parent().SpanID() == parent.SpanContext().SpanID() {
			children++
		}
	}
	if putErr != nil || stageErr != nil || children < 4 {
		t.Fatalf("got %d postgres spans under tacklr.tool (put %v, stage %v), want QueryRow+Query+Exec+CopyFrom", children, putErr, stageErr)
	}
}
