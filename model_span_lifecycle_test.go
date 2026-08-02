package tacklr

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// TestWatchModelStream_cancelEndsSpan: mid-stream cancel ends the model span
// with cancelled outcome/error class (not left open until process exit).
func TestWatchModelStream_cancelEndsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := telemetry.ContextWithTracer(context.Background(), tp.Tracer("test"))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	in := make(chan LLMResponseChunk)
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{Messages: 1})
	out := watchModelStream(ctx, span, telemetry.ModelPhaseTurn, in)

	// Producer holds the stream open (slow model).
	go func() {
		select {
		case in <- LLMResponseChunk{Type: StreamEventMessage, Content: "partial"}:
		case <-ctx.Done():
		}
		// Stay open until cancel; do not close until after cancel is observed.
		<-ctx.Done()
		close(in)
	}()

	// Consume first delta, then cancel the turn.
	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first chunk")
	}
	cancel()

	// out must close (watcher exit) and span must end.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				goto ended
			}
		case <-deadline:
			t.Fatal("watchModelStream did not close after cancel")
		}
	}
ended:
	// Allow span processor to record.
	time.Sleep(20 * time.Millisecond)
	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1 (got started=%d)", len(ended), len(sr.Started()))
	}
	st := ended[0]
	if st.Name() != telemetry.SpanModel {
		t.Fatalf("name = %s", st.Name())
	}
	if st.Status().Code != codes.Error {
		t.Fatalf("status = %v, want Error", st.Status())
	}
	attrs := map[string]string{}
	for _, a := range st.Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs[telemetry.AttrOutcome] != telemetry.OutcomeCancelled {
		t.Fatalf("outcome = %q, want cancelled; attrs=%v", attrs[telemetry.AttrOutcome], attrs)
	}
	if attrs[telemetry.AttrErrorClass] != telemetry.ErrorClassCancelled {
		t.Fatalf("error.class = %q, want cancelled; attrs=%v", attrs[telemetry.AttrErrorClass], attrs)
	}
}

// TestWatchModelStream_streamErrorEndsSpanEvenIfConsumerStops: provider error
// ends the span even when the harness stops reading and the producer keeps
// trying to send more chunks (would previously block forever).
func TestWatchModelStream_streamErrorEndsSpanEvenIfConsumerStops(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := telemetry.ContextWithTracer(context.Background(), tp.Tracer("test"))
	in := make(chan LLMResponseChunk, 8)
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 2, telemetry.WindowShape{Messages: 3, ToolPairs: 1})
	out := watchModelStream(ctx, span, telemetry.ModelPhaseTurn, in)

	in <- LLMResponseChunk{Type: StreamEventMessage, Content: "hi"}
	in <- LLMResponseChunk{
		Type:    StreamEventError,
		Content: "provider boom",
		Error:   errors.New("provider boom"),
	}
	// Extra chunks after error: consumer will not drain out fully.
	for i := 0; i < 32; i++ {
		in <- LLMResponseChunk{Type: StreamEventMessage, Content: "noise"}
	}
	close(in)

	// Read only the first two (message + error), then abandon out — simulates
	// agent_run modelFailed early return.
	got := 0
	timeout := time.After(2 * time.Second)
	for got < 2 {
		select {
		case _, ok := <-out:
			if !ok {
				t.Fatal("out closed before error chunk delivered")
			}
			got++
		case <-timeout:
			t.Fatal("timeout reading error path")
		}
	}

	// Span must end without anyone reading the rest of out.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(sr.Ended()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(sr.Ended()) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(sr.Ended()))
	}
	st := sr.Ended()[0]
	attrs := map[string]string{}
	for _, a := range st.Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs[telemetry.AttrOutcome] != telemetry.OutcomeError {
		t.Fatalf("outcome = %q, want error", attrs[telemetry.AttrOutcome])
	}
	if attrs[telemetry.AttrErrorClass] != telemetry.ErrorClassOther {
		t.Fatalf("error.class = %q", attrs[telemetry.AttrErrorClass])
	}
	if st.Status().Code != codes.Error {
		t.Fatalf("status = %v", st.Status())
	}
}

// TestEndModelSpan_cancelledOutcome is the direct end helper contract for cancel.
func TestEndModelSpan_cancelledOutcome(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := telemetry.ContextWithTracer(context.Background(), tp.Tracer("test"))
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{})
	telemetry.EndModelSpan(ctx, span, telemetry.ModelPhaseTurn, context.Canceled, 0, "", telemetry.TokenUsage{})

	if len(sr.Ended()) != 1 {
		t.Fatalf("ended = %d", len(sr.Ended()))
	}
	attrs := map[string]string{}
	for _, a := range sr.Ended()[0].Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs[telemetry.AttrOutcome] != telemetry.OutcomeCancelled {
		t.Fatalf("outcome=%q attrs=%v", attrs[telemetry.AttrOutcome], attrs)
	}
	if attrs[telemetry.AttrErrorClass] != telemetry.ErrorClassCancelled {
		t.Fatalf("class=%q", attrs[telemetry.AttrErrorClass])
	}
}
