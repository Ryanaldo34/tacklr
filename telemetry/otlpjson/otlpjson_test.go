package otlpjson_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/telemetry/otlpjson"
)

// collector is an OTLP/HTTP endpoint that keeps each request's body and
// headers, and answers with status.
type collector struct {
	mu      sync.Mutex
	bodies  []map[string]any
	headers []http.Header
	status  int
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	c.mu.Lock()
	c.bodies = append(c.bodies, body)
	c.headers = append(c.headers, r.Header.Clone())
	c.mu.Unlock()
	if r.URL.Path != "/otlp/v1/traces" || r.Header.Get("Content-Type") != "application/json" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(c.status)
	_, _ = w.Write([]byte("refused"))
}

// spans are the spans of every body the collector got, by name.
func (c *collector) spans() map[string]map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]map[string]any{}
	for _, b := range c.bodies {
		rss, _ := b["resourceSpans"].([]any) // an empty batch has none
		for _, rs := range rss {
			for _, ss := range rs.(map[string]any)["scopeSpans"].([]any) {
				for _, s := range ss.(map[string]any)["spans"].([]any) {
					span := s.(map[string]any)
					out[span["name"].(string)] = span
				}
			}
		}
	}
	return out
}

// attr is the OTLP JSON value of the attribute key of span.
func attr(span map[string]any, key string) map[string]any {
	for _, a := range span["attributes"].([]any) {
		if kv := a.(map[string]any); kv["key"] == key {
			return kv["value"].(map[string]any)
		}
	}
	return nil
}

// TestExporter_sendsSpansToTheEndpointInOTLPJSON: the installed tracer
// sends each span to {endpoint}/v1/traces in OTLP's JSON encoding, with the
// endpoint's headers: ids in hex, times and integers as strings, each kind
// of attribute value, events, links, the parent, and the status. A child
// of a client's traceparent is in the client's trace. A refusal and an
// unreachable endpoint fail the export.
func TestExporter_sendsSpansToTheEndpointInOTLPJSON(t *testing.T) {
	// Arrange
	c := &collector{status: http.StatusOK}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	tp := otlpjson.Install(otlpjson.New(srv.URL+"/otlp/", otlpjson.Headers("Authorization=Bearer%20key, x-team = edge,broken"), srv.Client()), "tacklr-edge")
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer := otel.Tracer("test")
	client := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), client)
	linked := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{9}, SpanID: trace.SpanID{9}})

	// Act
	ctx, root := tracer.Start(ctx, "invoke_agent", trace.WithSpanKind(trace.SpanKindServer), trace.WithLinks(trace.Link{SpanContext: linked, Attributes: []attribute.KeyValue{attribute.String("why", "resume")}}))
	_, child := tracer.Start(ctx, "execute_tool", trace.WithAttributes(
		attribute.Bool("ok", true), attribute.Int("n", 7), attribute.Float64("f", 0.5), attribute.String("s", "x"),
		attribute.BoolSlice("bs", []bool{true}), attribute.IntSlice("is", []int{1}), attribute.Float64Slice("fs", []float64{1.5}), attribute.StringSlice("ss", []string{"a"}),
	))
	child.AddEvent("parked", trace.WithAttributes(attribute.String("kind", "permission")))
	child.SetStatus(codes.Error, "boom")
	child.End()
	root.SetStatus(codes.Ok, "")
	root.End()
	_, unset := tracer.Start(context.Background(), "chat")
	unset.End()
	flushErr := tp.ForceFlush(context.Background())
	c.status = http.StatusBadRequest
	refused := otlpjson.New(srv.URL+"/otlp", nil, nil).ExportSpans(context.Background(), nil)
	unreachable := otlpjson.New("http://127.0.0.1:1", nil, nil).ExportSpans(context.Background(), nil)
	badURL := otlpjson.New("http://bad host", nil, nil).ExportSpans(context.Background(), nil)

	// Assert
	spans := c.spans()
	root2, tool, chat := spans["invoke_agent"], spans["execute_tool"], spans["chat"]
	if flushErr != nil || root2 == nil || tool == nil || chat == nil {
		t.Fatalf("flush = %v, spans = %v; want the three spans sent", flushErr, spans)
	}
	if root2["traceId"] != "4bf92f3577b34da6a3ce929d0e0e4736" || root2["parentSpanId"] != "00f067aa0ba902b7" || root2["kind"] != float64(2) ||
		root2["status"].(map[string]any)["code"] != float64(1) {
		t.Fatalf("root = %v, want a server span in the client's trace, under its span, ok", root2)
	}
	if links := root2["links"].([]any); len(links) != 1 || links[0].(map[string]any)["traceId"] != linked.TraceID().String() {
		t.Fatalf("links = %v, want the link to the resumer", root2["links"])
	}
	if tool["parentSpanId"] != root2["spanId"] || tool["status"].(map[string]any)["code"] != float64(2) || tool["status"].(map[string]any)["message"] != "boom" {
		t.Fatalf("tool = %v, want a failed child of the root", tool)
	}
	if attr(tool, "ok")["boolValue"] != true || attr(tool, "n")["intValue"] != "7" || attr(tool, "f")["doubleValue"] != 0.5 || attr(tool, "s")["stringValue"] != "x" ||
		attr(tool, "ss")["arrayValue"] == nil || attr(tool, "bs")["arrayValue"] == nil || attr(tool, "is")["arrayValue"] == nil || attr(tool, "fs")["arrayValue"] == nil {
		t.Fatalf("attributes = %v, want each kind of value", tool["attributes"])
	}
	if events := tool["events"].([]any); len(events) != 1 || events[0].(map[string]any)["name"] != "parked" {
		t.Fatalf("events = %v, want the park", tool["events"])
	}
	if _, ok := chat["status"].(map[string]any)["code"]; ok || !strings.HasPrefix(chat["startTimeUnixNano"].(string), "1") {
		t.Fatalf("chat = %v, want an unset status and a time in nanoseconds", chat)
	}
	if h := c.headers[0]; h.Get("Authorization") != "Bearer key" || h.Get("X-Team") != "edge" {
		t.Fatalf("headers = %v, want the endpoint's headers", h)
	}
	if refused == nil || !strings.Contains(refused.Error(), "refused") || unreachable == nil || badURL == nil {
		t.Fatalf("refused = %v, unreachable = %v, bad URL = %v; want each to fail", refused, unreachable, badURL)
	}
	if err := otlpjson.New(srv.URL, nil, nil).Shutdown(context.Background()); err != nil || errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown = %v, want nil", err)
	}
}
