// Package otlpjson sends spans to an OTLP/HTTP endpoint in OTLP's JSON
// encoding (POST {endpoint}/v1/traces, application/json), over net/http
// alone. It is the exporter of a program that cannot carry the OTLP
// exporters' protobuf and gRPC code, such as Tacklr on Cloudflare Workers
// (js/wasm), where net/http sends through fetch. Install sets up the
// process-wide tracer with it.
package otlpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Exporter is an sdktrace.SpanExporter to one OTLP/HTTP endpoint.
type Exporter struct {
	url     string
	headers map[string]string
	client  *http.Client
}

// New exports to endpoint (the OTLP/HTTP base URL, such as
// https://otlp.example.com/otlp), with headers on each request (an API
// key). A nil client is http.DefaultClient.
func New(endpoint string, headers map[string]string, client *http.Client) *Exporter {
	if client == nil {
		client = http.DefaultClient
	}
	return &Exporter{url: strings.TrimSuffix(endpoint, "/") + "/v1/traces", headers: headers, client: client}
}

// ExportSpans implements sdktrace.SpanExporter: one request for the batch.
func (e *Exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	body, _ := json.Marshal(encode(spans)) // strings, numbers and lists only
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("otlpjson: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("otlpjson: %s: %s", resp.Status, msg)
	}
	return nil
}

// Shutdown implements sdktrace.SpanExporter. The exporter holds nothing.
func (e *Exporter) Shutdown(context.Context) error { return nil }

// Install installs the process-wide tracer provider, exporting through e
// in batches, with the W3C propagator and parent-based sampling, as
// export.Init does for a host process. It returns the provider: Flush it
// when the platform may stop the program, and Shutdown it at the end.
func Install(e *Exporter, service string) *sdktrace.TracerProvider {
	res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", service)))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(e, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp
}

// Headers parses OTEL_EXPORTER_OTLP_HEADERS ("k1=v1,k2=v2", values
// URL-encoded, as the OpenTelemetry environment variables give them).
func Headers(s string) map[string]string {
	h := map[string]string{}
	for pair := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(k) == "" {
			continue
		}
		if unescaped, err := url.QueryUnescape(v); err == nil {
			v = unescaped
		}
		h[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return h
}

// The OTLP JSON encoding: the protobuf JSON mapping, with trace and span
// ids in hex and 64-bit integers as strings.
type (
	request struct {
		ResourceSpans []resourceSpans `json:"resourceSpans"`
	}
	resourceSpans struct {
		Resource   resourceJSON `json:"resource"`
		ScopeSpans []scopeSpans `json:"scopeSpans"`
	}
	resourceJSON struct {
		Attributes []keyValue `json:"attributes,omitempty"`
	}
	scopeSpans struct {
		Scope scope      `json:"scope"`
		Spans []spanJSON `json:"spans"`
	}
	scope struct {
		Name    string `json:"name"`
		Version string `json:"version,omitempty"`
	}
	spanJSON struct {
		TraceID      string      `json:"traceId"`
		SpanID       string      `json:"spanId"`
		ParentSpanID string      `json:"parentSpanId,omitempty"`
		Name         string      `json:"name"`
		Kind         int         `json:"kind"`
		Start        string      `json:"startTimeUnixNano"`
		End          string      `json:"endTimeUnixNano"`
		Attributes   []keyValue  `json:"attributes,omitempty"`
		Events       []eventJSON `json:"events,omitempty"`
		Links        []linkJSON  `json:"links,omitempty"`
		Status       statusJSON  `json:"status"`
	}
	eventJSON struct {
		Time       string     `json:"timeUnixNano"`
		Name       string     `json:"name"`
		Attributes []keyValue `json:"attributes,omitempty"`
	}
	linkJSON struct {
		TraceID    string     `json:"traceId"`
		SpanID     string     `json:"spanId"`
		Attributes []keyValue `json:"attributes,omitempty"`
	}
	statusJSON struct {
		Code    int    `json:"code,omitempty"`
		Message string `json:"message,omitempty"`
	}
	keyValue struct {
		Key   string   `json:"key"`
		Value anyValue `json:"value"`
	}
	anyValue struct {
		String *string     `json:"stringValue,omitempty"`
		Bool   *bool       `json:"boolValue,omitempty"`
		Int    *string     `json:"intValue,omitempty"`
		Double *float64    `json:"doubleValue,omitempty"`
		Array  *arrayValue `json:"arrayValue,omitempty"`
	}
	arrayValue struct {
		Values []anyValue `json:"values"`
	}
)

// encode groups spans by their resource, then by their scope.
func encode(spans []sdktrace.ReadOnlySpan) request {
	var out request
	resources := map[*resource.Resource]int{}
	scopes := map[*resource.Resource]map[instrumentation.Scope]int{}
	for _, s := range spans {
		res := s.Resource()
		ri, ok := resources[res]
		if !ok {
			ri = len(out.ResourceSpans)
			resources[res] = ri
			scopes[res] = map[instrumentation.Scope]int{}
			out.ResourceSpans = append(out.ResourceSpans, resourceSpans{Resource: resourceJSON{Attributes: attributes(res.Attributes())}})
		}
		sc := s.InstrumentationScope()
		si, ok := scopes[res][sc]
		if !ok {
			si = len(out.ResourceSpans[ri].ScopeSpans)
			scopes[res][sc] = si
			out.ResourceSpans[ri].ScopeSpans = append(out.ResourceSpans[ri].ScopeSpans, scopeSpans{Scope: scope{Name: sc.Name, Version: sc.Version}})
		}
		ss := &out.ResourceSpans[ri].ScopeSpans[si]
		ss.Spans = append(ss.Spans, span(s))
	}
	return out
}

func span(s sdktrace.ReadOnlySpan) spanJSON {
	sc := s.SpanContext()
	out := spanJSON{
		TraceID: sc.TraceID().String(),
		SpanID:  sc.SpanID().String(),
		Name:    s.Name(),
		// OpenTelemetry Go's span kinds have OTLP's numbers.
		Kind:       int(s.SpanKind()),
		Start:      nanos(s.StartTime()),
		End:        nanos(s.EndTime()),
		Attributes: attributes(s.Attributes()),
		Status:     statusJSON{Code: statusCode(s.Status().Code), Message: s.Status().Description},
	}
	if p := s.Parent(); p.IsValid() {
		out.ParentSpanID = p.SpanID().String()
	}
	for _, ev := range s.Events() {
		out.Events = append(out.Events, eventJSON{Time: nanos(ev.Time), Name: ev.Name, Attributes: attributes(ev.Attributes)})
	}
	for _, l := range s.Links() {
		out.Links = append(out.Links, linkJSON{
			TraceID:    l.SpanContext.TraceID().String(),
			SpanID:     l.SpanContext.SpanID().String(),
			Attributes: attributes(l.Attributes),
		})
	}
	return out
}

// statusCode is OTLP's status code: 0 unset, 1 ok, 2 error.
func statusCode(c codes.Code) int {
	switch c {
	case codes.Ok:
		return 1
	case codes.Error:
		return 2
	}
	return 0
}

func nanos(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func attributes(kvs []attribute.KeyValue) []keyValue {
	out := make([]keyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, keyValue{Key: string(kv.Key), Value: value(kv.Value)})
	}
	return out
}

func value(v attribute.Value) anyValue {
	switch v.Type() {
	case attribute.BOOL:
		b := v.AsBool()
		return anyValue{Bool: &b}
	case attribute.INT64:
		i := strconv.FormatInt(v.AsInt64(), 10)
		return anyValue{Int: &i}
	case attribute.FLOAT64:
		f := v.AsFloat64()
		return anyValue{Double: &f}
	case attribute.BOOLSLICE, attribute.INT64SLICE, attribute.FLOAT64SLICE, attribute.STRINGSLICE:
		var values []anyValue
		for _, e := range sliceValues(v) {
			values = append(values, value(e))
		}
		return anyValue{Array: &arrayValue{Values: values}}
	}
	s := v.String()
	return anyValue{String: &s}
}

// sliceValues is a slice attribute's elements as values.
func sliceValues(v attribute.Value) []attribute.Value {
	var out []attribute.Value
	switch v.Type() {
	case attribute.BOOLSLICE:
		for _, b := range v.AsBoolSlice() {
			out = append(out, attribute.BoolValue(b))
		}
	case attribute.INT64SLICE:
		for _, i := range v.AsInt64Slice() {
			out = append(out, attribute.Int64Value(i))
		}
	case attribute.FLOAT64SLICE:
		for _, f := range v.AsFloat64Slice() {
			out = append(out, attribute.Float64Value(f))
		}
	default:
		for _, s := range v.AsStringSlice() {
			out = append(out, attribute.StringValue(s))
		}
	}
	return out
}
