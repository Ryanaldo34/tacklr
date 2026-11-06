package acp_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/livesess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// tracedMCPServer is an MCP server over HTTP with one tool, search, that
// sends the trace id of each request it gets on traces.
func tracedMCPServer(t *testing.T) (string, <-chan trace.TraceID) {
	t.Helper()
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "kb", Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "search", Description: "Searches the knowledge base."},
		func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
			return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "found"}}}, struct{}{}, nil
		})
	traces := make(chan trace.TraceID, 64)
	mcpHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sc := trace.SpanContextFromContext(telemetry.FromHeaders(r.Context(), r.Header)); sc.IsValid() {
			select {
			case traces <- sc.TraceID():
			default:
			}
		}
		mcpHandler.ServeHTTP(w, r)
	}))
	// The session's harness keeps its MCP connection open after the test.
	t.Cleanup(func() {
		hs.CloseClientConnections()
		hs.Close()
	})
	return hs.URL, traces
}

// TestACP_aTurnIsOneTraceThroughEverySystemItTouches: on Temporal and on
// Durable Objects, one prompt is one trace from the client's traceparent
// to the answer: the protocol request; the turn and its resume, which
// links the question the user answered; each model request and tool call;
// the MCP server the tool reached, which got the trace; the specialist's
// turn in its own session; and, on Temporal, the Postgres statements of
// the turn's steps. Every span's parent is a span of the trace.
func TestACP_aTurnIsOneTraceThroughEverySystemItTouches(t *testing.T) {
	t.Parallel()
	deploy := tacklr.NewTool(tacklr.ToolConfig{
		Name: "deploy", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) { return "deployed", nil },
	})
	lookup := tacklr.NewTool(tacklr.ToolConfig{
		Name: "lookup", Handler: func(context.Context) (string, error) { return "42", nil },
	})
	call := func(ch chan<- tacklr.LLMResponseChunk, tc message.ToolCall) {
		tc.CallID = tc.ID
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{tc}}
	}
	say := func(ch chan<- tacklr.LLMResponseChunk, text string) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: text, IsComplete: true}
	}
	// The agent deploys (it asks first), searches the knowledge base over
	// MCP, and has the researcher look the answer up.
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		switch {
		case last.Role == message.RoleUser && strings.Contains(last.Content, "look it up"):
			call(ch, message.ToolCall{ID: "c-lookup", Name: "lookup", Arguments: `{}`})
		case last.Role == message.RoleUser:
			call(ch, message.ToolCall{ID: "c-deploy", Name: "deploy", Arguments: `{}`})
		case last.Content == "deployed":
			call(ch, message.ToolCall{ID: "c-search", Name: "search", Namespace: "kb", Arguments: `{}`})
		case last.Content == "found":
			call(ch, message.ToolCall{ID: "c-spawn", Name: "spawn_specialist", Arguments: `{"specialist":"researcher","task_description_and_context":"look it up","block":true}`})
		case last.Content == "42":
			say(ch, "the answer is 42")
		default:
			say(ch, "all done")
		}
	})
	agent := tacklr.AgentOptions{
		Name: "releases", Model: model, MaxWindowSize: 8192, SystemPrompt: "test prompt", MCP: mcp.Client{},
		Tools:       []*tacklr.Tool{deploy},
		Specialists: []*tacklr.Specialist{{Name: "researcher", Description: "Looks things up.", Instructions: "You look up.", Model: model, Tools: []*tacklr.Tool{lookup}}},
	}
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	temporalRT := acptest.NewRuntime(t, model, agent)
	objects := durableobject.NewClient(dotest.NewWire(durableobject.Config{Agent: agent, Sealer: sealer}))
	allow := func(req map[string]any) any {
		return map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": "allow-once"}}}
	}

	for name, rt := range map[string]struct {
		r        *acptest.Runtime
		wire     server.ProtocolWireStore
		database bool
	}{
		"temporal":        {temporalRT, nil, true},
		"durable objects": {&acptest.Runtime{Client: objects, Agent: agent}, objects.WireStore(), false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			mcpURL, mcpTraces := tracedMCPServer(t)
			c := acptest.Connect(t, acptest.Listen(t, acptest.Server(rt.r, rt.wire)), `{"protocolVersion":1}`)
			sid := acptest.SessionID(t, c.Call("session/new", `{"cwd":"/srv","mcpServers":[{"type":"http","name":"kb","url":"`+mcpURL+`","headers":[]}]}`))
			var traceID trace.TraceID
			var clientSpan trace.SpanID
			_, _ = rand.Read(traceID[:])
			_, _ = rand.Read(clientSpan[:])
			traceparent := "00-" + traceID.String() + "-" + clientSpan.String() + "-01"

			// Act
			end, _ := c.Until(c.Request("session/prompt", `{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"ship it"}],"_meta":{"traceparent":"`+traceparent+`"}}`), allow)
			// The answer can reach the client a moment before its turn's
			// span ends: wait for the spans the turn ends with.
			spans := traceOf(t, traceID, map[string]int{"session/prompt": 1, "invoke_agent releases": 2, "invoke_agent researcher": 1})

			// Assert
			if acptest.StopReason(end) != "end_turn" {
				t.Fatalf("prompt = %v, want end_turn", end)
			}
			names := map[string]int{}
			ids := map[trace.SpanID]sdktrace.ReadOnlySpan{}
			for _, s := range spans {
				names[s.Name()]++
				ids[s.SpanContext().SpanID()] = s
			}
			for _, s := range spans {
				if strings.HasPrefix(s.Name(), telemetry.OperationChat+" ") {
					names[telemetry.OperationChat]++
				}
			}
			for _, want := range []string{"session/prompt", "invoke_agent releases", "session/request_permission", telemetry.OperationChat, "execute_tool deploy", "execute_tool search", "execute_tool spawn_specialist", "invoke_agent researcher", "execute_tool lookup"} {
				if names[want] == 0 {
					t.Errorf("no %q span in the turn's trace; it has %v", want, names)
				}
			}
			if names["invoke_agent releases"] < 2 {
				t.Errorf("turn spans = %d, want the prompt's and the resume's", names["invoke_agent releases"])
			}
			for _, s := range spans {
				if p := s.Parent().SpanID(); p != clientSpan && ids[p] == nil {
					t.Errorf("span %q has parent %s, which is not in the trace", s.Name(), p)
				}
			}
			asked := slices.IndexFunc(spans, func(s sdktrace.ReadOnlySpan) bool { return s.Name() == "session/request_permission" })
			resumed := slices.IndexFunc(spans, func(s sdktrace.ReadOnlySpan) bool {
				return s.Name() == "invoke_agent releases" && len(s.Links()) == 1 && asked >= 0 && s.Links()[0].SpanContext.SpanID() == spans[asked].SpanContext().SpanID()
			})
			if resumed < 0 {
				t.Errorf("no resumed turn links the permission the user answered")
			}
			if !gotTrace(mcpTraces, traceID) {
				t.Errorf("the MCP server got no request in the turn's trace %s", traceID)
			}
			database := slices.ContainsFunc(spans, func(s sdktrace.ReadOnlySpan) bool {
				return slices.ContainsFunc(s.Attributes(), func(kv attribute.KeyValue) bool { return strings.HasPrefix(string(kv.Key), "db.") })
			})
			if database != rt.database {
				t.Errorf("database spans in the trace = %v, want %v", database, rt.database)
			}
		})
	}
}

// traceOf waits until the recorded spans of trace id have at least the
// count of each name of want, and returns them all.
func traceOf(t *testing.T, id trace.TraceID, want map[string]int) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var spans []sdktrace.ReadOnlySpan
		have := map[string]int{}
		for _, s := range livesess.Spans.Ended() {
			if s.SpanContext().TraceID() == id {
				spans = append(spans, s)
				have[s.Name()]++
			}
		}
		done := true
		for name, n := range want {
			done = done && have[name] >= n
		}
		if done || time.Now().After(deadline) {
			return spans
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// gotTrace reports whether traces has id among what it got so far.
func gotTrace(traces <-chan trace.TraceID, id trace.TraceID) bool {
	for {
		select {
		case got := <-traces:
			if got == id {
				return true
			}
		default:
			return false
		}
	}
}
