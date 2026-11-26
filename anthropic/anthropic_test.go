package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/anthropic"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// api is a Messages API that answers each /messages request with the
// next stream and keeps every request body. Its count endpoint answers 7.
type api struct {
	mu       sync.Mutex
	streams  []string
	requests []map[string]any
	header   http.Header
}

func newAPI(t *testing.T, streams ...string) (*anthropic.Model, *api) {
	t.Helper()
	a := &api{streams: streams}
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/count_tokens") {
			_, _ = io.WriteString(w, `{"input_tokens":7}`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		a.mu.Lock()
		a.requests = append(a.requests, body)
		a.header = r.Header.Clone()
		stream := a.streams[0]
		a.streams = a.streams[1:]
		a.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, stream)
	}))
	return anthropic.New(srv.Client()).WithApiKey("key").WithModel("claude-opus-5-5").WithURL(srv.URL), a
}

// sse is a stream of events, each an event line and a data line.
func sse(events ...string) string {
	var b strings.Builder
	for _, ev := range events {
		var head struct{ Type string }
		_ = json.Unmarshal([]byte(ev), &head)
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", head.Type, ev)
	}
	return b.String()
}

const (
	start = `{"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"cache_read_input_tokens":100,"cache_creation_input_tokens":5}}}`
	stop  = `{"type":"message_stop"}`
)

func usage(stopReason string) string {
	return `{"type":"message_delta","delta":{"stop_reason":"` + stopReason + `"},"usage":{"output_tokens":3}}`
}

// textReply is a stream that answers text.
func textReply(text string) string {
	return sse(start,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+quote(text)+`}}`,
		`{"type":"content_block_stop","index":0}`,
		usage("end_turn"), stop)
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func collect(t *testing.T, ch chan tacklr.LLMResponseChunk) []tacklr.LLMResponseChunk {
	t.Helper()
	var out []tacklr.LLMResponseChunk
	for c := range ch {
		out = append(out, c)
	}
	return out
}

func hello() tacklr.ModelRequest {
	return tacklr.ModelRequest{Messages: []*message.Message{{Role: message.RoleUser, Content: "hi"}}}
}

// TestModel_capabilitiesFollowTheModelID: every model takes text, the four
// image types and PDF. A Claude model id, also behind a gateway's prefix,
// has a 1M-token window, or 200K on an older model. Another id, such as a
// deployment name, has no known window unless the host sets one.
func TestModel_capabilitiesFollowTheModelID(t *testing.T) {
	t.Parallel()
	mimes := []string{"", "text/markdown", "image/png", "image/webp", "application/pdf", "application/zip"}
	for _, tc := range []struct {
		model, window string
		set           int
		supports      []bool
	}{
		{model: "claude-opus-5-5", window: "1000000 <nil>", supports: []bool{true, true, true, true, true, false}},
		{model: "claude-haiku-4-5", window: "200000 <nil>", supports: []bool{true, true, true, true, true, false}},
		{model: "gateway/claude-sonnet-5-5", window: "1000000 <nil>", supports: []bool{true, true, true, true, true, false}},
		{model: "my-deployment", window: `0 max context window: unknown model "my-deployment": unknown model`, supports: []bool{true, true, true, true, true, false}},
		{model: "my-deployment", set: 8000, window: "8000 <nil>", supports: []bool{true, true, true, true, true, false}},
	} {
		m := anthropic.New(nil).WithModel(tc.model).WithContextWindow(tc.set)
		n, err := m.MaxContextWindow()
		supports := make([]bool, len(mimes))
		for i, mime := range mimes {
			supports[i] = m.SupportsMIME(mime)
		}

		if got := fmt.Sprint(n, " ", err); got != tc.window || !slices.Equal(supports, tc.supports) || m.ModelTelemetryIdentity() != (telemetry.ModelIdentity{Provider: "anthropic", Model: tc.model}) {
			t.Errorf("%s (window %d): window %s, supports %v of %v; want %s and %v", tc.model, tc.set, got, supports, mimes, tc.window, tc.supports)
		}
	}
}

// TestInvoke_requiresKeyAndModel: a request without an API key or a model
// fails before it reaches the API.
func TestInvoke_requiresKeyAndModel(t *testing.T) {
	t.Parallel()
	m := anthropic.New(http.DefaultClient)
	_, noKey := m.Invoke(t.Context(), hello())
	_, countNoKey := m.CountTokens(t.Context(), hello())
	m.WithApiKey("k")
	_, noModel := m.Invoke(t.Context(), hello())
	_, countNoModel := m.CountTokens(t.Context(), hello())

	if !errors.Is(noKey, tacklr.ErrApiKeyNotSet) || !errors.Is(countNoKey, tacklr.ErrApiKeyNotSet) ||
		!errors.Is(noModel, tacklr.ErrModelNotSet) || !errors.Is(countNoModel, tacklr.ErrModelNotSet) {
		t.Fatalf("no key = %v, count with no key = %v, no model = %v, count with no model = %v", noKey, countNoKey, noModel, countNoModel)
	}
}

// sent is the body of the one request m sent for req.
func sent(ctx context.Context, t *testing.T, m *anthropic.Model, a *api, req tacklr.ModelRequest) map[string]any {
	t.Helper()
	collect(t, must(m.Invoke(ctx, req)))
	return a.requests[len(a.requests)-1]
}

// same reports whether got, decoded from JSON, is the JSON want.
func same(got any, want string) bool {
	var w any
	_ = json.Unmarshal([]byte(want), &w)
	return reflect.DeepEqual(got, w)
}

// turnsOf are the request's messages as decoded JSON.
func turnsOf(body map[string]any) []map[string]any {
	var out []map[string]any
	for _, t := range body["messages"].([]any) {
		out = append(out, t.(map[string]any))
	}
	return out
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestInvoke_cachesTheSystemPromptAndTheConversation(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{SystemPrompt: "be brief", Messages: hello().Messages})

	if !same(body["system"], `[{"cache_control":{"type":"ephemeral"},"text":"be brief","type":"text"}]`) || !same(body["cache_control"], `{"type":"ephemeral"}`) {
		t.Errorf("system = %v, cache_control = %v; want both cached", body["system"], body["cache_control"])
	}
}

func TestInvoke_sendsUserMediaAsBlocksBeforeTheText(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))
	look := &message.Message{Role: message.RoleUser, Content: "look", ContentParts: []message.ContentPart{
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "data:image/png;base64,iVBO"}},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "https://example.com/cat.png"}},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "data:image/png,raw"}},
		{Type: message.ContentTypeInputImage},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{Data: "JVBE", MIMEType: "application/pdf"}},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{FileID: "file_1"}},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{}},
		{Type: message.ContentTypeOutputText, Text: "not media"},
	}}

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{Messages: []*message.Message{nil, look}})

	want := `[{"source":{"data":"iVBO","media_type":"image/png","type":"base64"},"type":"image"},
		{"source":{"type":"url","url":"https://example.com/cat.png"},"type":"image"},
		{"source":{"data":"JVBE","media_type":"application/pdf","type":"base64"},"type":"document"},
		{"source":{"file_id":"file_1","type":"file"},"type":"document"},
		{"text":"look","type":"text"}]`
	if got := turnsOf(body)[0]["content"]; !same(got, want) {
		t.Errorf("user content = %v, want the usable media, then the text", got)
	}
}

func TestInvoke_sendsDeveloperAndSystemMessagesAsTaggedUserText(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{Messages: []*message.Message{
		{Role: message.RoleUser, Content: "hi"},
		{Role: message.RoleDeveloper, Content: "keep going"},
		{Role: message.RoleDeveloper, Content: "  "},
		{Role: message.RoleSystem, Content: "note"},
	}})

	want := `[{"role":"user","content":[{"text":"hi","type":"text"},
		{"text":"<system-reminder>\nkeep going\n</system-reminder>","type":"text"},
		{"text":"<system-reminder>\nnote\n</system-reminder>","type":"text"}]}]`
	if !same(body["messages"], want) {
		t.Errorf("messages = %v, want one user turn with the tagged text", body["messages"])
	}
}

func TestInvoke_cachesThePlan(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{Messages: []*message.Message{
		{Role: message.RoleUser, Content: "hi"},
		{Role: message.RoleDeveloper, Content: "PROJECT PLAN\n────────────\nship it"},
	}})

	plan := turnsOf(body)[0]["content"].([]any)[1]
	if !same(plan, `{"cache_control":{"type":"ephemeral"},"text":"<system-reminder>\nPROJECT PLAN\n────────────\nship it\n</system-reminder>","type":"text"}`) {
		t.Errorf("plan block = %v, want a cached system reminder", plan)
	}
}

func TestInvoke_replaysClaudeThinkingAndLeavesOutOtherReasoning(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{Messages: []*message.Message{
		{Role: message.RoleUser, Content: "hi"},
		{Role: message.RoleReasoning, Content: "hmm", EncryptedContent: `{"type":"thinking","thinking":"hmm","signature":"sig"}`},
		{Role: message.RoleReasoning, Content: "other", EncryptedContent: "gAAAAcipher"},
		{Role: message.RoleAssistant, Content: "checking"},
	}})

	if got := turnsOf(body)[1]["content"]; !same(got, `[{"signature":"sig","thinking":"hmm","type":"thinking"},{"text":"checking","type":"text"}]`) {
		t.Errorf("assistant content = %v, want the thinking as sent, then the text", got)
	}
}

func TestInvoke_sendsAReplysCallsAsOneTurnAndTheirResultsFirstInTheNext(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{Messages: []*message.Message{
		{Role: message.RoleUser, Content: "hi"},
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{
			{ID: "toolu_1", Name: "greet", Namespace: "ado", Arguments: `{"name":"x"}`},
			{ID: "toolu_2", Name: "get", Arguments: "not json"},
		}},
		{Role: message.RoleTool, ToolCallID: "toolu_ghost", Content: "lost"},
		{Role: message.RoleTool, ToolCallID: "toolu_1", Content: "ok"},
		{Role: message.RoleDeveloper, Content: "keep going"},
		{Role: message.RoleTool, ToolCallID: "toolu_2", Content: "ok2"},
	}})

	turns := turnsOf(body)
	calls := `[{"id":"toolu_1","input":{"name":"x"},"name":"ado__greet","type":"tool_use"},{"id":"toolu_2","input":{},"name":"get","type":"tool_use"}]`
	results := `[{"content":"ok","tool_use_id":"toolu_1","type":"tool_result"},{"content":"ok2","tool_use_id":"toolu_2","type":"tool_result"},
		{"text":"<system-reminder>\nkeep going\n</system-reminder>","type":"text"}]`
	if len(turns) != 3 || !same(turns[1]["content"], calls) || !same(turns[2]["content"], results) {
		t.Errorf("turns = %v, want the calls in one turn and their results first in the next", turns)
	}
}

func TestInvoke_sendsToolsWithTheirSchemas(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))
	greet := tacklr.NewTool(tacklr.ToolConfig{Name: "greet", Namespace: "ado", Description: "Greet", Handler: func(context.Context, struct {
		Name string `json:"name"`
	}) (string, error) {
		return "", nil
	}})

	body := sent(t.Context(), t, m, a, tacklr.ModelRequest{Tools: []*tacklr.Tool{greet}, Messages: hello().Messages})

	want := `[{"description":"Greet","name":"ado__greet","input_schema":{"additionalProperties":false,"properties":{"name":{"type":"string"}},"required":["name"],"type":"object"}}]`
	if !same(body["tools"], want) {
		t.Errorf("tools = %v, want the namespaced tool with its schema", body["tools"])
	}
}

func TestInvoke_aTurnWithoutToolCallsSendsToolChoiceNone(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"), textReply("b"))
	get := tacklr.NewTool(tacklr.ToolConfig{Name: "get", Handler: func(context.Context) (string, error) { return "", nil }})
	ctx := tacklr.ContextWithToolChoiceNone(t.Context())

	withTools := sent(ctx, t, m, a, tacklr.ModelRequest{Tools: []*tacklr.Tool{get}, Messages: hello().Messages})
	withoutTools := sent(ctx, t, m, a, hello())

	if !same(withTools["tool_choice"], `{"type":"none"}`) || withoutTools["tool_choice"] != nil {
		t.Errorf("tool_choice with tools = %v, without = %v; want none, and none sent without tools", withTools["tool_choice"], withoutTools["tool_choice"])
	}
}

func TestInvoke_asksAnAdaptiveModelToThinkAtItsEffort(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m.WithEffort("high"), a, hello())

	if !same(body["thinking"], `{"display":"summarized","type":"adaptive"}`) || !same(body["output_config"], `{"effort":"high"}`) || body["max_tokens"] != 64000.0 {
		t.Errorf("thinking %v, output_config %v, max_tokens %v; want summarized adaptive thinking at high effort and the default limit", body["thinking"], body["output_config"], body["max_tokens"])
	}
}

func TestInvoke_sendsAnOlderModelNoThinkingAndItsMaxTokens(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	body := sent(t.Context(), t, m.WithModel("claude-haiku-4-5").WithMaxTokens(1000), a, hello())

	if _, thinks := body["thinking"]; thinks || body["output_config"] != nil || body["max_tokens"] != 1000.0 || body["model"] != "claude-haiku-4-5" {
		t.Errorf("request = %v, want no thinking or effort and 1000 max tokens", body)
	}
}

func TestInvoke_authenticatesWithTheKeyAndTheAPIVersion(t *testing.T) {
	t.Parallel()
	m, a := newAPI(t, textReply("a"))

	sent(t.Context(), t, m, a, hello())

	if a.header.Get("X-Api-Key") != "key" || a.header.Get("Anthropic-Version") != "2026-10-21" {
		t.Errorf("headers = %v, want the key and the API version", a.header)
	}
}

// TestInvoke_streamsAReplyAsChunks: text and thinking stream as deltas and
// complete with their whole text; a thinking block completes with its
// JSON for replay, a redacted one too; a tool call completes with its
// streamed input, its namespace split from its name, or an empty object.
// Empty text and server blocks send nothing, and the reply completes with
// its usage, cached input counted as input.
func TestInvoke_streamsAReplyAsChunks(t *testing.T) {
	t.Parallel()
	m, _ := newAPI(t, sse(start,
		`{"type":"ping"}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me "}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"see"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"on it"}}`,
		`{"type":"content_block_delta","index":9,"delta":{"type":"text_delta","text":"no block"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"redacted_thinking","data":"cipher"}}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"toolu_1","name":"ado__greet"}}`,
		`{"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"{\"name\":"}}`,
		`{"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"\"x\"}"}}`,
		`{"type":"content_block_stop","index":3}`,
		`{"type":"content_block_start","index":4,"content_block":{"type":"tool_use","id":"toolu_2","name":"get"}}`,
		`{"type":"content_block_stop","index":4}`,
		`{"type":"content_block_start","index":5,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_stop","index":5}`,
		`{"type":"content_block_start","index":6,"content_block":{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search"}}`,
		`{"type":"content_block_stop","index":6}`,
		`{"type":"content_block_stop","index":7}`,
		usage("tool_use"), stop))

	got := collect(t, must(m.Invoke(t.Context(), hello())))

	want := []tacklr.LLMResponseChunk{
		{Type: session.EventReasoning, MessageId: "msg_1:0", Content: "let me "},
		{Type: session.EventReasoning, MessageId: "msg_1:0", Content: "see"},
		{Type: session.EventReasoning, MessageId: "msg_1:0", Content: "let me see", IsComplete: true, EncryptedContent: `{"type":"thinking","thinking":"let me see","signature":"sig"}`},
		{Type: session.EventMessage, MessageId: "msg_1:1", Content: "on it"},
		{Type: session.EventMessage, MessageId: "msg_1:1", Content: "on it", IsComplete: true},
		{Type: session.EventReasoning, MessageId: "msg_1:2", IsComplete: true, EncryptedContent: `{"type":"redacted_thinking","data":"cipher"}`},
		{Type: session.EventFunctionCall, MessageId: "msg_1:3", IsComplete: true, ToolCalls: []message.ToolCall{{ID: "toolu_1", Type: "tool_use", CallID: "toolu_1", Name: "greet", Namespace: "ado", Arguments: `{"name":"x"}`, Status: "completed"}}},
		{Type: session.EventFunctionCall, MessageId: "msg_1:4", IsComplete: true, ToolCalls: []message.ToolCall{{ID: "toolu_2", Type: "tool_use", CallID: "toolu_2", Name: "get", Arguments: `{}`, Status: "completed"}}},
		{Type: session.EventComplete, IsComplete: true, InputTokens: 115, OutputTokens: 3, CachedTokens: 100, CacheWriteTokens: 5},
	}
	if g, w := fmt.Sprintf("%+v", got), fmt.Sprintf("%+v", want); g != w {
		t.Errorf("chunks =\n%s\nwant\n%s", g, w)
	}
}

// TestHarness_replaysAToolRoundWithItsThinkingInOrder: a reply that thinks
// between its text and its tool call, run by the harness, replays on the
// next request as one assistant turn in the order the model wrote it,
// each thinking block as the API sent it, then the call's result.
func TestHarness_replaysAToolRoundWithItsThinkingInOrder(t *testing.T) {
	t.Parallel()
	// Arrange
	m, a := newAPI(t, sse(start,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"plan"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"s1"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":"I will greet."}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"thinking","thinking":""}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"signature_delta","signature":"s2"}}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"toolu_1","name":"greet"}}`,
		`{"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"{}"}}`,
		`{"type":"content_block_stop","index":3}`,
		usage("tool_use"), stop), textReply("hello sent"))
	greet := tacklr.NewTool(tacklr.ToolConfig{Name: "greet", Handler: func(context.Context) (string, error) { return "hello", nil }})
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(tacklr.AgentOptions{Model: m, Tools: []*tacklr.Tool{greet}}, "replay", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	out, stopEvents := tacklr.PipeEvents(func(session.Event) {})
	defer stopEvents()
	if err := h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "greet"}}}, out); err != nil {
		t.Fatal(err)
	}
	reply, err := h.Infer(t.Context(), out)
	if err != nil {
		t.Fatal(err)
	}
	round := tacklr.NewRound(reply)
	if round.Calls[0], err = h.RunCall(t.Context(), tacklr.NewCall(reply.Calls[0], 0), out); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(t.Context(), tacklr.CommitIn{Round: round}, out); err != nil {
		t.Fatal(err)
	}

	// Act
	answer, err := h.Infer(t.Context(), out)

	// Assert
	turns := turnsOf(a.requests[1])
	assistant := `[{"signature":"s1","thinking":"plan","type":"thinking"},{"text":"I will greet.","type":"text"},
		{"signature":"s2","thinking":"","type":"thinking"},{"id":"toolu_1","input":{},"name":"greet","type":"tool_use"}]`
	if err != nil || answer.Result != "hello sent" || len(turns) != 3 || !same(turns[1]["content"], assistant) ||
		!same(turns[2]["content"], `[{"content":"hello","tool_use_id":"toolu_1","type":"tool_result"}]`) {
		t.Fatalf("answer %q (%v), replayed %v", answer.Result, err, turns)
	}
}

// TestInvoke_failuresAreTyped: an answer over the context window and a
// stop at it are ErrContextLength; an overload, a rate limit, a server
// failure, an error event, a cut or unfinished stream and a provider that
// is gone are network errors the runtime retries; a refusal and the
// response limit are their stop-reason errors; another HTTP error keeps
// its status; a stream that is not JSON fails.
func TestInvoke_failuresAreTyped(t *testing.T) {
	t.Parallel()
	answer := func(status int, body string, length int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			if length > 0 {
				// A declared length longer than the body cuts the connection.
				w.Header().Set("Content-Length", fmt.Sprint(length))
			}
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}
	}
	partial := `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`
	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		gone    bool
		want    func(err error) bool
	}{
		"a prompt over the window": {handler: answer(http.StatusBadRequest, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1000001 tokens > 1000000 maximum"}}`, 0), want: func(err error) bool {
			return errors.Is(err, session.ErrContextLength)
		}},
		"a request too large": {handler: answer(http.StatusRequestEntityTooLarge, `{"type":"error","error":{"type":"request_too_large","message":"too big"}}`, 0), want: func(err error) bool { return errors.Is(err, session.ErrContextLength) }},
		"an overload": {handler: answer(529, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, 0), want: func(err error) bool {
			apiErr, ok := errors.AsType[*anthropic.APIStatusError](err)
			return errors.Is(err, tacklr.ErrNetwork) && ok && apiErr.Status == 529 && apiErr.Code == "overloaded_error"
		}},
		"a failure body that is not JSON and is cut": {handler: answer(http.StatusBadGateway, "bad gateway", 100), want: func(err error) bool { return errors.Is(err, tacklr.ErrNetwork) }},
		"an authentication failure": {handler: answer(http.StatusUnauthorized, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`, 0), want: func(err error) bool {
			apiErr, ok := errors.AsType[*anthropic.APIStatusError](err)
			return ok && apiErr.Status == http.StatusUnauthorized && err.Error() == "provider HTTP 401 (authentication_error): invalid x-api-key"
		}},
		"an error event": {handler: answer(http.StatusOK, sse(start, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`), 0), want: func(err error) bool { return errors.Is(err, tacklr.ErrNetwork) }},
		"an error event with no error": {handler: answer(http.StatusOK, sse(start, `{"type":"error"}`), 0), want: func(err error) bool {
			return err != nil && err.Error() == "provider HTTP 200: no error body"
		}},
		"a refusal": {handler: answer(http.StatusOK, sse(start, `{"type":"message_delta","delta":{"stop_reason":"refusal","stop_details":{"type":"refusal","category":"cyber","explanation":"not allowed"}}}`, stop), 0), want: func(err error) bool {
			return errors.Is(err, session.ErrModelRefused) && strings.Contains(err.Error(), "not allowed")
		}},
		"a refusal without an explanation": {handler: answer(http.StatusOK, sse(start, usage("refusal"), stop), 0), want: func(err error) bool {
			return errors.Is(err, session.ErrModelRefused) && strings.Contains(err.Error(), "the model refused")
		}},
		"the response limit":       {handler: answer(http.StatusOK, sse(start, usage("max_tokens"), stop), 0), want: func(err error) bool { return errors.Is(err, session.ErrMaxTokens) }},
		"a stop at the window":     {handler: answer(http.StatusOK, sse(start, usage("model_context_window_exceeded"), stop), 0), want: func(err error) bool { return errors.Is(err, session.ErrContextLength) }},
		"a stream that is no JSON": {handler: answer(http.StatusOK, "data: {nope\n\n", 0), want: func(err error) bool { return errors.Is(err, anthropic.ErrMalformedStream) }},
		"a stream that stops early": {handler: answer(http.StatusOK, sse(start, partial), 0), want: func(err error) bool {
			return errors.Is(err, anthropic.ErrIncompleteStream) && errors.Is(err, tacklr.ErrNetwork)
		}},
		"a stream cut off": {handler: answer(http.StatusOK, sse(start, partial), 10_000), want: func(err error) bool {
			return errors.Is(err, anthropic.ErrIncompleteStream) && errors.Is(err, io.ErrUnexpectedEOF)
		}},
		"a provider that is gone": {handler: answer(http.StatusOK, "", 0), gone: true, want: func(err error) bool { return errors.Is(err, tacklr.ErrNetwork) }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := testhttp.New(t, tc.handler)
			m := anthropic.New(srv.Client()).WithApiKey("k").WithModel("claude-opus-5-5").WithURL(srv.URL)
			if tc.gone {
				srv.Close()
			}

			chunks := collect(t, must(m.Invoke(t.Context(), hello())))

			last := chunks[len(chunks)-1]
			if last.Type != session.EventError || !last.IsComplete || !tc.want(last.Error) {
				t.Errorf("last chunk = %+v", last)
			}
		})
	}
}

func TestCountTokens_sendsTheRequestWithoutItsResponseSettings(t *testing.T) {
	t.Parallel()
	var path string
	var body map[string]any
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		path = r.URL.Path
		_ = json.Unmarshal(raw, &body)
		_, _ = io.WriteString(w, `{"input_tokens":42}`)
	}))
	m := anthropic.New(srv.Client()).WithApiKey("k").WithModel("claude-opus-5-5").WithEffort("high").WithURL(srv.URL)

	n, err := m.CountTokens(t.Context(), hello())

	settings := []string{"max_tokens", "stream", "output_config", "cache_control"}
	sentSettings := slices.ContainsFunc(settings, func(k string) bool { _, ok := body[k]; return ok })
	if err != nil || n != 42 || path != "/messages/count_tokens" || sentSettings || body["model"] != "claude-opus-5-5" || body["thinking"] == nil || len(turnsOf(body)) != 1 {
		t.Errorf("count = %d %v; sent %s %v, want the model, thinking and messages only", n, err, path, body)
	}
}

// TestCountTokens_failuresAreTyped: a gateway without the endpoint (404)
// fails with ErrCannotCount and names the API; a prompt over the window is
// ErrContextLength; a body that is not JSON, a body cut off and an API
// that is gone fail the count.
func TestCountTokens_failuresAreTyped(t *testing.T) {
	t.Parallel()
	answer := func(status int, body string, length int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			if length > 0 {
				w.Header().Set("Content-Length", fmt.Sprint(length))
			}
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}
	}
	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		gone    bool
		want    func(err error) bool
	}{
		"a gateway without the endpoint": {handler: answer(http.StatusNotFound, "", 0), want: func(err error) bool {
			return errors.Is(err, tacklr.ErrCannotCount) && strings.Contains(err.Error(), "http://127.0.0.1")
		}},
		"a prompt over the window": {handler: answer(http.StatusBadRequest, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long"}}`, 0), want: func(err error) bool {
			return errors.Is(err, session.ErrContextLength)
		}},
		"a body that is not JSON": {handler: answer(http.StatusOK, "nope", 0), want: func(err error) bool { return err != nil && strings.Contains(err.Error(), "decode token count") }},
		"a body cut off": {handler: answer(http.StatusOK, `{"input_tokens":`, 100), want: func(err error) bool {
			return errors.Is(err, tacklr.ErrNetwork) && errors.Is(err, io.ErrUnexpectedEOF)
		}},
		"an API that is gone": {handler: answer(http.StatusOK, "", 0), gone: true, want: func(err error) bool { return errors.Is(err, tacklr.ErrNetwork) }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := testhttp.New(t, tc.handler)
			m := anthropic.New(srv.Client()).WithApiKey("k").WithModel("claude-opus-5-5").WithURL(srv.URL)
			if tc.gone {
				srv.Close()
			}

			_, err := m.CountTokens(t.Context(), hello())

			if !tc.want(err) {
				t.Errorf("count err = %v", err)
			}
		})
	}
}

func TestInvoke_aCallCancelledWhileItWaitsForTheAPIEndsTheStream(t *testing.T) {
	t.Parallel()
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sse(start, `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	m := anthropic.New(srv.Client()).WithApiKey("k").WithModel("claude-opus-5-5").WithURL(srv.URL)
	ctx, cancel := context.WithCancel(t.Context())
	ch := must(m.Invoke(ctx, hello()))
	<-ch

	cancel()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("stream did not end after cancel")
		}
	}
}
