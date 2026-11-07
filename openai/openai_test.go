package openai_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
)

// request is what the provider received in the last /responses call.
type request struct {
	body   map[string]any
	header http.Header
}

// provider is a Responses API that answers every request with the SSE
// events, one data line each, and keeps the last request.
func provider(t *testing.T, events ...string) (*httptest.Server, *request) {
	t.Helper()
	var stream strings.Builder
	for _, ev := range events {
		stream.WriteString("data: " + ev + "\n\n")
	}
	got := &request{}
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got.body)
		got.header = r.Header.Clone()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, stream.String())
	}))
	return srv, got
}

// invoke runs one request against srv and returns every chunk of the stream.
func invoke(ctx context.Context, t *testing.T, m *openai.Model, req tacklr.ModelRequest) []tacklr.LLMResponseChunk {
	t.Helper()
	ch, err := m.Invoke(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	var out []tacklr.LLMResponseChunk
	for c := range ch {
		out = append(out, c)
	}
	return out
}

func hello() tacklr.ModelRequest {
	return tacklr.ModelRequest{Messages: []*message.Message{{Role: message.RoleUser, Content: "hi"}}}
}

const completed = `{"type":"response.completed","response":{"status":"completed"}}`

// TestModel_capabilitiesFollowTheModelID: media support and the context
// window come from the model id; an unknown model takes text only and has
// no known window.
func TestModel_capabilitiesFollowTheModelID(t *testing.T) {
	t.Parallel()
	gpt4o, unknown, unset := openai.New(nil).WithModel("gpt-4o"), openai.New(nil).WithModel("unknown-local-llm"), openai.New(nil)
	mimes := []string{"", "text/markdown", "image/png", "application/pdf", "application/zip"}

	supported := func(m *openai.Model) []bool {
		out := make([]bool, len(mimes))
		for i, mime := range mimes {
			out[i] = m.SupportsMIME(mime)
		}
		return out
	}
	window := func(model string) string {
		n, err := openai.New(nil).WithModel(model).MaxContextWindow()
		return fmt.Sprint(n, " ", errors.Is(err, tacklr.ErrUnknownModel))
	}

	if got := supported(gpt4o); !slices.Equal(got, []bool{true, true, true, true, false}) {
		t.Errorf("gpt-4o supports %v of %v", got, mimes)
	}
	if got := supported(unset); !slices.Equal(got, []bool{true, true, false, false, false}) {
		t.Errorf("no model supports %v of %v", got, mimes)
	}
	if got := tacklr.UnsupportedMIMEs(unknown, []string{"text/plain", "image/png", "image/png", "application/pdf"}); !slices.Equal(got, []string{"image/png", "application/pdf"}) {
		t.Errorf("an unknown model rejects %v, want each binary type once", got)
	}
	for model, want := range map[string]string{"gpt-5.4": "1000000 false", "o3-custom": "200000 false", "gpt-5-preview": "1000000 false", "mystery-model": "0 true"} {
		if got := window(model); got != want {
			t.Errorf("window of %s = %s, want %s", model, got, want)
		}
	}
}

// TestInvoke_requiresKeyAndModel: a call without an API key or a model
// fails before it reaches the provider.
func TestInvoke_requiresKeyAndModel(t *testing.T) {
	t.Parallel()
	m := openai.New(http.DefaultClient)
	_, noKey := m.Invoke(t.Context(), tacklr.ModelRequest{})
	_, countNoKey := m.CountTokens(t.Context(), tacklr.ModelRequest{})
	m.WithApiKey("k")
	_, noModel := m.Invoke(t.Context(), tacklr.ModelRequest{})
	_, countNoModel := m.CountTokens(t.Context(), tacklr.ModelRequest{})

	if !errors.Is(noKey, tacklr.ErrApiKeyNotSet) || !errors.Is(countNoKey, tacklr.ErrApiKeyNotSet) ||
		!errors.Is(noModel, tacklr.ErrModelNotSet) || !errors.Is(countNoModel, tacklr.ErrModelNotSet) {
		t.Fatalf("no key = %v, count with no key = %v, no model = %v, count with no model = %v", noKey, countNoKey, noModel, countNoModel)
	}
}

// TestCountTokens_reportsWhatTheProviderAnswers: a count is the provider's
// input_tokens. A provider without the count endpoint (404) fails the count
// with ErrCannotCount, and names the provider. A request past the context
// length is ErrContextLength, which is also ErrMaxTokens. A body that
// is not JSON, a body cut off by the network, a provider that is gone and a
// base URL that does not parse fail the count.
func TestCountTokens_reportsWhatTheProviderAnswers(t *testing.T) {
	t.Parallel()
	req := tacklr.ModelRequest{Messages: []*message.Message{{Role: message.RoleUser, Content: "count these tokens please"}}}
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
	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		gone    bool
		url     string
		want    func(n int, err error) bool
	}{
		"a count": {handler: answer(http.StatusOK, `{"input_tokens":42}`, 0), want: func(n int, err error) bool { return err == nil && n == 42 }},
		"a provider without the endpoint": {handler: answer(http.StatusNotFound, "", 0), want: func(_ int, err error) bool {
			apiErr, ok := errors.AsType[*openai.APIStatusError](err)
			return ok && apiErr.Status == http.StatusNotFound && errors.Is(err, tacklr.ErrCannotCount) && strings.Contains(err.Error(), "http://127.0.0.1")
		}},
		"a 400 for a request past the context length": {handler: answer(http.StatusBadRequest, `{"error":{"message":"context_length_exceeded"}}`, 0), want: func(_ int, err error) bool {
			return errors.Is(err, session.ErrContextLength) && errors.Is(err, session.ErrMaxTokens)
		}},
		"a body that is not JSON": {handler: answer(http.StatusOK, `not json`, 0), want: func(_ int, err error) bool {
			return err != nil && strings.Contains(err.Error(), "unmarshal response")
		}},
		"a body cut off": {handler: answer(http.StatusOK, `{"input_tokens":`, 100), want: func(_ int, err error) bool {
			return errors.Is(err, tacklr.ErrNetwork) && errors.Is(err, io.ErrUnexpectedEOF)
		}},
		"a provider that is gone": {handler: answer(http.StatusOK, "", 0), gone: true, want: func(_ int, err error) bool { return errors.Is(err, tacklr.ErrNetwork) }},
		"a base URL that does not parse": {handler: answer(http.StatusOK, "", 0), url: "http://bad\x7f", want: func(_ int, err error) bool {
			return err != nil && strings.Contains(err.Error(), "build provider request")
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := testhttp.New(t, tc.handler)
			m := openai.New(srv.Client()).WithApiKey("k").WithModel("m").WithURL(cmp.Or(tc.url, srv.URL))
			if tc.gone {
				srv.Close()
			}

			n, err := m.CountTokens(t.Context(), req)

			if !tc.want(n, err) {
				t.Errorf("count = %d %v", n, err)
			}
		})
	}
}

// TestInvoke_sendsEachProvidersPromptCacheProfile: the request carries the
// cache key each provider reads, GPT-5.6 also gets its cache options and
// breakpoints, and the stream reports the cached tokens.
func TestInvoke_sendsEachProvidersPromptCacheProfile(t *testing.T) {
	t.Parallel()
	plan := &message.Message{Role: message.RoleDeveloper, Content: "PROJECT PLAN\n────────────\nDo the work"}
	echo := tacklr.NewTool(tacklr.ToolConfig{Name: "echo", Handler: func(context.Context) (string, error) { return "ok", nil }})
	type want struct {
		header      string
		options     any
		breakpoints int
		toolChoice  any
		reasoning   any
		cached      [2]int
	}
	for name, tc := range map[string]struct {
		model func(*http.Client) *openai.Model
		ctx   context.Context
		req   tacklr.ModelRequest
		usage string
		want  want
	}{
		"xAI": {
			model: func(c *http.Client) *openai.Model { return openai.New(c).WithModel("grok-4.6") },
			ctx:   t.Context(),
			req:   tacklr.ModelRequest{Messages: []*message.Message{{Role: message.RoleUser, Content: "ask"}, plan}, SystemPrompt: "stable system", CacheKey: "sess-1"},
			usage: `{"input_tokens":10,"output_tokens":1,"input_tokens_details":{"cached_tokens":8}}`,
			want:  want{header: "sess-1", cached: [2]int{8, 0}},
		},
		"GPT-5.6": {
			model: func(c *http.Client) *openai.Model {
				return openai.New(c).WithModel("gpt-5.6").WithReasoningLevel("high")
			},
			ctx: tacklr.ContextWithToolChoiceNone(t.Context()),
			req: tacklr.ModelRequest{Messages: []*message.Message{
				{Role: message.RoleUser, Content: "ask"}, plan,
				{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "call_1", CallID: "call_1", Name: "echo", Arguments: "{}"}}},
				{Role: message.RoleTool, ToolCallID: "call_1", Content: "tool-out"},
			}, Tools: []*tacklr.Tool{echo}, SystemPrompt: "stable system", CacheKey: " sess-1 "},
			usage: `{"input_tokens":20,"output_tokens":2,"input_tokens_details":{"cached_tokens":12,"cache_write_tokens":8}}`,
			want: want{
				options: map[string]any{"mode": "implicit", "ttl": "30m"}, breakpoints: 3, toolChoice: "none",
				reasoning: map[string]any{"effort": "high", "summary": "auto"}, cached: [2]int{12, 8},
			},
		},
		// The strategy decides the profile, not a model id that names GPT-5.6.
		"OpenRouter": {
			model: func(c *http.Client) *openai.Model {
				return openai.NewOpenRouter(c).WithModel("openai/gpt-5.6").WithReasoningSummary("concise")
			},
			ctx:   t.Context(),
			req:   tacklr.ModelRequest{Messages: []*message.Message{{Role: message.RoleUser, Content: "ask"}, plan}, SystemPrompt: "stable system", CacheKey: "sess-1"},
			usage: `{"input_tokens":5,"output_tokens":1}`,
			want:  want{reasoning: map[string]any{"summary": "concise"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv, sent := provider(t, `{"type":"response.completed","response":{"status":"completed","usage":`+tc.usage+`}}`, "[DONE]")
			m := tc.model(srv.Client()).WithApiKey("k").WithURL(srv.URL)

			chunks := invoke(tc.ctx, t, m, tc.req)

			input, _ := json.Marshal(sent.body["input"])
			got := want{
				header:      sent.header.Get("x-grok-conv-id"),
				options:     sent.body["prompt_cache_options"],
				breakpoints: strings.Count(string(input), `"prompt_cache_breakpoint"`),
				toolChoice:  sent.body["tool_choice"],
				reasoning:   sent.body["reasoning"],
				cached:      [2]int{chunks[0].CachedTokens, chunks[0].CacheWriteTokens},
			}
			if sent.body["prompt_cache_key"] != "sess-1" || fmt.Sprint(got) != fmt.Sprint(tc.want) || len(chunks) != 1 || chunks[0].Type != session.EventComplete {
				t.Errorf("key %v, request and usage = %+v, want %+v; chunks %+v", sent.body["prompt_cache_key"], got, tc.want, chunks)
			}
		})
	}
}

// TestInvoke_sendsTheConversationAsResponsesInput: the input pairs each
// namespaced call with its output by call id, or by item id when no output
// has the call id, keeps an incomplete call's
// status, drops an output with no call, a nil message and a part with no
// image or file, sends file data as a named data URL, marks the last tool
// output as a cache breakpoint, and sends a reasoning id only with its
// ciphertext.
func TestInvoke_sendsTheConversationAsResponsesInput(t *testing.T) {
	t.Parallel()
	srv, sent := provider(t, completed, "[DONE]")
	m := openai.New(srv.Client()).WithApiKey("k").WithModel("m").WithURL(srv.URL)
	greet := tacklr.NewTool(tacklr.ToolConfig{Name: "greet", Namespace: "ado", Handler: func(context.Context) (string, error) { return "", nil }})

	invoke(t.Context(), t, m, tacklr.ModelRequest{Messages: []*message.Message{
		nil,
		{Role: message.RoleUser, Content: "hi"},
		{Role: message.RoleUser, ContentParts: []message.ContentPart{{Type: message.ContentTypeInputImage}, {Type: message.ContentTypeInputFile}}},
		{Role: message.RoleUser, Content: "read this", ContentParts: []message.ContentPart{{Type: message.ContentTypeInputFile, FileData: &message.FileData{Data: "QUJD", MIMEType: "application/pdf"}}}},
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{
			{CallID: "call_1", Name: "greet", Namespace: "ado"},
			{ID: "fc_item", CallID: "call_2", Name: "greet", Namespace: "ado", Arguments: `{"a":1}`, Status: string(message.StatusIncomplete)},
			{ID: "fc_3", CallID: "call_3", Name: "greet", Namespace: "ado"},
		}},
		{Role: message.RoleTool, ToolCallID: "fc_3", Content: "ok3"},
		{Role: message.RoleTool, ToolCallID: "call_1", Content: "ok"},
		{Role: message.RoleTool, ToolCallID: "call_2", Content: "ok2"},
		{Role: message.RoleTool, ToolCallID: "fc_ghost", Content: "nope"},
		{Role: message.RoleReasoning, MessageID: "rs_store", EncryptedContent: "gAAAAABcipher", Content: "plan"},
		{Role: message.RoleReasoning, MessageID: "rs_orphan", Content: "after"},
	}, Tools: []*tacklr.Tool{greet}})

	input, _ := json.Marshal(sent.body["input"])
	tools, _ := json.Marshal(sent.body["tools"])
	want := `[{"content":"hi","role":"user"},` +
		`{"content":"","role":"user"},` +
		`{"content":[{"text":"read this","type":"input_text"},{"file_data":"data:application/pdf;base64,QUJD","filename":"document.pdf","type":"input_file"}],"role":"user"},` +
		`{"arguments":"{}","call_id":"call_1","name":"ado__greet","status":"completed","type":"function_call"},` +
		`{"call_id":"call_1","output":"ok","status":"completed","type":"function_call_output"},` +
		`{"arguments":"{\"a\":1}","call_id":"call_2","name":"ado__greet","status":"incomplete","type":"function_call"},` +
		`{"call_id":"call_2","output":"ok2","status":"completed","type":"function_call_output"},` +
		`{"arguments":"{}","call_id":"call_3","name":"ado__greet","status":"completed","type":"function_call"},` +
		`{"call_id":"call_3","output":[{"prompt_cache_breakpoint":{"mode":"explicit"},"text":"ok3","type":"input_text"}],"status":"completed","type":"function_call_output"},` +
		`{"encrypted_content":"gAAAAABcipher","id":"rs_store","summary":[{"text":"plan","type":"summary_text"}],"type":"reasoning"},` +
		`{"summary":[{"text":"after","type":"summary_text"}],"type":"reasoning"}]`
	if string(input) != want || !strings.HasPrefix(string(tools), `[{"description":"","name":"ado__greet","parameters":{`) || !strings.HasSuffix(string(tools), `"strict":true,"type":"function"}]`) {
		t.Errorf("input = %s\nwant    %s\ntools = %s", input, want, tools)
	}
}

// TestInvoke_streamsEachOutputItemAsAChunk: text deltas are messages on any
// item, reasoning deltas, summaries and reasoning text are reasoning, a call
// keeps its call id or takes its item id and its namespace from its name, a
// done message is complete, an item
// whose shape does not parse is left out, and the end reports the usage.
func TestInvoke_streamsEachOutputItemAsAChunk(t *testing.T) {
	t.Parallel()
	srv, _ := provider(t,
		`{"type":"response.output_item.added","item":{"id":"rs_1","type":"reasoning"}}`,
		`{"type":"response.output_text.delta","item_id":"rs_1","delta":"<think>internal</think>"}`,
		`{"type":"response.output_item.added","item":{"id":"msg_1","type":"message"}}`,
		`{"type":"response.output_text.delta","delta":"Hello"}`,
		`{"type":"response.reasoning_text.delta","item_id":"rs_2","delta":"think"}`,
		`{"type":"response.reasoning_summary_text.delta","item_id":"rs_2","delta":"sum"}`,
		`{"type":"response.output_item.done","item":{"id":"rs_3","type":"reasoning","summary":[{"type":"summary_text","text":""},{"type":"summary_text","text":"final summary"}]}}`,
		`{"type":"response.output_item.done","item":{"id":"rs_4","type":"reasoning","content":[{"type":"reasoning_text","text":""},{"type":"reasoning_text","text":"plain"}]}}`,
		`{"type":"response.output_item.done","item":{"id":"rs_5","type":"reasoning","summary":"not a list"}}`,
		`{"type":"response.output_item.done","item":{"type":5}}`,
		`{"type":"response.output_item.done","item":{"type":"function_call","arguments":5}}`,
		`{"type":"response.output_item.done","item":{"type":"function_call","id":"fc_y","arguments":"{}","name":"ado__echo"}}`,
		`{"type":"response.output_item.done","item":{"id":"msg_0","type":"message"}}`,
		`{"type":"response.output_item.done","item":{"type":"function_call","status":"incomplete","arguments":"{","call_id":"fc_x","name":"echo"}}`,
		`{"type":"response.output_item.done","item":{"id":"msg_m","type":"message","content":[{"type":"refusal","refusal":"x"},{"type":"output_text","text":"hi"}]}}`,
		`{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":5,"output_tokens_details":{"reasoning_tokens":2}}}}`,
		"[DONE]",
	)
	m := openai.New(srv.Client()).WithApiKey("k").WithModel("gpt-4o").WithURL(srv.URL)

	chunks := invoke(t.Context(), t, m, hello())

	var got []string
	for _, c := range chunks {
		line := fmt.Sprintf("%s %s %q complete=%v", c.Type, c.MessageId, c.Content, c.IsComplete)
		for _, tc := range c.ToolCalls {
			name := tc.Name
			if tc.Namespace != "" {
				name = tc.Namespace + " " + tc.Name
			}
			line += fmt.Sprintf(" call %s/%s %s", tc.ID, tc.CallID, name)
		}
		if c.Type == session.EventComplete {
			line += fmt.Sprint(" tokens ", c.InputTokens, c.OutputTokens, c.ReasoningTokens)
		}
		got = append(got, line)
	}
	want := []string{
		`message rs_1 "<think>internal</think>" complete=false`,
		`message msg_1 "Hello" complete=false`,
		`reasoning rs_2 "think" complete=false`,
		`reasoning rs_2 "sum" complete=false`,
		`reasoning rs_3 "final summary" complete=true`,
		`reasoning rs_4 "plain" complete=true`,
		`function_call  "" complete=false call fc_y/fc_y ado echo`,
		`message msg_0 "" complete=true`,
		`function_call  "" complete=false call fc_x/fc_x echo`,
		`message msg_m "hi" complete=true`,
		`complete  "" complete=true tokens 10 5 2`,
	}
	if !slices.Equal(got, want) {
		t.Errorf("chunks =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestInvoke_aStreamWithoutAUsableEndEndsInATypedError: each way a stream
// can stop short ends in one error chunk that names the stop reason.
func TestInvoke_aStreamWithoutAUsableEndEndsInATypedError(t *testing.T) {
	t.Parallel()
	partial := `{"type":"response.output_text.delta","item_id":"msg","delta":"partial"}`
	for name, tc := range map[string]struct {
		events []string
		is     error
		says   string
	}{
		"an incomplete response at the token limit": {[]string{`{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`}, session.ErrMaxTokens, "max_output_tokens"},
		"an incomplete response for another reason": {[]string{`{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"other"}}}`}, nil, "response incomplete (other)"},
		"an incomplete response with no reason":     {[]string{`{"type":"response.incomplete","response":{"status":"incomplete"}}`}, nil, "status=incomplete"},
		"a completed response that is incomplete":   {[]string{`{"type":"response.completed","response":{"status":"incomplete","incomplete_details":{"reason":"max_tokens"}}}`}, session.ErrMaxTokens, "max_tokens"},
		"a failed response the filter stopped":      {[]string{`{"type":"response.failed","incomplete_details":{"reason":"content_filter"}}`}, session.ErrModelRefused, "content_filter"},
		"a failed response with an error":           {[]string{`{"type":"response.failed","error":{"code":"content_filter","message":"blocked by filter"}}`}, session.ErrModelRefused, "blocked by filter"},
		"a failed response with a nested error":     {[]string{`{"type":"response.failed","response":{"error":{"code":"server_error","message":"nested block"}}}`}, nil, "(server_error): nested block"},
		"a failed response with only an error type": {[]string{`{"type":"response.failed","response":{"error":{"type":"server_error"}}}`}, nil, "server_error"},
		"a failed response with a long flat error":  {[]string{`{"type":"response.failed","response":{"error_message":"flat failure","error_code":"bad_input","trace":"` + strings.Repeat("x", 400) + `"}}`}, nil, "(bad_input): flat failure"},
		"an error event":                      {[]string{partial, `{"type":"error","error":{"message":"provider failed mid stream"}}`}, nil, "provider failed mid stream"},
		"a message that only refuses":         {[]string{`{"type":"response.output_item.done","item":{"id":"msg_r","type":"message","content":[{"type":"refusal","refusal":"I cannot help with that"}]}}`, completed, "[DONE]"}, session.ErrModelRefused, "I cannot help with that"},
		"a message that refuses with no text": {[]string{`{"type":"response.output_item.done","item":{"id":"msg_e","type":"message","content":[{"type":"refusal","refusal":""}]}}`, completed, "[DONE]"}, session.ErrModelRefused, "model refused"},
		"done before a terminal event":        {[]string{partial, "[DONE]"}, openai.ErrIncompleteStream, "incomplete provider stream"},
		"a close before a terminal event":     {[]string{partial}, openai.ErrIncompleteStream, "incomplete provider stream"},
		"an event that is not JSON":           {[]string{"not-json"}, openai.ErrMalformedStream, "malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv, _ := provider(t, tc.events...)
			m := openai.New(srv.Client()).WithApiKey("k").WithModel("gpt-4o").WithURL(srv.URL)

			chunks := invoke(t.Context(), t, m, hello())

			last := chunks[len(chunks)-1]
			wantChunks := 1 + strings.Count(strings.Join(tc.events, ""), `"delta":"partial"`)
			if len(chunks) != wantChunks || last.Type != session.EventError || !last.IsComplete || (tc.is != nil && !errors.Is(last.Error, tc.is)) || !strings.Contains(last.Content, tc.says) {
				t.Errorf("chunks = %+v, want %d ending in an error that is %v and says %q", chunks, wantChunks, tc.is, tc.says)
			}
		})
	}
}

// TestInvoke_aCallThatCannotBeSentOrFailsEndsInOneError: a base URL that
// does not parse, a failed status whose body the network cuts off, and a
// provider that is gone each end the stream in one error chunk, for an empty
// request and a long one.
func TestInvoke_aCallThatCannotBeSentOrFailsEndsInOneError(t *testing.T) {
	t.Parallel()
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":`)
	}))
	gone := testhttp.New(t, http.NotFoundHandler())
	gone.Close()
	unsent := openai.New(srv.Client()).WithApiKey("k").WithModel("gpt-4o").WithURL("http://bad\x7f")
	unreached := openai.New(srv.Client()).WithApiKey("k").WithModel("gpt-4o").WithURL(gone.URL)
	failed := openai.New(srv.Client()).WithApiKey("k").WithModel("gpt-4o").WithURL(srv.URL)

	long := tacklr.ModelRequest{}
	for range 30 {
		long.Messages = append(long.Messages, &message.Message{Role: message.RoleUser, Content: "hi"})
	}

	unsentChunks := invoke(t.Context(), t, unsent, tacklr.ModelRequest{})
	failedChunks := invoke(t.Context(), t, failed, long)
	unreachedChunks := invoke(t.Context(), t, unreached, hello())

	if len(unsentChunks) != 1 || !strings.Contains(unsentChunks[0].Content, "build provider request") {
		t.Errorf("chunks from a URL that does not parse = %+v, want one error that says so", unsentChunks)
	}
	if apiErr, ok := errors.AsType[*openai.APIStatusError](failedChunks[len(failedChunks)-1].Error); len(failedChunks) != 1 || !ok || apiErr.Status != http.StatusInternalServerError {
		t.Errorf("chunks from a 500 with a cut body = %+v, want one provider error with its status", failedChunks)
	}
	if len(unreachedChunks) != 1 || !errors.Is(unreachedChunks[0].Error, tacklr.ErrNetwork) {
		t.Errorf("chunks from a provider that is gone = %+v, want one network error", unreachedChunks)
	}
}

// TestInvoke_aDroppedConnectionEndsInAnIncompleteStreamError: a body cut
// off by the network is an incomplete stream.
func TestInvoke_aDroppedConnectionEndsInAnIncompleteStreamError(t *testing.T) {
	t.Parallel()
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		const sent = `data: {"type":"response.output_text.delta","delta":"partial"}` + "\n"
		w.Header().Set("Content-Type", "text/event-stream")
		// The declared length is longer than the body, so the server closes
		// the connection in the middle of it.
		w.Header().Set("Content-Length", fmt.Sprint(len(sent)+100))
		_, _ = io.WriteString(w, sent)
	}))
	m := openai.New(srv.Client()).WithApiKey("k").WithModel("gpt-4o").WithURL(srv.URL)

	chunks := invoke(t.Context(), t, m, hello())

	if len(chunks) != 2 || chunks[0].Content != "partial" || !errors.Is(chunks[1].Error, openai.ErrIncompleteStream) || !errors.Is(chunks[1].Error, io.ErrUnexpectedEOF) {
		t.Fatalf("chunks = %+v, want the partial text, then an incomplete stream from the cut body", chunks)
	}
}

// TestInvoke_aCancelledCallEndsTheStream: cancelling the caller's context
// closes the stream, both while sent lines wait to be read and while the
// call waits for the provider.
func TestInvoke_aCancelledCallEndsTheStream(t *testing.T) {
	t.Parallel()
	line := `data: {"type":"response.output_text.delta","item_id":"m","delta":"x"}` + "\n\n"
	for name, burst := range map[string]int{"with lines still to read": 100, "while it waits for the provider": 1} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, strings.Repeat(line, burst))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			m := openai.New(srv.Client()).WithApiKey("k").WithModel("m").WithURL(srv.URL)
			ctx, cancel := context.WithCancel(t.Context())
			ch, err := m.Invoke(ctx, hello())
			if err != nil {
				t.Fatal(err)
			}
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
		})
	}
}
