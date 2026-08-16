package inference

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pkoukk/tiktoken-go"

	"github.com/ryanaldo34/tacklr"
)

func TestMaxContextWindow_knownAndPrefixAndUnknown(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil)
	s.WithModel("gpt-5.4")
	n, err := s.MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5.4: n=%d err=%v", n, err)
	}

	s = NewOpenAIInferenceStrategy(nil)
	s.WithModel("o3-custom")
	n, err = s.MaxContextWindow()
	if err != nil || n != 200000 {
		t.Fatalf("o3 prefix: n=%d err=%v", n, err)
	}

	s = NewOpenAIInferenceStrategy(nil)
	s.WithModel("gpt-5-preview")
	n, err = s.MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5 prefix: n=%d err=%v", n, err)
	}

	s = NewOpenAIInferenceStrategy(nil)
	s.WithModel("mystery-model")
	_, err = s.MaxContextWindow()
	if err == nil || !errors.Is(err, tacklr.ErrUnknownModel) {
		t.Fatalf("unknown: err=%v", err)
	}
}

func TestWithReasoningAndStructuredOutput_onInvokeRequest(t *testing.T) {
	var saw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)

	type Out struct {
		Answer string `json:"answer" desc:"the answer"`
	}
	s := NewOpenAIInferenceStrategy(srv.Client())
	s.WithApiKey("k")
	s.WithModel("m")
	s.WithURL(srv.URL)
	s.WithReasoningLevel("high")
	s.WithReasoningSummary("detailed")
	s.WithStructuredOutput(Out{})
	s.SetSystemPrompt("be precise")

	// Also clear structured output path for nil.
	_ = s.WithStructuredOutput(nil)
	s.WithStructuredOutput(Out{})

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "q"},
		{Role: tacklr.RoleAssistant, Content: "partial", ToolCalls: []tacklr.ToolCall{
			{CallID: "c1", Name: "lookup", Arguments: `{}`},
		}},
		{Role: tacklr.RoleTool, ToolCallID: "c1", Content: "tool-out"},
		{Role: tacklr.RoleSystem, Content: "sys"},
		{Role: tacklr.RoleDeveloper, Content: "dev handoff"},
	}, []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Handler: func(ctx context.Context) (string, error) { return "", nil }}),
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}

	if saw["instructions"] != "be precise" {
		t.Errorf("instructions = %v", saw["instructions"])
	}
	reasoning, _ := saw["reasoning"].(map[string]any)
	if reasoning["effort"] != "high" || reasoning["summary"] != "detailed" {
		t.Errorf("reasoning = %v", saw["reasoning"])
	}
	if saw["text"] == nil {
		t.Error("expected structured output text.format")
	}
	// Input should include function_call_output and system-wired developer.
	inRaw, _ := json.Marshal(saw["input"])
	inStr := string(inRaw)
	if !strings.Contains(inStr, "function_call_output") {
		t.Errorf("input missing tool output: %s", inStr)
	}
	if !strings.Contains(inStr, "dev handoff") {
		t.Errorf("input missing developer content: %s", inStr)
	}
}

func TestCountTokens_missingConfigAndNonFallbackError(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	if _, err := s.CountTokens(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("no key: %v", err)
	}
	s.WithApiKey("k")
	if _, err := s.CountTokens(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Fatalf("no model: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":{"message":"upstream down"}}`)
	}))
	t.Cleanup(srv.Close)
	s = NewOpenAIInferenceStrategy(srv.Client())
	s.WithApiKey("k")
	s.WithModel("m")
	s.WithURL(srv.URL)
	_, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil)
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) || apiErr.Status != 500 {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(apiErr.Body, "upstream down") {
		t.Errorf("body = %q", apiErr.Body)
	}
}

func TestCountTokens_withToolsAndInstructions(t *testing.T) {
	var saw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		_, _ = io.WriteString(w, `{"input_tokens":7}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client())
	s.WithApiKey("k")
	s.WithModel("m")
	s.WithURL(srv.URL)
	s.SetSystemPrompt("count me")
	tool := tacklr.NewTool(tacklr.ToolConfig{Name: "t", Handler: func(ctx context.Context) (string, error) { return "", nil }})
	n, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, []*tacklr.Tool{tool})
	if err != nil || n != 7 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if saw["instructions"] != "count me" {
		t.Errorf("instructions = %v", saw["instructions"])
	}
	toolsRaw, _ := json.Marshal(saw["tools"])
	if !strings.Contains(string(toolsRaw), "t") {
		t.Errorf("tools = %s", toolsRaw)
	}
}

func TestInvoke_missingConfig(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	if _, err := s.Invoke(context.Background(), nil, nil, ""); !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("%v", err)
	}
	s.WithApiKey("k")
	if _, err := s.Invoke(context.Background(), nil, nil, ""); !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Fatalf("%v", err)
	}
}

func TestInvoke_httpDoError_closesChannel(t *testing.T) {
	// Client that always fails.
	s := NewOpenAIInferenceStrategy(&http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("network down")
		}),
	})
	s.WithApiKey("k")
	s.WithModel("m")
	s.WithURL("http://example.invalid")

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var sawErr bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				if !sawErr {
					t.Fatal("channel closed without StreamEventError")
				}
				return
			}
			if c.Type == tacklr.StreamEventError && strings.Contains(c.Content, "network down") {
				sawErr = true
			}
		case <-deadline:
			t.Fatal("channel did not close")
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAPIStatusError_ErrorString(t *testing.T) {
	e := &APIStatusError{Status: 429, Body: "slow down"}
	if !strings.Contains(e.Error(), "429") || !strings.Contains(e.Error(), "slow down") {
		t.Fatalf("%q", e.Error())
	}
	e.Code = "rate_limit"
	if !strings.Contains(e.Error(), "rate_limit") {
		t.Fatalf("%q", e.Error())
	}
}

func TestExtractErrorMessage_rawBodyFallback(t *testing.T) {
	if got := extractErrorMessage([]byte("not-json")); got != "not-json" {
		t.Fatalf("%q", got)
	}
}

func TestParseSSE_functionCallWithNamespaceAndIDOnly(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"type":"function_call","status":"completed","arguments":"{}","id":"only_id","name":"crm.get_customer"}}`,
		`data: {"type":"response.output_item.done","item":{"type":"function_call","status":"completed","arguments":"{}","call_id":"cid","name":"echo","namespace":"ns"}}`,
		`data: {"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_done"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, body)
	var fcs []tacklr.LLMResponseChunk
	var reasoningDone bool
	for _, c := range chunks {
		if c.Type == tacklr.StreamEventFunctionCall {
			fcs = append(fcs, c)
		}
		if c.Type == tacklr.StreamEventReasoning && c.IsComplete && c.MessageId == "rs_done" {
			reasoningDone = true
		}
	}
	if len(fcs) != 2 {
		t.Fatalf("function calls = %d", len(fcs))
	}
	// name with dot splits to namespace when namespace empty.
	if fcs[0].ToolCalls[0].Namespace != "crm" || fcs[0].ToolCalls[0].Name != "get_customer" {
		t.Errorf("first = %+v", fcs[0].ToolCalls[0])
	}
	if fcs[0].ToolCalls[0].ID != "only_id" || fcs[0].ToolCalls[0].CallID != "only_id" {
		t.Errorf("id normalize = %+v", fcs[0].ToolCalls[0])
	}
	if fcs[1].ToolCalls[0].Namespace != "ns" || fcs[1].ToolCalls[0].Name != "echo" {
		t.Errorf("second = %+v", fcs[1].ToolCalls[0])
	}
	if !reasoningDone {
		t.Error("expected reasoning complete chunk")
	}
}

func TestWithStructuredOutput_pointerAndClear(t *testing.T) {
	type S struct {
		X string `json:"x"`
	}
	s := NewOpenAIInferenceStrategy(nil)
	s.WithStructuredOutput(&S{})
	if s.structuredOutputName != "S" || s.structuredOutputSchema == nil {
		t.Fatalf("schema not set: name=%q schema=%v", s.structuredOutputName, s.structuredOutputSchema)
	}
	s.WithStructuredOutput(nil)
	if s.structuredOutputSchema != nil || s.structuredOutputName != "" {
		t.Fatal("nil should clear structured output")
	}
}

func TestParseAPIErrorMeta_nonJSON(t *testing.T) {
	code, typ := parseAPIErrorMeta([]byte("not-json"))
	if code != "" || typ != "" {
		t.Fatalf("code=%q typ=%q", code, typ)
	}
	// Valid error object used by classifyProviderFailure.
	err := classifyProviderFailure(400, []byte(`not-json`))
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestMarshalMessagesToInput_responsesToolAndReasoningWire: one coverage of
// Responses multi-turn input shape — interleaved call/output, no item id on
// function_call, status on tool items, empty args → {}, reasoning summary
// required without status, roles including system-wired developer.
func TestMarshalMessagesToInput_responsesToolAndReasoningWire(t *testing.T) {
	items := marshalMessagesToInput([]*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "hi"},
		{Role: tacklr.RoleReasoning, MessageID: "rs_1", Content: "thought trail"},
		{Role: tacklr.RoleReasoning, MessageID: "rs_empty"},
		{Role: tacklr.RoleAssistant, ToolCalls: []tacklr.ToolCall{
			{ID: "fc_item_1", CallID: "call_aaa", Name: "web_search", Arguments: `{"query":"q"}`},
			{ID: "fc_item_2", CallID: "call_bbb", Name: "list_plan"}, // empty args
		}},
		{Role: tacklr.RoleTool, ToolCallID: "call_aaa", Content: "hits"},
		{Role: tacklr.RoleTool, ToolCallID: "call_bbb", Content: "plan"},
		{Role: tacklr.RoleSystem, Content: "sys"},
		{Role: tacklr.RoleDeveloper, Content: "dev"},
		{Role: tacklr.RoleAssistant, Content: "said"},
	})
	// user, rs_1, rs_empty, fc_aaa, out_aaa, fc_bbb, out_bbb, sys, dev-as-sys, said
	if len(items) != 10 {
		t.Fatalf("items = %d, want 10: %v", len(items), items)
	}

	var rs map[string]any
	if err := json.Unmarshal(items[1], &rs); err != nil {
		t.Fatal(err)
	}
	if rs["type"] != "reasoning" || rs["id"] != "rs_1" {
		t.Fatalf("reasoning = %#v", rs)
	}
	if _, hasStatus := rs["status"]; hasStatus {
		t.Fatalf("reasoning input must omit status: %#v", rs)
	}
	sum, ok := rs["summary"].([]any)
	if !ok || len(sum) != 1 {
		t.Fatalf("summary = %#v", rs["summary"])
	}
	part, _ := sum[0].(map[string]any)
	if part["type"] != "summary_text" || part["text"] != "thought trail" {
		t.Fatalf("summary part = %#v", part)
	}
	var rsEmpty map[string]any
	if err := json.Unmarshal(items[2], &rsEmpty); err != nil {
		t.Fatal(err)
	}
	emptySum, ok := rsEmpty["summary"].([]any)
	if !ok || len(emptySum) != 0 {
		t.Fatalf("empty reasoning summary = %#v, want []", rsEmpty["summary"])
	}

	var fc1, out1, fc2, out2 map[string]any
	_ = json.Unmarshal(items[3], &fc1)
	_ = json.Unmarshal(items[4], &out1)
	_ = json.Unmarshal(items[5], &fc2)
	_ = json.Unmarshal(items[6], &out2)
	if fc1["type"] != "function_call" || fc1["call_id"] != "call_aaa" || fc1["status"] != "completed" {
		t.Fatalf("fc1 = %#v", fc1)
	}
	if _, hasID := fc1["id"]; hasID {
		t.Fatalf("function_call must omit provider item id: %#v", fc1)
	}
	if out1["type"] != "function_call_output" || out1["call_id"] != "call_aaa" || out1["output"] != "hits" || out1["status"] != "completed" {
		t.Fatalf("out1 = %#v", out1)
	}
	if fc2["call_id"] != "call_bbb" || fc2["arguments"] != "{}" {
		t.Fatalf("fc2 empty-args = %#v", fc2)
	}
	if out2["output"] != "plan" {
		t.Fatalf("out2 = %#v", out2)
	}

	var dev map[string]any
	_ = json.Unmarshal(items[8], &dev)
	if dev["role"] != "system" || dev["content"] != "dev" {
		t.Fatalf("developer must wire as system: %#v", dev)
	}
}

func TestCountTokens_badJSONResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `not-json`)
	}))
	t.Cleanup(srv.Close)
	s := NewOpenAIInferenceStrategy(srv.Client())
	s.WithApiKey("k")
	s.WithModel("m")
	s.WithURL(srv.URL)
	_, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("err = %v", err)
	}
}

func TestCountTokens_structuredOutputAndDoError(t *testing.T) {
	type Out struct {
		V int `json:"v"`
	}
	// HTTP Do error
	s := NewOpenAIInferenceStrategy(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	})})
	s.WithApiKey("k").WithModel("m").WithURL("http://example.invalid")
	if _, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil); err == nil {
		t.Fatal("want network error")
	}

	// structured output on request + successful count
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"input_tokens":7,"object":"count"}`)
	}))
	t.Cleanup(srv.Close)
	s2 := NewOpenAIInferenceStrategy(srv.Client())
	s2.WithApiKey("k").WithModel("m").WithURL(srv.URL)
	s2.WithStructuredOutput(Out{})
	s2.SetSystemPrompt("sys")
	n, err := s2.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil)
	if err != nil || n != 7 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestInvoke_createRequestErrorAndCancelledSend(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	s.WithApiKey("k").WithModel("m").WithURL("http://\x00") // invalid URL
	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil, "")
	if err != nil {
		// NewRequest may fail before goroutine
		return
	}
	for range ch {
	}

	// Cancelled ctx before sendChunk of HTTP error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s2 := NewOpenAIInferenceStrategy(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("x")
	})})
	s2.WithApiKey("k").WithModel("m").WithURL("http://example.invalid")
	ch2, err := s2.Invoke(ctx, []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}
}

func TestCountTokens_createRequestAndReadBodyErrors(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	s.WithApiKey("k").WithModel("m").WithURL("http://\x00")
	if _, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil); err == nil {
		t.Fatal("want create request error")
	}

	// Body read failure after 200.
	s2 := NewOpenAIInferenceStrategy(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(errReader{}),
			Header:     make(http.Header),
		}, nil
	})})
	s2.WithApiKey("k").WithModel("m").WithURL("http://example.invalid")
	if _, err := s2.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil); err == nil {
		t.Fatal("want read body error")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read fail") }

func TestEmitFunctionCallChunk_badJSON(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil)
	ch := make(chan tacklr.LLMResponseChunk, 2)
	s.emitFunctionCallChunk([]byte(`{`), ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	if n != 0 {
		t.Fatalf("expected no chunks, got %d", n)
	}
}

func TestCountTokens_tiktokenEncodingError(t *testing.T) {
	prev := getEncoding
	t.Cleanup(func() { getEncoding = prev })
	getEncoding = func(string) (*tiktoken.Tiktoken, error) {
		return nil, errors.New("no encoding")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	t.Cleanup(srv.Close)
	s := NewOpenAIInferenceStrategy(srv.Client())
	s.WithApiKey("k").WithModel("m").WithURL(srv.URL)
	if _, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil); err == nil || !strings.Contains(err.Error(), "tiktoken") {
		t.Fatalf("err = %v", err)
	}
}

func TestWithReasoningLevel_setsDefaultSummary(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil)
	s.WithReasoningLevel("low")
	if s.reasoning != "low" || s.reasoningSummary != "auto" {
		t.Fatalf("reasoning=%q summary=%q", s.reasoning, s.reasoningSummary)
	}
	s.WithReasoningSummary("")
	if s.reasoningSummary != "" {
		t.Fatal("clear summary")
	}
}

func TestEmitOutputItemComplete_ignoresBadJSON(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil)
	ch := make(chan tacklr.LLMResponseChunk, 4)
	s.emitOutputItemComplete([]byte(`{`), ch, nil)
	s.emitOutputItemComplete([]byte(`{"type":"message","id":"m"}`), ch, nil)
	s.emitReasoningChunk([]byte(`{`), ch, nil) // unmarshal fail: no send
	close(ch)
	var n int
	for range ch {
		n++
	}
	if n != 1 {
		t.Fatalf("chunks = %d, want 1 message complete", n)
	}
}

func TestParseSSE_skipsNonDataAndBadJSON(t *testing.T) {
	body := strings.Join([]string{
		`event: ping`,
		`data: not-json`,
		`data: {"type":"response.output_text.delta","item_id":"m","delta":""}`,
		`data: {"type":"response.output_text.delta","item_id":"m","delta":"z"}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, body)
	var text string
	for _, c := range chunks {
		text += c.Content
	}
	if text != "z" {
		t.Fatalf("text = %q", text)
	}
}
