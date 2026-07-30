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

func TestCompressContextWindow_noop(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil)
	if err := s.CompressContextWindow(); err != nil {
		t.Fatal(err)
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
	})
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
	if _, err := s.Invoke(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("%v", err)
	}
	s.WithApiKey("k")
	if _, err := s.Invoke(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrModelNotSet) {
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

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil)
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

func TestWithStructuredOutput_pointerAndInvalid(t *testing.T) {
	type S struct {
		X string `json:"x"`
	}
	s := NewOpenAIInferenceStrategy(nil)
	s.WithStructuredOutput(&S{})
	if s.structuredOutputName != "S" || s.structuredOutputSchema == nil {
		t.Fatalf("schema not set: name=%q schema=%v", s.structuredOutputName, s.structuredOutputSchema)
	}
	// Non-struct (e.g. string) may disable structured output.
	s.WithStructuredOutput("nope")
}

func TestMarshalMessagesToInput_assistantToolCallsOnly(t *testing.T) {
	items, err := marshalMessagesToInput([]*tacklr.Message{
		{Role: tacklr.RoleAssistant, ToolCalls: []tacklr.ToolCall{
			{CallID: "c", Name: "n", Arguments: `{}`},
		}},
		{Role: tacklr.RoleReasoning, Content: "ignored role"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1 function_call only", len(items))
	}
	if !strings.Contains(string(items[0]), "function_call") {
		t.Fatalf("%s", items[0])
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
	s.emitOutputItemComplete([]byte(`{`), ch)
	s.emitOutputItemComplete([]byte(`{"type":"message","id":"m"}`), ch)
	s.emitReasoningChunk([]byte(`{`), ch) // unmarshal fail: no send
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
