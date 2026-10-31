package openai

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
	"github.com/ryanaldo34/tacklr/internal/testhttp"
)

func TestSupportsMIME_visionAndPDF(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil).WithModel("gpt-4o")
	if !s.SupportsMIME("text/plain") || !s.SupportsMIME("") {
		t.Fatal("text always supported")
	}
	if !s.SupportsMIME("image/png") || !s.SupportsMIME("image/jpeg") {
		t.Fatal("gpt-4o should support images")
	}
	if !s.SupportsMIME("application/pdf") {
		t.Fatal("gpt-4o should support PDF")
	}
	if s.SupportsMIME("application/zip") {
		t.Fatal("unknown binary rejected")
	}
	s2 := NewOpenAIInferenceStrategy(nil).WithModel("unknown-local-llm")
	if s2.SupportsMIME("image/png") || s2.SupportsMIME("application/pdf") {
		t.Fatal("unknown model rejects binary")
	}
	if !s2.SupportsMIME("text/markdown") {
		t.Fatal("text/* always true")
	}
	s3 := NewOpenAIInferenceStrategy(nil)
	if s3.SupportsMIME("image/png") {
		t.Fatal("empty model rejects image")
	}
}

func TestMaxContextWindow_knownPrefixAndUnknown(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil).WithModel("gpt-5.4")
	n, err := s.MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5.4: n=%d err=%v", n, err)
	}
	n, err = NewOpenAIInferenceStrategy(nil).WithModel("o3-custom").MaxContextWindow()
	if err != nil || n != 200000 {
		t.Fatalf("o3 prefix: n=%d err=%v", n, err)
	}
	n, err = NewOpenAIInferenceStrategy(nil).WithModel("gpt-5-preview").MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5 prefix: n=%d err=%v", n, err)
	}
	_, err = NewOpenAIInferenceStrategy(nil).WithModel("mystery-model").MaxContextWindow()
	if err == nil || !errors.Is(err, tacklr.ErrUnknownModel) {
		t.Fatalf("unknown: err=%v", err)
	}
}

func TestUnsupportedMIMEs(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil).WithModel("unknown")
	bad := tacklr.UnsupportedMIMEs(s, []string{"text/plain", "image/png", "image/png", "application/pdf"})
	if len(bad) != 2 {
		t.Fatalf("bad=%v", bad)
	}
}

func TestInvoke_truncatedStreamDoesNotCommitPartialAssistant(t *testing.T) {
	// Arrange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/responses/input_tokens") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"input_tokens":1}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","item_id":"msg","delta":"partial"}`+"\n")
	}))
	t.Cleanup(server.Close)
	strategy := NewOpenAIInferenceStrategy(server.Client()).
		WithApiKey("test-key").
		WithModel("gpt-5.4").
		WithURL(server.URL)
	ch, err := strategy.Invoke(t.Context(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hello"}}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	var committed string
	for ev := range ch {
		if ev.Type == tacklr.StreamEventError {
			streamErr = ev.Error
		}
		if ev.Type == tacklr.StreamEventMessage && ev.IsComplete {
			committed += ev.Content
		}
	}
	if !errors.Is(streamErr, ErrIncompleteStream) {
		t.Fatalf("stream error = %v", streamErr)
	}
	if committed != "" {
		t.Fatalf("committed partial assistant %q", committed)
	}
}

func TestCountTokens_404WithoutLocalFallback_returnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"nope"}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	_, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "count these tokens please"},
	}, nil)
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
		t.Fatalf("want 404 API error, got %v", err)
	}
}

func TestCountTokens_fallsBackToTiktokenOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"nope"}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL).
		WithLocalTokenFallback()

	n, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "count these tokens please"},
	}, nil)
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected positive tiktoken fallback count, got %d", n)
	}
}

func TestInvoke_contextCancel_stopsStream(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		// Slow infinite stream until client disconnects.
		for i := 0; i < 1000; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","item_id":"m","delta":"x"}`+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Invoke(ctx, []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start")
	}
	// Read at least one chunk then cancel.
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no chunks")
	}
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel closed — success
			}
		case <-deadline:
			t.Fatal("stream did not end after cancel")
		}
	}
}

func TestInvoke_namespacedToolCallOnTheWire(t *testing.T) {
	var saw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"type":"response.completed","response":{"status":"completed"}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).WithApiKey("k").WithModel("m").WithURL(srv.URL)
	s.WithReasoningLevel("high")
	tool := tacklr.NewTool(tacklr.ToolConfig{
		Name: "greet", Namespace: "ado",
		Handler: func(ctx context.Context) (string, error) { return "", nil },
	})
	ch, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "hi"},
		{Role: tacklr.RoleAssistant, ToolCalls: []tacklr.ToolCall{
			{CallID: "call_1", Name: "greet", Namespace: "ado"},
			{ID: "fc_item", CallID: "call_2", Name: "greet", Namespace: "ado", Arguments: `{}`},
		}},
		{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: "ok"},
		{Role: tacklr.RoleTool, ToolCallID: "call_2", Content: "ok2"},
		{Role: tacklr.RoleTool, ToolCallID: "fc_ghost", Content: "nope"},
		{
			Role:             tacklr.RoleReasoning,
			MessageID:        "rs_store",
			EncryptedContent: "gAAAAABcipher",
			Content:          "plan",
		},
		{Role: tacklr.RoleReasoning, MessageID: "rs_orphan", Content: "after"},
	}, []*tacklr.Tool{tool}, "")
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}

	toolsRaw, _ := json.Marshal(saw["tools"])
	if !strings.Contains(string(toolsRaw), `"name":"ado__greet"`) {
		t.Fatalf("tools = %s", toolsRaw)
	}
	inRaw, _ := json.Marshal(saw["input"])
	inStr := string(inRaw)
	if !strings.Contains(inStr, `"name":"ado__greet"`) || !strings.Contains(inStr, `"arguments":"{}"`) {
		t.Fatalf("input missing namespaced call: %s", inStr)
	}
	if strings.Contains(inStr, `"id":"fc_item"`) || strings.Contains(inStr, "fc_ghost") || strings.Contains(inStr, "nope") {
		t.Fatalf("must not emit item id or orphan output: %s", inStr)
	}
	if !strings.Contains(inStr, `"encrypted_content":"gAAAAABcipher"`) {
		t.Fatalf("missing ciphertext: %s", inStr)
	}
	if strings.Contains(inStr, "rs_orphan") {
		t.Fatalf("orphan reasoning must omit id: %s", inStr)
	}
	reasoning, _ := saw["reasoning"].(map[string]any)
	if reasoning["effort"] != "high" || reasoning["summary"] != "auto" {
		t.Fatalf("reasoning = %v", saw["reasoning"])
	}
}

func TestInvoke_requiresKeyAndModel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	if _, err := s.Invoke(context.Background(), nil, nil, ""); !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("%v", err)
	}
	s.WithApiKey("k")
	if _, err := s.Invoke(context.Background(), nil, nil, ""); !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Fatalf("%v", err)
	}
	if _, err := s.CountTokens(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Fatalf("count: %v", err)
	}
}

func TestInvoke_promptCacheGrokCompat(t *testing.T) {
	var saw map[string]any
	var grokConv string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		grokConv = r.Header.Get("x-grok-conv-id")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":1,"input_tokens_details":{"cached_tokens":8}}}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("grok-4.6").
		WithURL(srv.URL)
	s.CacheKey = "sess-grok-1"
	s.SetSystemPrompt("stable system")

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "ask"},
		{Role: tacklr.RoleDeveloper, Content: "PROJECT PLAN\n────────────\nDo the work"},
	}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var usage tacklr.LLMResponseChunk
	for c := range ch {
		if c.Type == tacklr.StreamEventComplete {
			usage = c
		}
		if c.Type == tacklr.StreamEventError {
			t.Fatalf("stream: %s", c.Content)
		}
	}
	if usage.CachedTokens != 8 {
		t.Fatalf("cached = %d", usage.CachedTokens)
	}
	if grokConv != "sess-grok-1" {
		t.Fatalf("x-grok-conv-id = %q", grokConv)
	}
	if saw["prompt_cache_key"] != "sess-grok-1" {
		t.Fatalf("prompt_cache_key = %v", saw["prompt_cache_key"])
	}
	if _, ok := saw["prompt_cache_options"]; ok {
		t.Fatalf("xAI must not receive prompt_cache_options: %v", saw["prompt_cache_options"])
	}
	inRaw, _ := json.Marshal(saw["input"])
	if strings.Contains(string(inRaw), "prompt_cache_breakpoint") {
		t.Fatalf("xAI must not receive breakpoints: %s", inRaw)
	}
}

func TestInvoke_promptCacheGPT56BreakpointsAndToolChoiceNone(t *testing.T) {
	var saw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":20,"output_tokens":2,"input_tokens_details":{"cached_tokens":12,"cache_write_tokens":8}}}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("gpt-5.6").
		WithURL(srv.URL)
	s.CacheKey = "sess-oai-1"
	tool := tacklr.NewTool(tacklr.ToolConfig{Name: "echo", Handler: func(context.Context) (string, error) { return "ok", nil }})

	ctx := tacklr.ContextWithToolChoiceNone(context.Background())
	ch, err := s.Invoke(ctx, []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "ask"},
		{Role: tacklr.RoleDeveloper, Content: "PROJECT PLAN\n────────────\nDo the work"},
		{Role: tacklr.RoleAssistant, ToolCalls: []tacklr.ToolCall{
			{ID: "call_1", CallID: "call_1", Name: "echo", Arguments: "{}"},
		}},
		{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: "tool-out"},
	}, []*tacklr.Tool{tool}, "stable system")
	if err != nil {
		t.Fatal(err)
	}
	var usage tacklr.LLMResponseChunk
	for c := range ch {
		if c.Type == tacklr.StreamEventComplete {
			usage = c
		}
		if c.Type == tacklr.StreamEventError {
			t.Fatalf("stream: %s", c.Content)
		}
	}
	if usage.CachedTokens != 12 || usage.CacheWriteTokens != 8 {
		t.Fatalf("usage cached=%d write=%d", usage.CachedTokens, usage.CacheWriteTokens)
	}
	if saw["prompt_cache_key"] != "sess-oai-1" {
		t.Fatalf("prompt_cache_key = %v", saw["prompt_cache_key"])
	}
	opts, _ := saw["prompt_cache_options"].(map[string]any)
	if opts["mode"] != "implicit" || opts["ttl"] != "30m" {
		t.Fatalf("prompt_cache_options = %v", saw["prompt_cache_options"])
	}
	if saw["tool_choice"] != "none" {
		t.Fatalf("tool_choice = %v", saw["tool_choice"])
	}
	inRaw, _ := json.Marshal(saw["input"])
	if strings.Count(string(inRaw), `"prompt_cache_breakpoint"`) < 3 {
		t.Fatalf("want breakpoints on system, plan, last tool output: %s", inRaw)
	}
}

func TestInvoke_openRouterStrategySendsCacheKeyOnly(t *testing.T) {
	var saw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":5,"output_tokens":1}}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	// A model string that contains the old GPT-5.6 trigger must still get the
	// OpenRouter key-only profile: the strategy decides, not the model name.
	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("openai/gpt-5.6").
		WithURL(srv.URL)
	s.CacheKey = "sess-openrouter-1"

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "ask"},
	}, nil, "stable system")
	if err != nil {
		t.Fatal(err)
	}
	for c := range ch {
		if c.Type == tacklr.StreamEventError {
			t.Fatalf("stream: %s", c.Content)
		}
	}
	if _, ok := saw["prompt_cache_options"]; ok {
		t.Fatalf("OpenRouter must not receive prompt_cache_options: %v", saw["prompt_cache_options"])
	}
	if saw["prompt_cache_key"] != "sess-openrouter-1" {
		t.Fatalf("prompt_cache_key = %v", saw["prompt_cache_key"])
	}
	inRaw, _ := json.Marshal(saw["input"])
	if strings.Contains(string(inRaw), "prompt_cache_breakpoint") {
		t.Fatalf("OpenRouter must not receive breakpoints: %s", inRaw)
	}
}

func TestPromptCache_grokAndGPTShapes(t *testing.T) {
	grok := newPromptCache("grok-4.6", "https://api.x.ai/v1", "")
	if grok.breakpoints() {
		t.Fatal("grok breakpoints")
	}
	req := &responsesRequest{}
	h := make(http.Header)
	grok.apply(req)
	grok.headers(h)
	if req.PromptCache != nil || h.Get("x-grok-conv-id") != "" {
		t.Fatalf("empty grok key leaked: %+v %v", req.PromptCache, h)
	}
	newPromptCache("grok-4.6", "https://us.api.x.ai/v1", "sess").headers(h)
	if h.Get("x-grok-conv-id") != "sess" {
		t.Fatalf("header = %q", h.Get("x-grok-conv-id"))
	}

	gpt := newPromptCache("gpt-5.6", "https://api.openai.com/v1", "sess")
	if !gpt.breakpoints() {
		t.Fatal("gpt breakpoints")
	}
	req = &responsesRequest{}
	gpt.apply(req)
	gpt.headers(h)
	if req.PromptCacheKey != "sess" || req.PromptCache == nil || req.PromptCache.Mode != "implicit" || req.PromptCache.TTL != "30m" {
		t.Fatalf("gpt cache = %+v", req)
	}

	s := NewOpenAIInferenceStrategy(nil).WithModel("gpt-5.6-sol")
	s.SetPromptCacheKey("  abc  ")
	if s.CacheKey != "abc" {
		t.Fatalf("CacheKey = %q", s.CacheKey)
	}
	n, err := s.MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5 window = %d %v", n, err)
	}
	s.WithModel("o3-mini")
	n, err = s.MaxContextWindow()
	if err != nil || n != 200000 {
		t.Fatalf("o3 window = %d %v", n, err)
	}
	s.WithReasoningSummary("auto")
	s.WithReasoningSummary("")

	orphans := marshalMessagesToInput([]*tacklr.Message{
		{Role: tacklr.RoleTool, ToolCallID: "orphan", Content: "x"},
	}, "", false)
	if len(orphans) != 0 {
		t.Fatalf("orphan tool output = %d", len(orphans))
	}
	ns := marshalMessagesToInput([]*tacklr.Message{
		{Role: tacklr.RoleAssistant, ToolCalls: []tacklr.ToolCall{
			{ID: "call_1", CallID: "call_1", Name: "echo", Namespace: "svc", Arguments: "{}"},
		}},
		{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: "ok"},
	}, "sys", true)
	joined := string(ns[0]) + string(ns[1]) + string(ns[2])
	if !strings.Contains(joined, "svc__echo") || !strings.Contains(joined, "prompt_cache_breakpoint") {
		t.Fatalf("namespaced call = %s", joined)
	}
}

type failingSSEReader struct{}

func (failingSSEReader) Read([]byte) (int, error) {
	return 0, errors.New("stream read failed")
}

func collectSSE(t *testing.T, body string) []tacklr.LLMResponseChunk {
	t.Helper()
	if strings.Contains(body, "data: [DONE]") &&
		!strings.Contains(body, `"type":"response.completed"`) &&
		!strings.Contains(body, `"type":"response.incomplete"`) &&
		!strings.Contains(body, `"type":"response.failed"`) &&
		!strings.Contains(body, `"type":"error"`) {
		body = strings.Replace(body, "data: [DONE]",
			`data: {"type":"response.completed","response":{"status":"completed"}}`+"\n"+`data: [DONE]`, 1)
	}
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/input_tokens") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"input_tokens":1}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	s := NewOpenAIInferenceStrategy(srv.Client()).WithApiKey("test-key").WithModel("gpt-4o").WithURL(srv.URL)
	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		return []tacklr.LLMResponseChunk{{Type: tacklr.StreamEventError, Error: err, Content: err.Error(), IsComplete: true}}
	}
	var out []tacklr.LLMResponseChunk
	for c := range ch {
		out = append(out, c)
	}
	return out
}

func collectRawSSE(t *testing.T, reader io.Reader) []tacklr.LLMResponseChunk {
	t.Helper()
	strategy := NewOpenAIInferenceStrategy(nil)
	ch := make(chan tacklr.LLMResponseChunk, 16)
	strategy.parseSSEResponse(t.Context(), reader, ch, "")
	close(ch)
	var chunks []tacklr.LLMResponseChunk
	for chunk := range ch {
		chunks = append(chunks, chunk)
	}
	return chunks
}

func TestParseSSE_requiresTerminalResponseEvent(t *testing.T) {
	// Arrange
	doneWithoutTerminal := strings.NewReader("data: [DONE]\n\n")
	eofWithoutTerminal := strings.NewReader(`data: {"type":"response.output_text.delta","delta":"partial"}` + "\n")
	scannerFailure := io.MultiReader(
		strings.NewReader(`data: {"type":"response.output_text.delta","delta":"partial"}`+"\n"),
		failingSSEReader{},
	)

	// Act
	doneChunks := collectRawSSE(t, doneWithoutTerminal)
	eofChunks := collectRawSSE(t, eofWithoutTerminal)
	failureChunks := collectRawSSE(t, scannerFailure)

	// Assert
	for name, chunks := range map[string][]tacklr.LLMResponseChunk{
		"done":    doneChunks,
		"eof":     eofChunks,
		"scanner": failureChunks,
	} {
		if len(chunks) == 0 || !errors.Is(chunks[len(chunks)-1].Error, ErrIncompleteStream) {
			t.Fatalf("%s chunks = %#v", name, chunks)
		}
	}

	malformed := collectSSE(t, "data: not-json\ndata: [DONE]\n\n")
	if len(malformed) != 1 || !errors.Is(malformed[0].Error, ErrMalformedStream) {
		t.Fatalf("malformed = %#v", malformed)
	}
}

func TestParseSSE_outputTextAlwaysMessage_likeMain(t *testing.T) {
	// DeepSeek/Foundry thinking often arrives as output_text on a reasoning or
	// message item. Main always classifies output_text as StreamEventMessage so
	// ACP agent_message_chunk carries the stream (client demuxes <think> etc.).
	body := strings.Join([]string{
		`data: {"type":"response.output_item.added","item":{"id":"rs_1","type":"reasoning"}}`,
		`data: {"type":"response.output_text.delta","item_id":"rs_1","delta":"<think>internal"}`,
		`data: {"type":"response.output_text.delta","item_id":"rs_1","delta":"</think>"}`,
		`data: {"type":"response.output_item.done","item":{"id":"rs_1","type":"reasoning"}}`,
		`data: {"type":"response.output_item.added","item":{"id":"msg_1","type":"message"}}`,
		`data: {"type":"response.output_text.delta","item_id":"msg_1","delta":"Hello"}`,
		`data: {"type":"response.output_item.done","item":{"id":"msg_1","type":"message"}}`,
		`data: [DONE]`,
		"",
	}, "\n")

	chunks := collectSSE(t, body)
	var messages []string
	for _, c := range chunks {
		if c.IsComplete || c.Content == "" {
			continue
		}
		if c.Type != tacklr.StreamEventMessage {
			t.Fatalf("output_text must be StreamEventMessage (main behavior), got type=%v content=%q", c.Type, c.Content)
		}
		messages = append(messages, c.Content)
	}
	if got := strings.Join(messages, ""); got != "<think>internal</think>Hello" {
		t.Fatalf("messages = %q", got)
	}
}

// TestParseSSE_incompleteFailedAndRefusal is one stream that covers terminal
// incomplete/failed classification and refusal-only completed messages.
func TestParseSSE_incompleteFailedAndRefusal(t *testing.T) {
	// Incomplete with max tokens reason.
	bodyIncomplete := strings.Join([]string{
		`data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, bodyIncomplete)
	if len(chunks) == 0 || chunks[0].Error == nil {
		t.Fatalf("incomplete chunks = %+v", chunks)
	}
	if !errors.Is(chunks[0].Error, tacklr.ErrMaxTokens) {
		t.Fatalf("want ErrMaxTokens, got %v", chunks[0].Error)
	}

	// Failed with content filter at top level.
	bodyFailed := strings.Join([]string{
		`data: {"type":"response.failed","incomplete_details":{"reason":"content_filter"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyFailed)
	if len(chunks) == 0 || !errors.Is(chunks[0].Error, tacklr.ErrModelRefused) {
		t.Fatalf("failed chunks = %+v", chunks)
	}

	// Incomplete without classifiable reason still errors (enriched body).
	bodyBare := strings.Join([]string{
		`data: {"type":"response.incomplete","response":{"status":"incomplete"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyBare)
	if len(chunks) == 0 || chunks[0].Error == nil {
		t.Fatalf("bare incomplete = %+v", chunks)
	}
	if !strings.Contains(chunks[0].Error.Error(), "status=incomplete") {
		t.Fatalf("want status in error, got %v", chunks[0].Error)
	}

	// response.completed with incomplete status is also terminal.
	bodyCompletedInc := strings.Join([]string{
		`data: {"type":"response.completed","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyCompletedInc)
	if len(chunks) == 0 || !errors.Is(chunks[0].Error, tacklr.ErrMaxTokens) {
		t.Fatalf("completed+incomplete = %+v", chunks)
	}

	// Successful response.completed must not emit an error chunk.
	bodyOK := strings.Join([]string{
		`data: {"type":"response.completed","response":{"status":"completed"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyOK)
	for _, c := range chunks {
		if c.Type == tacklr.StreamEventError {
			t.Fatalf("successful completed should not error: %+v", c)
		}
	}

	// Mid-stream error events fail closed with a terminal error chunk.
	bodyStreamError := strings.Join([]string{
		`data: {"type":"error","error":{"message":"provider failed mid stream"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyStreamError)
	if len(chunks) == 0 || chunks[len(chunks)-1].Type != tacklr.StreamEventError {
		t.Fatalf("stream error chunks = %+v", chunks)
	}

	// Completed responses surface token usage for telemetry.
	bodyUsage := strings.Join([]string{
		`data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":5,"output_tokens_details":{"reasoning_tokens":2}}}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyUsage)
	var complete *tacklr.LLMResponseChunk
	for i := range chunks {
		if chunks[i].Type == tacklr.StreamEventComplete {
			complete = &chunks[i]
			break
		}
	}
	if complete == nil || complete.InputTokens != 10 || complete.OutputTokens != 5 || complete.ReasoningTokens != 2 {
		t.Fatalf("usage chunk = %+v all = %+v", complete, chunks)
	}

	// Failed with provider error object → classifyAPIStatus path.
	bodyErrObj := strings.Join([]string{
		`data: {"type":"response.failed","error":{"code":"content_filter","message":"blocked by filter","type":"invalid_request_error"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyErrObj)
	if len(chunks) == 0 || !errors.Is(chunks[0].Error, tacklr.ErrModelRefused) {
		t.Fatalf("failed+error object = %+v", chunks)
	}

	// Nested response.error on incomplete (not incomplete_details).
	bodyRespErr := strings.Join([]string{
		`data: {"type":"response.failed","response":{"error":{"code":"content_filter","message":"nested block"}}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyRespErr)
	if len(chunks) == 0 || chunks[0].Error == nil {
		t.Fatalf("nested response.error = %+v", chunks)
	}

	// Refusal-only message complete → ErrModelRefused with refusal text.
	bodyRefusal := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"id":"msg_r","type":"message","status":"completed","content":[{"type":"refusal","refusal":"I cannot help with that"}]}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyRefusal)
	if len(chunks) != 1 || chunks[0].Type != tacklr.StreamEventError {
		t.Fatalf("refusal chunks = %+v", chunks)
	}
	if !errors.Is(chunks[0].Error, tacklr.ErrModelRefused) {
		t.Fatalf("err = %v", chunks[0].Error)
	}
	if !strings.Contains(chunks[0].Content, "I cannot help") {
		t.Fatalf("content = %q", chunks[0].Content)
	}

	// Mixed content with real text is not a refusal (complete as message).
	bodyMixed := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"id":"msg_m","type":"message","content":[{"type":"refusal","refusal":"x"},{"type":"output_text","text":"hi"}]}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyMixed)
	if len(chunks) != 1 || chunks[0].Type != tacklr.StreamEventMessage || chunks[0].Error != nil {
		t.Fatalf("mixed = %+v", chunks)
	}

	// Empty refusal type with no text → default refusal text path.
	bodyEmptyRefusal := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"id":"msg_e","type":"message","content":[{"type":"refusal","refusal":""}]}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyEmptyRefusal)
	if len(chunks) != 1 || !errors.Is(chunks[0].Error, tacklr.ErrModelRefused) {
		t.Fatalf("empty refusal = %+v", chunks)
	}
	if !strings.Contains(chunks[0].Content, "model refused") {
		t.Fatalf("default refusal text missing: %q", chunks[0].Content)
	}

	// Reasoning delta + summary delta channels.
	bodyReason := strings.Join([]string{
		`data: {"type":"response.reasoning_text.delta","item_id":"rs","delta":"think"}`,
		`data: {"type":"response.reasoning_summary_text.delta","item_id":"rs","delta":"sum"}`,
		`data: {"type":"response.output_item.done","item":{"id":"rs","type":"reasoning"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyReason)
	var thought string
	for _, c := range chunks {
		if c.Type == tacklr.StreamEventReasoning {
			thought += c.Content
		}
	}
	if thought != "thinksum" {
		t.Fatalf("thought = %q", thought)
	}

	// Reasoning completion can arrive only on output_item.done summary payload.
	bodyReasonSummary := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"id":"rs2","type":"reasoning","summary":[{"type":"summary_text","text":"final summary"}]}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks = collectSSE(t, bodyReasonSummary)
	var summary string
	for _, c := range chunks {
		if c.Type == tacklr.StreamEventReasoning && c.Content != "" {
			summary = c.Content
		}
	}
	if summary != "final summary" {
		t.Fatalf("summary = %q chunks = %+v", summary, chunks)
	}

	// Context cancel stops mid-parse.
	s := NewOpenAIInferenceStrategy(nil)
	ch := make(chan tacklr.LLMResponseChunk, 8)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.parseSSEResponse(ctx, strings.NewReader(bodyReason), ch, "")
	close(ch)
	for range ch {
	}
}

func TestParseSSE_functionCall_incompleteNotComplete(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"type":"function_call","status":"incomplete","arguments":"{","call_id":"fc_x","name":"echo"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, body)
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d, want 1", len(chunks))
	}
	if chunks[0].IsComplete {
		t.Error("incomplete status must not set IsComplete")
	}
	if chunks[0].ToolCalls[0].ID != "fc_x" {
		t.Errorf("ID = %q, want normalized fc_x", chunks[0].ToolCalls[0].ID)
	}
}
