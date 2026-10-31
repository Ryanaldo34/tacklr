package testkit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/openai"
)

// HTTPModel is the real OpenAI-compatible client pointed at a local server.
// respond sees the conversation the client sent and writes chunks. An error
// chunk becomes the HTTP error the client classifies. Anything else is SSE.
func HTTPModel(tb testing.TB, respond func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk)) tacklr.InferenceStrategy {
	tb.Helper()
	return newHTTPModel(tb, "gpt-4o", respond)
}

// TextHTTPModel is HTTPModel on a model id that accepts text only.
func TextHTTPModel(tb testing.TB, respond func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk)) tacklr.InferenceStrategy {
	tb.Helper()
	return newHTTPModel(tb, "text-only", respond)
}

func newHTTPModel(tb testing.TB, model string, respond func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk)) tacklr.InferenceStrategy {
	tb.Helper()
	// life ends when the test does. A script blocked on ctx.Done() has to
	// release the connection before httptest.Server.Close, and the workflow
	// test environment does not cancel an activity context it has abandoned.
	life, cancelLife := context.WithCancel(context.Background())
	srv := testhttp.New(tb, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/input_tokens") {
			// A message's content length is its token count, so a test sets
			// the window in characters.
			body, _ := io.ReadAll(r.Body)
			msgs, _ := messagesFromResponses(body)
			n := 0
			for _, m := range msgs {
				n += len(m.Content)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int{"input_tokens": n})
			return
		}
		if r.URL.Path != "/responses" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Every request must pair each call with its result: a provider
		// rejects one that does not.
		if bad := unpairedCalls(body); bad != "" {
			tb.Errorf("model request is not paired: %s", bad)
			writeProviderError(w, errors.New(bad))
			return
		}
		msgs, tools := messagesFromResponses(body)
		reqCtx, cancelReq := context.WithCancel(life)
		defer cancelReq()
		stop := context.AfterFunc(r.Context(), cancelReq)
		defer stop()
		ch := make(chan tacklr.LLMResponseChunk, 8)
		go func() {
			defer close(ch)
			if respond != nil {
				respond(reqCtx, msgs, tools, ch)
			}
		}()
		// A lone error chunk is an HTTP failure so the client classifies
		// refusal, max tokens, and network the same way a provider would.
		// Anything else is flushed as SSE so a caller can observe the stream
		// before the script returns.
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		started := false
		begin := func() {
			if started {
				return
			}
			started = true
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
		}
		emit := func(c tacklr.LLMResponseChunk) {
			begin()
			writeChunk(w, c)
			flush()
		}
		c, ok := <-ch
		if !ok {
			begin()
			_, _ = io.WriteString(w, completedSSE)
			flush()
			return
		}
		if c.Error != nil {
			next, ok2 := <-ch
			if !ok2 {
				writeProviderError(w, c.Error)
				return
			}
			emit(c)
			emit(next)
		} else {
			emit(c)
		}
		for c := range ch {
			emit(c)
		}
		begin()
		_, _ = io.WriteString(w, completedSSE)
		flush()
	}))
	// After the server close hook, so a blocked script is released first.
	tb.Cleanup(cancelLife)
	return openai.NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("test-key").
		WithModel(model).
		WithURL(srv.URL)
}

const completedSSE = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"

func writeProviderError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	code := "invalid_request"
	switch {
	case errors.Is(err, tacklr.ErrModelRefused):
		code = "content_filter"
	case errors.Is(err, tacklr.ErrMaxTokens):
		code = "max_tokens"
	case errors.Is(err, tacklr.ErrNetwork):
		status = http.StatusBadGateway
		code = "upstream"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"message": err.Error(), "code": code},
	})
}

func writeChunk(w http.ResponseWriter, c tacklr.LLMResponseChunk) {
	switch c.Type {
	case tacklr.StreamEventFunctionCall:
		for _, tc := range c.ToolCalls {
			name := tc.Name
			if tc.Namespace != "" {
				name = tc.Namespace + "__" + tc.Name
			}
			item, _ := json.Marshal(map[string]any{
				"type": "function_call", "id": tc.ID, "call_id": tc.CallID,
				"name": name, "arguments": tc.Arguments, "status": "completed",
			})
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"item\":"+string(item)+"}\n\n")
		}
	case tacklr.StreamEventReasoning:
		if c.Content != "" {
			delta, _ := json.Marshal(c.Content)
			_, _ = io.WriteString(w, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"delta\":"+string(delta)+"}\n\n")
		}
		if c.IsComplete {
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n")
		}
	case tacklr.StreamEventMessage:
		if c.Content != "" {
			delta, _ := json.Marshal(c.Content)
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":"+string(delta)+"}\n\n")
		}
		if c.IsComplete {
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"\"}]}}\n\n")
		}
	case tacklr.StreamEventError:
		payload, _ := json.Marshal(map[string]any{
			"type": "response.failed",
			"response": map[string]any{
				"status": "failed",
				"error":  map[string]string{"message": c.Content, "code": "server_error"},
			},
		})
		_, _ = io.WriteString(w, "data: "+string(payload)+"\n\n")
	}
}

// unpairedCalls names the first call in a Responses request without its
// result, or result without its call; "" when each call has its result.
func unpairedCalls(body []byte) string {
	var req struct {
		Input []struct {
			Type   string `json:"type"`
			CallID string `json:"call_id"`
		} `json:"input"`
	}
	if json.Unmarshal(body, &req) != nil {
		return ""
	}
	open := map[string]bool{}
	var order []string
	for _, item := range req.Input {
		switch item.Type {
		case "function_call":
			open[item.CallID] = true
			order = append(order, item.CallID)
		case "function_call_output":
			if !open[item.CallID] {
				return "result " + item.CallID + " has no call"
			}
			delete(open, item.CallID)
		}
	}
	for _, id := range order {
		if open[id] {
			return "call " + id + " has no result"
		}
	}
	return ""
}

func messagesFromResponses(body []byte) ([]*tacklr.Message, []*tacklr.Tool) {
	var req struct {
		Input json.RawMessage `json:"input"`
		Tools json.RawMessage `json:"tools"`
	}
	if json.Unmarshal(body, &req) != nil || len(req.Input) == 0 {
		return nil, toolsFromResponses(req.Tools)
	}
	var raws []json.RawMessage
	if json.Unmarshal(req.Input, &raws) != nil {
		return nil, toolsFromResponses(req.Tools)
	}
	var msgs []*tacklr.Message
	var calls []tacklr.ToolCall
	flush := func() {
		if len(calls) == 0 {
			return
		}
		msgs = append(msgs, &tacklr.Message{Role: tacklr.RoleAssistant, ToolCalls: calls})
		calls = nil
	}
	for _, raw := range raws {
		var item struct {
			Type      string          `json:"type"`
			Role      string          `json:"role"`
			Content   json.RawMessage `json:"content"`
			CallID    string          `json:"call_id"`
			Name      string          `json:"name"`
			Arguments string          `json:"arguments"`
			Output    json.RawMessage `json:"output"`
		}
		if json.Unmarshal(raw, &item) != nil {
			continue
		}
		switch {
		case item.Type == "function_call":
			ns, name := splitToolName(item.Name)
			calls = append(calls, tacklr.ToolCall{ID: item.CallID, CallID: item.CallID, Name: name, Namespace: ns, Arguments: item.Arguments})
		case item.Type == "function_call_output":
			flush()
			msgs = append(msgs, &tacklr.Message{Role: tacklr.RoleTool, Content: textContent(item.Output), ToolCallID: item.CallID})
		case item.Role != "":
			flush()
			text, parts := messageContent(item.Content)
			msgs = append(msgs, &tacklr.Message{Role: tacklr.MessageRole(item.Role), Content: text, ContentParts: parts})
		}
	}
	flush()
	return msgs, toolsFromResponses(req.Tools)
}

func toolsFromResponses(raw json.RawMessage) []*tacklr.Tool {
	var defs []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &defs) != nil {
		return nil
	}
	tools := make([]*tacklr.Tool, 0, len(defs))
	for _, d := range defs {
		if d.Name == "" {
			continue
		}
		ns, name := splitToolName(d.Name)
		tools = append(tools, tacklr.NewTool(tacklr.ToolConfig{
			Name:      name,
			Namespace: ns,
			Handler:   func(context.Context) (string, error) { return "", nil },
		}))
	}
	return tools
}

func splitToolName(name string) (namespace, short string) {
	ns, short, ok := strings.Cut(name, "__")
	if !ok || ns == "" || short == "" {
		return "", name
	}
	return ns, short
}

func messageContent(raw json.RawMessage) (string, []tacklr.ContentPart) {
	if len(raw) == 0 {
		return "", nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var parts []struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		ImageURL string `json:"image_url"`
		Detail   string `json:"detail"`
		Filename string `json:"filename"`
		FileData string `json:"file_data"`
		FileID   string `json:"file_id"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return "", nil
	}
	var b strings.Builder
	var out []tacklr.ContentPart
	for _, p := range parts {
		switch p.Type {
		case "input_text", "output_text", "":
			b.WriteString(p.Text)
		case "input_image":
			out = append(out, tacklr.ContentPart{
				Type:     tacklr.ContentTypeInputImage,
				ImageURL: &tacklr.ImageURL{URL: p.ImageURL, Detail: p.Detail},
			})
		case "input_file":
			out = append(out, tacklr.ContentPart{
				Type: tacklr.ContentTypeInputFile,
				FileData: &tacklr.FileData{
					Data:     p.FileData,
					Filename: p.Filename,
					FileID:   p.FileID,
					MIMEType: mimeFromDataURL(p.FileData),
				},
			})
		}
	}
	return b.String(), out
}

func mimeFromDataURL(s string) string {
	rest, ok := strings.CutPrefix(s, "data:")
	if !ok {
		return ""
	}
	mime, _, ok := strings.Cut(rest, ";")
	if !ok {
		mime, _, _ = strings.Cut(rest, ",")
	}
	return mime
}

func textContent(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var parts []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) == nil {
		var b strings.Builder
		for _, p := range parts {
			b.WriteString(p.Text)
		}
		return b.String()
	}
	return ""
}
