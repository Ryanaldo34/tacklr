package testkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/openai"
)

// HTTPModel is the real OpenAI-compatible client pointed at a local server.
// respond sees the conversation the client sent and writes chunks. An error
// chunk becomes the HTTP error the client classifies. Anything else is SSE.
func HTTPModel(tb testing.TB, respond func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk)) tacklr.Model {
	tb.Helper()
	return newHTTPModel(tb, "gpt-4o", respond, contentTokens)
}

// TextHTTPModel is HTTPModel on a model id that accepts text only.
func TextHTTPModel(tb testing.TB, respond func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk)) tacklr.Model {
	tb.Helper()
	return newHTTPModel(tb, "text-only", respond, contentTokens)
}

// CountedHTTPModel is HTTPModel whose provider counts a request's tokens
// with count. msgs is the request as the provider got it: the system
// prompt is its first message. The token-count endpoint answers the count,
// or HTTP 500 when count fails, and each response reports the count as its
// input tokens.
func CountedHTTPModel(tb testing.TB, respond func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk), count func(msgs []*message.Message) (int, error)) tacklr.Model {
	tb.Helper()
	return newHTTPModel(tb, "gpt-4o", respond, count)
}

// contentTokens counts a message's content length as its tokens, so a
// test sets the window in characters.
func contentTokens(msgs []*message.Message) (int, error) {
	return ContentTokenEstimate(msgs), nil
}

func newHTTPModel(tb testing.TB, model string, respond func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk), count func(msgs []*message.Message) (int, error)) tacklr.Model {
	tb.Helper()
	// inputTokens is a request's token count; zero when count fails.
	inputTokens := func(body []byte) int {
		msgs, _ := messagesFromResponses(body)
		n, _ := count(msgs)
		return n
	}
	// life ends when the test does. A script blocked on ctx.Done() has to
	// release the connection before httptest.Server.Close, and the workflow
	// test environment does not cancel an activity context it has abandoned.
	life, cancelLife := context.WithCancel(context.Background())
	srv := testhttp.New(tb, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/input_tokens") {
			body, _ := io.ReadAll(r.Body)
			msgs, _ := messagesFromResponses(body)
			n, err := count(msgs)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int{"input_tokens": n})
			return
		}
		// The client asks for a token count or a response; this is a response.
		body, _ := io.ReadAll(r.Body)
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
		// An error chunk first is an HTTP failure so the client classifies
		// refusal, max tokens, and network the same way a provider would.
		// Anything else is flushed as SSE so a caller can observe the stream
		// before the script returns; an error chunk after it fails the
		// stream.
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
			_, _ = io.WriteString(w, completedSSE(inputTokens(body)))
			flush()
			return
		}
		if c.Error != nil {
			writeProviderError(w, c.Error)
			cancelReq()
			for range ch {
			}
			return
		}
		emit(c)
		for c := range ch {
			emit(c)
		}
		begin()
		_, _ = io.WriteString(w, completedSSE(inputTokens(body)))
		flush()
	}))
	// After the server close hook, so a blocked script is released first.
	tb.Cleanup(cancelLife)
	return openai.New(srv.Client()).
		WithApiKey("test-key").
		WithModel(model).
		WithURL(srv.URL)
}

// completedSSE ends a response, reporting its input tokens as a provider
// does: the same count the token-count endpoint gives for the request.
func completedSSE(inputTokens int) string {
	return fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":%d,\"output_tokens\":1}}}\n\n", inputTokens)
}

func writeProviderError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	code := "invalid_request"
	switch {
	case errors.Is(err, session.ErrModelRefused):
		code = "content_filter"
	case errors.Is(err, session.ErrMaxTokens):
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
	case session.EventFunctionCall:
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
	case session.EventReasoning:
		if c.Content != "" {
			delta, _ := json.Marshal(c.Content)
			_, _ = io.WriteString(w, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"delta\":"+string(delta)+"}\n\n")
		}
		if c.IsComplete {
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n")
		}
	case session.EventMessage:
		if c.Content != "" {
			delta, _ := json.Marshal(c.Content)
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":"+string(delta)+"}\n\n")
		}
		if c.IsComplete {
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"\"}]}}\n\n")
		}
	case session.EventError:
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
	_ = json.Unmarshal(body, &req)
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

func messagesFromResponses(body []byte) ([]*message.Message, []*tacklr.Tool) {
	var req struct {
		Input json.RawMessage `json:"input"`
		Tools json.RawMessage `json:"tools"`
	}
	_ = json.Unmarshal(body, &req)
	var raws []json.RawMessage
	_ = json.Unmarshal(req.Input, &raws)
	var msgs []*message.Message
	var calls []message.ToolCall
	flush := func() {
		if len(calls) == 0 {
			return
		}
		msgs = append(msgs, &message.Message{Role: message.RoleAssistant, ToolCalls: calls})
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
		// An item that does not decode matches no case.
		_ = json.Unmarshal(raw, &item)
		switch {
		case item.Type == "function_call":
			ns, name := splitToolName(item.Name)
			calls = append(calls, message.ToolCall{ID: item.CallID, CallID: item.CallID, Name: name, Namespace: ns, Arguments: item.Arguments})
		case item.Type == "function_call_output":
			flush()
			msgs = append(msgs, &message.Message{Role: message.RoleTool, Content: textContent(item.Output), ToolCallID: item.CallID})
		case item.Role != "":
			flush()
			text, parts := messageContent(item.Content)
			msgs = append(msgs, &message.Message{Role: message.Role(item.Role), Content: text, ContentParts: parts})
		}
	}
	flush()
	return msgs, toolsFromResponses(req.Tools)
}

func toolsFromResponses(raw json.RawMessage) []*tacklr.Tool {
	var defs []struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(raw, &defs)
	tools := make([]*tacklr.Tool, 0, len(defs))
	for _, d := range defs {
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

func messageContent(raw json.RawMessage) (string, []message.ContentPart) {
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
	_ = json.Unmarshal(raw, &parts)
	var b strings.Builder
	var out []message.ContentPart
	for _, p := range parts {
		switch p.Type {
		case "input_text", "output_text", "":
			b.WriteString(p.Text)
		case "input_image":
			out = append(out, message.ContentPart{
				Type:     message.ContentTypeInputImage,
				ImageURL: &message.ImageURL{URL: p.ImageURL, Detail: p.Detail},
			})
		case "input_file":
			out = append(out, message.ContentPart{
				Type: message.ContentTypeInputFile,
				FileData: &message.FileData{
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

// mimeFromDataURL is the MIME type of a data URL the client sent:
// data:<mime>[;base64],<data>.
func mimeFromDataURL(s string) string {
	mime, _, _ := strings.Cut(strings.TrimPrefix(s, "data:"), ",")
	mime, _, _ = strings.Cut(mime, ";")
	return mime
}

func textContent(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var parts []struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(raw, &parts)
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.Text)
	}
	return b.String()
}
