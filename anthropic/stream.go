package anthropic

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/provider"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// stream turns a Messages API event stream into response chunks: text
// and thinking stream as deltas, and each block completes when it stops.
type stream struct {
	ctx     context.Context
	chunks  provider.Chunks
	summary string

	id     string
	blocks map[int]*block
	usage  usage
	stop   string
	reason string // the refusal's explanation
}

// block is a content block as it streams.
type block struct {
	kind      string
	id, name  string
	text      strings.Builder
	signature string
	data      string // a redacted_thinking block's ciphertext
	input     strings.Builder
}

type usage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
}

type event struct {
	Type    string `json:"type"`
	Index   int    `json:"index"`
	Message *struct {
		ID    string `json:"id"`
		Usage usage  `json:"usage"`
	} `json:"message"`
	ContentBlock *struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Name string `json:"name"`
		Text string `json:"text"`
		Data string `json:"data"`
	} `json:"content_block"`
	Delta *struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		Thinking    string `json:"thinking"`
		Signature   string `json:"signature"`
		PartialJSON string `json:"partial_json"`
		StopReason  string `json:"stop_reason"`
		StopDetails *struct {
			Explanation string `json:"explanation"`
		} `json:"stop_details"`
	} `json:"delta"`
	Usage *usage    `json:"usage"`
	Error *apiError `json:"error"`
}

func (s *stream) read(body io.Reader) {
	sse := provider.NewSSE(body)
	s.blocks = map[int]*block{}
	for {
		data, ok := sse.Next()
		if !ok || s.ctx.Err() != nil {
			break
		}
		var ev event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			s.fail(fmt.Errorf("%w: %w", ErrMalformedStream, err))
			return
		}
		if done := s.handle(ev); done {
			return
		}
	}
	if s.ctx.Err() != nil {
		return
	}
	if err := sse.Err(); err != nil {
		s.fail(tacklr.Network(fmt.Errorf("%w: %w", ErrIncompleteStream, err)))
		return
	}
	s.fail(tacklr.Network(ErrIncompleteStream))
}

// handle applies one event. It is true when the stream ended.
func (s *stream) handle(ev event) bool {
	switch ev.Type {
	case "message_start":
		if ev.Message != nil {
			s.id, s.usage = ev.Message.ID, ev.Message.Usage
		}
	case "content_block_start":
		if cb := ev.ContentBlock; cb != nil {
			b := &block{kind: cb.Type, id: cb.ID, name: cb.Name, data: cb.Data}
			b.text.WriteString(cb.Text)
			s.blocks[ev.Index] = b
		}
	case "content_block_delta":
		s.delta(ev)
	case "content_block_stop":
		if b, ok := s.blocks[ev.Index]; ok {
			s.complete(ev.Index, b)
			delete(s.blocks, ev.Index)
		}
	case "message_delta":
		if ev.Delta != nil {
			s.stop = ev.Delta.StopReason
			if d := ev.Delta.StopDetails; d != nil {
				s.reason = d.Explanation
			}
		}
		if ev.Usage != nil {
			s.usage.OutputTokens = ev.Usage.OutputTokens
		}
	case "message_stop":
		s.finish()
		return true
	case "error":
		s.fail(classifyError(http.StatusOK, ev.Error))
		return true
	}
	return false
}

func (s *stream) delta(ev event) {
	b, ok := s.blocks[ev.Index]
	if !ok || ev.Delta == nil {
		return
	}
	d := ev.Delta
	switch d.Type {
	case "text_delta":
		b.text.WriteString(d.Text)
		s.send(session.EventMessage, ev.Index, d.Text)
	case "thinking_delta":
		b.text.WriteString(d.Thinking)
		s.send(session.EventReasoning, ev.Index, d.Thinking)
	case "signature_delta":
		b.signature += d.Signature
	case "input_json_delta":
		b.input.WriteString(d.PartialJSON)
	}
}

// send streams a delta of block index.
func (s *stream) send(t session.EventType, index int, text string) {
	if text != "" {
		s.chunks.Send(tacklr.LLMResponseChunk{Type: t, MessageId: s.blockID(index), Content: text})
	}
}

func (s *stream) blockID(index int) string {
	return fmt.Sprintf("%s:%d", s.id, index)
}

// complete sends a stopped block. A thinking block keeps its JSON as the
// API sent it, which the next request replays unchanged.
func (s *stream) complete(index int, b *block) {
	chunk := tacklr.LLMResponseChunk{MessageId: s.blockID(index), IsComplete: true}
	switch b.kind {
	case "text":
		if b.text.Len() == 0 {
			return
		}
		chunk.Type, chunk.Content = session.EventMessage, b.text.String()
	case "thinking":
		chunk.Type, chunk.Content = session.EventReasoning, b.text.String()
		chunk.EncryptedContent = string(encode(struct {
			Type      string `json:"type"`
			Thinking  string `json:"thinking"`
			Signature string `json:"signature"`
		}{"thinking", b.text.String(), b.signature}))
	case "redacted_thinking":
		chunk.Type = session.EventReasoning
		chunk.EncryptedContent = string(encode(struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}{"redacted_thinking", b.data}))
	case "tool_use":
		namespace, name, ok := strings.Cut(b.name, "__")
		if !ok {
			namespace, name = "", b.name
		}
		chunk.Type = session.EventFunctionCall
		chunk.ToolCalls = []message.ToolCall{{
			ID:        b.id,
			Type:      "tool_use",
			CallID:    b.id,
			Name:      name,
			Namespace: namespace,
			Arguments: string(toolInput(b.input.String())),
			Status:    string(message.StatusCompleted),
		}}
	default:
		return
	}
	s.chunks.Send(chunk)
}

// finish ends a stopped message: a refusal or a limit is the stream's
// error, anything else completes it with its usage.
func (s *stream) finish() {
	switch s.stop {
	case "refusal":
		s.fail(fmt.Errorf("%w: %s", session.ErrModelRefused, cmp.Or(s.reason, "the model refused")))
	case "max_tokens":
		s.fail(fmt.Errorf("%w: the response reached max_tokens", session.ErrMaxTokens))
	case "model_context_window_exceeded":
		s.fail(fmt.Errorf("%w: the response reached the context window", session.ErrContextLength))
	default:
		u := s.usage
		s.chunks.Send(tacklr.LLMResponseChunk{
			Type:       session.EventComplete,
			IsComplete: true,
			// The input is what the request sent: the uncached tokens and
			// those read from and written to the cache.
			InputTokens:      u.InputTokens + u.CacheReadInputTokens + u.CacheCreationInputTokens,
			OutputTokens:     u.OutputTokens,
			CachedTokens:     u.CacheReadInputTokens,
			CacheWriteTokens: u.CacheCreationInputTokens,
		})
	}
}

func (s *stream) fail(err error) {
	provider.EmitFailed(s.ctx, err, http.StatusOK, s.summary)
	s.chunks.Fail(err)
}
