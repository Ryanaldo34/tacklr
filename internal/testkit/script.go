package testkit

import (
	"context"
	"fmt"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// Reply is what a scripted model answers to a conversation.
type Reply func(msgs []*message.Message) tacklr.LLMResponseChunk

// ScriptedModel is HTTPModel answering each request with reply.
func ScriptedModel(tb testing.TB, reply Reply) tacklr.Model {
	tb.Helper()
	return HTTPModel(tb, func(_ context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		ch <- reply(msgs)
	})
}

// Text is a reply of the whole message s.
func Text(s string) tacklr.LLMResponseChunk {
	return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: s, IsComplete: true}
}

// Fail is a reply that fails the request with err.
func Fail(err error) tacklr.LLMResponseChunk {
	return tacklr.LLMResponseChunk{Type: session.EventError, Error: err}
}

// Call is a call of tool name with args, by id.
func Call(id, name, args string) message.ToolCall {
	return message.ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}

// Calls is a reply that makes calls.
func Calls(calls ...message.ToolCall) tacklr.LLMResponseChunk {
	return tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: calls}
}

// CallThenAnswer calls tool with args, and answers "done: " and the tool's
// result once the call has one.
func CallThenAnswer(tool, args string) Reply {
	return func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if last := LastMessage(msgs); last != nil && last.Role == message.RoleTool {
			return Text("done: " + last.Content)
		}
		return Calls(Call(fmt.Sprintf("call-%d", len(msgs)), tool, args))
	}
}

// LastMessage is the last message of msgs, or nil.
func LastMessage(msgs []*message.Message) *message.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil {
			return msgs[i]
		}
	}
	return nil
}

// ToolResults are the results of the tool messages in msgs, in order.
func ToolResults(msgs []*message.Message) []string {
	var out []string
	for _, m := range msgs {
		if m != nil && m.Role == message.RoleTool {
			out = append(out, m.Content)
		}
	}
	return out
}
