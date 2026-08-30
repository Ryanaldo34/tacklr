package inprocess

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

type hostMail struct{}

func (hostMail) Kind() builtins.ProviderKind { return builtins.ProviderGmail }
func (hostMail) Validate(context.Context) error {
	return nil
}
func (hostMail) ReadInbox(context.Context, builtins.ReadInboxRequest) (builtins.Inbox, error) {
	return builtins.Inbox{}, nil
}
func (hostMail) SendEmail(context.Context, builtins.SendEmailRequest) (builtins.SentEmail, error) {
	return builtins.SentEmail{}, nil
}

// TestPrompt_hostPickedBuiltinToolsReachTheModel: tools constructed from
// package builtins and listed on AgentOptions.Tools are registered for the turn.
func TestPrompt_hostPickedBuiltinToolsReachTheModel(t *testing.T) {
	ctx := t.Context()
	mail := hostMail{}
	exa := builtins.NewExa("test-key")
	var names []string
	model := &testkit.ScriptedModel{
		InvokeFn: func(_ context.Context, _ []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			names = make([]string, 0, len(tools))
			for _, tool := range tools {
				names = append(names, tool.Name())
			}
			ch <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventMessage,
				Content:    "ok",
				IsComplete: true,
			}
		},
	}
	rt := New(Config{Catalog: newCatalog(t, model, durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Tools: []*tacklr.Tool{
				builtins.ReadInbox(mail),
				builtins.SendEmail(mail),
				builtins.WebSearch(exa),
				builtins.WebFetch(exa),
			},
		},
	}), Projection: vfs.DirectProjection{}})

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	_ = waitEvents(t, rt, id, sub, 5*time.Second)

	for _, want := range []string{"read_inbox", "send_email", "web_search", "web_fetch"} {
		if !slices.Contains(names, want) {
			t.Fatalf("model tools %v missing %q", names, want)
		}
	}
}
