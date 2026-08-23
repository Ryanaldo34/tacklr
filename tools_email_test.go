package tacklr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mail "github.com/ryanaldo34/tacklr/email"
)

type fakeEmailProvider struct {
	kind        mail.ProviderKind
	validateErr error
	validated   int
	readRequest mail.ReadInboxRequest
	sendRequest mail.SendEmailRequest
	readErr     error
	sendErr     error
}

func (p *fakeEmailProvider) Kind() mail.ProviderKind { return p.kind }

func (p *fakeEmailProvider) Validate(context.Context) error {
	p.validated++
	return p.validateErr
}

func (p *fakeEmailProvider) ReadInbox(_ context.Context, req mail.ReadInboxRequest) (mail.Inbox, error) {
	p.readRequest = req
	if p.readErr != nil {
		return mail.Inbox{}, p.readErr
	}
	return mail.Inbox{
		Messages: []mail.Message{{
			ID: "message-1", From: "sender@example.com", Subject: "Status",
			ReceivedAt: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC), Unread: true,
		}},
		NextCursor: "page-2",
	}, nil
}

func (p *fakeEmailProvider) SendEmail(_ context.Context, req mail.SendEmailRequest) (mail.SentEmail, error) {
	p.sendRequest = req
	if p.sendErr != nil {
		return mail.SentEmail{}, p.sendErr
	}
	return mail.SentEmail{ID: "sent-1", ThreadID: "thread-1"}, nil
}

func TestEmailProvider_injectsAndDelegatesBuiltins(t *testing.T) {
	// Arrange
	provider := &fakeEmailProvider{kind: mail.ProviderGmail}
	h := mustNewAgent(t, AgentOptions{Model: &mockStrategy{}, EmailProvider: provider})
	t.Cleanup(h.Close)
	readTool := h.findTool("read_inbox", "")
	sendTool := h.findTool("send_email", "")
	if readTool == nil || sendTool == nil {
		t.Fatal("email tools were not injected")
	}

	// Act
	readResult, readErr := readTool.invoke(t.Context(), `{"query":"from:sender","unread_only":true}`, nopRuntime())
	sendResult, sendErr := sendTool.invoke(t.Context(), `{
		"to":["owner@example.com"],"cc":[],"bcc":[],"subject":"Status","body":"Ready",
		"reply_to_message_id":"message-1"
	}`, nopRuntime())
	catalog := ToolsAsJson(h.tools)

	// Assert
	if provider.validated != 1 {
		t.Fatalf("provider validation calls = %d, want 1", provider.validated)
	}
	if readErr != nil || !strings.Contains(readResult.output, `"id":"message-1"`) || !strings.Contains(readResult.output, `"next_cursor":"page-2"`) {
		t.Fatalf("read result = %q, err = %v", readResult.output, readErr)
	}
	if provider.readRequest.Query != "from:sender" || !provider.readRequest.UnreadOnly || provider.readRequest.Limit != 20 {
		t.Fatalf("read request = %+v", provider.readRequest)
	}
	if sendErr != nil || !strings.Contains(sendResult.output, `"id":"sent-1"`) {
		t.Fatalf("send result = %q, err = %v", sendResult.output, sendErr)
	}
	if len(provider.sendRequest.To) != 1 || provider.sendRequest.Subject != "Status" || provider.sendRequest.ReplyToMessageID != "message-1" {
		t.Fatalf("send request = %+v", provider.sendRequest)
	}
	if sendTool.Access != ToolWriteAccess || len(sendTool.OnCall) != 1 {
		t.Fatalf("send tool access = %v, on-call layers = %d", sendTool.Access, len(sendTool.OnCall))
	}
	if !strings.Contains(catalog, `"name":"read_inbox"`) || !strings.Contains(catalog, `"name":"send_email"`) {
		t.Fatalf("model tool catalog missing email tools: %s", catalog)
	}
}

func TestEmailProvider_rejectsInvalidConfiguration(t *testing.T) {
	// Arrange
	validationFailure := errors.New("credentials expired")
	var nilProvider *fakeEmailProvider
	cases := []struct {
		name     string
		provider mail.Provider
		want     string
	}{
		{name: "typed nil", provider: nilProvider, want: "must not be nil"},
		{name: "unsupported kind", provider: &fakeEmailProvider{kind: "imap"}, want: "unsupported provider"},
		{name: "provider validation", provider: &fakeEmailProvider{kind: mail.ProviderOutlook, validateErr: validationFailure}, want: "credentials expired"},
	}

	// Act and assert
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewAgent(t.Context(), AgentOptions{Model: &mockStrategy{}, EmailProvider: tc.provider})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestEmailTools_validateRequestsAndPropagateToWorkers(t *testing.T) {
	// Arrange
	provider := &fakeEmailProvider{kind: mail.ProviderOutlook}
	parent := mustNewAgent(t, AgentOptions{Model: &mockStrategy{}, EmailProvider: provider})
	t.Cleanup(parent.Close)
	worker := mustNewAgent(t, parent.workerOptsFromSpec(&SubAgent{WorkerName: "mail", Model: &mockStrategy{}}))
	t.Cleanup(worker.Close)

	// Act and assert
	if worker.findTool("read_inbox", "") == nil || worker.findTool("send_email", "") == nil {
		t.Fatal("worker did not inherit email tools")
	}
	readTool := parent.findTool("read_inbox", "")
	if _, err := readTool.invoke(t.Context(), `{"limit":101}`, nopRuntime()); err == nil || !strings.Contains(err.Error(), "between 1 and 100") {
		t.Fatalf("read limit error = %v", err)
	}
	sendTool := parent.findTool("send_email", "")
	if _, err := sendTool.invoke(t.Context(), `{"to":[],"cc":[],"bcc":[],"subject":"","body":"","reply_to_message_id":""}`, nopRuntime()); err == nil || !strings.Contains(err.Error(), "recipient") {
		t.Fatalf("send validation error = %v", err)
	}
	provider.readErr = errors.New("inbox unavailable")
	if _, err := readTool.invoke(t.Context(), `{}`, nopRuntime()); !errors.Is(err, provider.readErr) {
		t.Fatalf("read provider error = %v", err)
	}
	provider.sendErr = errors.New("send rejected")
	if _, err := sendTool.invoke(t.Context(), `{"to":["a@example.com"],"cc":[],"bcc":[],"subject":"S","body":"B","reply_to_message_id":""}`, nopRuntime()); !errors.Is(err, provider.sendErr) {
		t.Fatalf("send provider error = %v", err)
	}

	withoutEmail := mustNewAgent(t, AgentOptions{Model: &mockStrategy{}})
	t.Cleanup(withoutEmail.Close)
	if withoutEmail.findTool("read_inbox", "") != nil || withoutEmail.findTool("send_email", "") != nil {
		t.Fatal("email tools were injected without a provider")
	}
}
