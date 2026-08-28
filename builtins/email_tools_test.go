package builtins

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
)

type fakeEmailProvider struct {
	kind        ProviderKind
	validateErr error
	readRequest ReadInboxRequest
	sendRequest SendEmailRequest
	readErr     error
	sendErr     error
}

func (p *fakeEmailProvider) Kind() ProviderKind { return p.kind }

func (p *fakeEmailProvider) Validate(context.Context) error { return p.validateErr }

func (p *fakeEmailProvider) ReadInbox(_ context.Context, req ReadInboxRequest) (Inbox, error) {
	p.readRequest = req
	if p.readErr != nil {
		return Inbox{}, p.readErr
	}
	return Inbox{
		Messages: []Message{{
			ID: "message-1", From: "sender@example.com", Subject: "Status",
			ReceivedAt: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC), Unread: true,
		}},
		NextCursor: "page-2",
	}, nil
}

func (p *fakeEmailProvider) SendEmail(_ context.Context, req SendEmailRequest) (SentEmail, error) {
	p.sendRequest = req
	if p.sendErr != nil {
		return SentEmail{}, p.sendErr
	}
	return SentEmail{ID: "sent-1", ThreadID: "thread-1"}, nil
}

func TestReadInbox_delegatesStructuredFilters(t *testing.T) {
	provider := &fakeEmailProvider{kind: ProviderGmail}
	hasAttachment := true
	inbox, err := runReadInbox(t.Context(), provider, readInboxArgs{
		From: "sender@example.com", To: "owner@example.com", Subject: "Status",
		ReceivedAfter: "2026-08-01", ReceivedBefore: "2026-08-31",
		HasAttachment: &hasAttachment, UnreadOnly: true,
	})
	if err != nil || len(inbox.Messages) != 1 || inbox.Messages[0].ID != "message-1" || inbox.NextCursor != "page-2" {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	if provider.readRequest.From != "sender@example.com" || provider.readRequest.Limit != 20 || !provider.readRequest.UnreadOnly {
		t.Fatalf("request = %+v", provider.readRequest)
	}

	tool := ReadInbox(provider)
	if tool.Name() != "read_inbox" || tool.Access() != tacklr.ToolReadAccess || tool.Category() != tacklr.ToolCategoryRead {
		t.Fatalf("tool = %s access=%v category=%s", tool.Name(), tool.Access(), tool.Category())
	}
	props, _ := tool.AsJson()["parameters"].(map[string]any)["properties"].(map[string]any)
	if _, exists := props["query"]; exists {
		t.Fatal("read_inbox schema exposes a provider query parameter")
	}
	for _, name := range []string{"from", "to", "subject", "received_after", "received_before", "has_attachment"} {
		if _, exists := props[name]; !exists {
			t.Fatalf("read_inbox schema missing %q", name)
		}
	}
}

func TestSendEmail_delegatesAndGatesPermission(t *testing.T) {
	provider := &fakeEmailProvider{kind: ProviderOutlook}
	sent, err := runSendEmail(t.Context(), provider, sendEmailArgs{
		To: []string{"owner@example.com"}, Subject: "Status", Body: "Ready", ReplyToMessageID: "message-1",
	})
	if err != nil || sent.ID != "sent-1" {
		t.Fatalf("sent = %+v, err = %v", sent, err)
	}
	if len(provider.sendRequest.To) != 1 || provider.sendRequest.ReplyToMessageID != "message-1" {
		t.Fatalf("request = %+v", provider.sendRequest)
	}

	tool := SendEmail(provider)
	if tool.Name() != "send_email" || tool.Access() != tacklr.ToolWriteAccess {
		t.Fatalf("tool = %s access=%v", tool.Name(), tool.Access())
	}
}

func TestEmailTools_validateRequestsAndProviderErrors(t *testing.T) {
	provider := &fakeEmailProvider{kind: ProviderOutlook}
	if _, err := runReadInbox(t.Context(), provider, readInboxArgs{Limit: 101}); err == nil || !strings.Contains(err.Error(), "between 1 and 100") {
		t.Fatalf("read limit error = %v", err)
	}
	if _, err := runReadInbox(t.Context(), provider, readInboxArgs{ReceivedAfter: "not-a-date"}); err == nil || !strings.Contains(err.Error(), "YYYY-MM-DD") {
		t.Fatalf("read filter validation error = %v", err)
	}
	if _, err := runSendEmail(t.Context(), provider, sendEmailArgs{}); err == nil || !strings.Contains(err.Error(), "recipient") {
		t.Fatalf("send validation error = %v", err)
	}
	provider.readErr = errors.New("inbox unavailable")
	if _, err := runReadInbox(t.Context(), provider, readInboxArgs{}); !errors.Is(err, provider.readErr) {
		t.Fatalf("read provider error = %v", err)
	}
	provider.sendErr = errors.New("send rejected")
	if _, err := runSendEmail(t.Context(), provider, sendEmailArgs{To: []string{"a@example.com"}, Subject: "S", Body: "B"}); !errors.Is(err, provider.sendErr) {
		t.Fatalf("send provider error = %v", err)
	}
	if _, err := runReadInbox(t.Context(), nil, readInboxArgs{}); err == nil || !strings.Contains(err.Error(), "email provider is required") {
		t.Fatalf("nil read provider = %v", err)
	}
	if _, err := runSendEmail(t.Context(), nil, sendEmailArgs{To: []string{"a@example.com"}, Subject: "S", Body: "B"}); err == nil || !strings.Contains(err.Error(), "email provider is required") {
		t.Fatalf("nil send provider = %v", err)
	}
}

func TestEmailConstructors_panicWithoutProvider(t *testing.T) {
	for _, fn := range []func(){
		func() { ReadInbox(nil) },
		func() { SendEmail(nil) },
	} {
		var panicked bool
		func() {
			defer func() { panicked = recover() != nil }()
			fn()
		}()
		if !panicked {
			t.Fatal("nil provider constructor did not panic")
		}
	}
}
