package email

import (
	"context"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr"
)

type stubMail struct {
	inbox Inbox
	err   error
}

func (stubMail) Kind() ProviderKind             { return ProviderGmail }
func (stubMail) Validate(context.Context) error { return nil }
func (s stubMail) ReadInbox(context.Context, ReadInboxRequest) (Inbox, error) {
	return s.inbox, s.err
}
func (s stubMail) SendEmail(context.Context, SendEmailRequest) (SentEmail, error) {
	return SentEmail{}, s.err
}

func TestEmailTools_schemaAndValidation(t *testing.T) {
	p := stubMail{inbox: Inbox{Messages: []Message{{ID: "message-1"}}}}
	inbox, err := runReadInbox(t.Context(), p, readInboxArgs{From: "sender@example.com", Limit: 5})
	if err != nil || len(inbox.Messages) != 1 || inbox.Messages[0].ID != "message-1" {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	if _, err := runReadInbox(t.Context(), p, readInboxArgs{Limit: 101}); err == nil || !strings.Contains(err.Error(), "between 1 and 100") {
		t.Fatalf("read limit error = %v", err)
	}
	if _, err := runReadInbox(t.Context(), p, readInboxArgs{ReceivedAfter: "not-a-date"}); err == nil || !strings.Contains(err.Error(), "YYYY-MM-DD") {
		t.Fatalf("read filter validation error = %v", err)
	}
	if _, err := runSendEmail(t.Context(), p, sendEmailArgs{}); err == nil || !strings.Contains(err.Error(), "recipient") {
		t.Fatalf("send validation error = %v", err)
	}
	p.err = errStub
	if _, err := runReadInbox(t.Context(), p, readInboxArgs{}); err == nil {
		t.Fatal("provider error was swallowed")
	}
	if _, err := runSendEmail(t.Context(), p, sendEmailArgs{To: []string{"a@example.com"}, Subject: "S", Body: "B"}); err == nil {
		t.Fatal("send provider error was swallowed")
	}
	if _, err := runReadInbox(t.Context(), nil, readInboxArgs{}); err == nil || !strings.Contains(err.Error(), "email provider is required") {
		t.Fatalf("nil read provider = %v", err)
	}
	if _, err := runSendEmail(t.Context(), nil, sendEmailArgs{To: []string{"a@example.com"}, Subject: "S", Body: "B"}); err == nil || !strings.Contains(err.Error(), "email provider is required") {
		t.Fatalf("nil send provider = %v", err)
	}

	tool := ReadInbox(p)
	if tool.Name() != "read_inbox" || tool.Access() != tacklr.ToolReadAccess || tool.Category() != message.ToolCategoryRead {
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
	send := SendEmail(p)
	if send.Name() != "send_email" || send.Access() != tacklr.ToolWriteAccess {
		t.Fatalf("tool = %s access=%v", send.Name(), send.Access())
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

type stubErr string

func (e stubErr) Error() string { return string(e) }

const errStub stubErr = "down"
