// Package gmail adapts the official Gmail SDK to email.Provider.
package gmail

import (
	"context"
	"encoding/base64"
	"fmt"
	"mime"
	"net/mail"
	"strings"
	"time"

	provider "github.com/ryanaldo34/tacklr/email"
	googlemail "google.golang.org/api/gmail/v1"
)

// Provider implements email.Provider with google.golang.org/api/gmail/v1.
// Construct the Gmail service with the OAuth scopes your host permits.
type Provider struct {
	service *googlemail.Service
}

// New returns a Gmail provider backed by service.
func New(service *googlemail.Service) *Provider { return &Provider{service: service} }

func (*Provider) Kind() provider.ProviderKind { return provider.ProviderGmail }

// Validate checks that the official Gmail SDK client is present.
func (p *Provider) Validate(context.Context) error {
	if p == nil || p.service == nil {
		return fmt.Errorf("email/gmail: service is required")
	}
	return nil
}

// ReadInbox lists full messages through the Gmail API.
func (p *Provider) ReadInbox(ctx context.Context, req provider.ReadInboxRequest) (provider.Inbox, error) {
	if err := p.Validate(ctx); err != nil {
		return provider.Inbox{}, err
	}
	call := p.service.Users.Messages.List("me").MaxResults(int64(req.Limit)).Context(ctx)
	if req.Query != "" {
		call.Q(req.Query)
	}
	if req.Mailbox != "" {
		call.LabelIds(req.Mailbox)
	}
	if req.UnreadOnly {
		call.Q(joinQuery(req.Query, "is:unread"))
	}
	if req.Cursor != "" {
		call.PageToken(req.Cursor)
	}
	page, err := call.Do()
	if err != nil {
		return provider.Inbox{}, fmt.Errorf("email/gmail: list messages: %w", err)
	}
	out := provider.Inbox{NextCursor: page.NextPageToken}
	for _, item := range page.Messages {
		message, err := p.service.Users.Messages.Get("me", item.Id).Format("full").Context(ctx).Do()
		if err != nil {
			return provider.Inbox{}, fmt.Errorf("email/gmail: get message %q: %w", item.Id, err)
		}
		out.Messages = append(out.Messages, messageFromSDK(message))
	}
	return out, nil
}

// SendEmail sends a RFC 2822 message through the Gmail API.
func (p *Provider) SendEmail(ctx context.Context, req provider.SendEmailRequest) (provider.SentEmail, error) {
	if err := p.Validate(ctx); err != nil {
		return provider.SentEmail{}, err
	}
	raw := buildMessage(req)
	message := &googlemail.Message{Raw: base64.RawURLEncoding.EncodeToString([]byte(raw))}
	if req.ReplyToMessageID != "" {
		original, err := p.service.Users.Messages.Get("me", req.ReplyToMessageID).Format("metadata").Context(ctx).Do()
		if err != nil {
			return provider.SentEmail{}, fmt.Errorf("email/gmail: get reply message %q: %w", req.ReplyToMessageID, err)
		}
		message.ThreadId = original.ThreadId
	}
	sent, err := p.service.Users.Messages.Send("me", message).Context(ctx).Do()
	if err != nil {
		return provider.SentEmail{}, fmt.Errorf("email/gmail: send message: %w", err)
	}
	return provider.SentEmail{ID: sent.Id, ThreadID: sent.ThreadId}, nil
}

func joinQuery(query, extra string) string {
	if strings.TrimSpace(query) == "" {
		return extra
	}
	return query + " " + extra
}

func messageFromSDK(message *googlemail.Message) provider.Message {
	headers := map[string]string{}
	if message.Payload != nil {
		for _, header := range message.Payload.Headers {
			headers[strings.ToLower(header.Name)] = header.Value
		}
	}
	received, _ := mail.ParseDate(headers["date"])
	if received.IsZero() && message.InternalDate > 0 {
		received = time.UnixMilli(message.InternalDate).UTC()
	}
	return provider.Message{
		ID: message.Id, ThreadID: message.ThreadId, From: headers["from"],
		To: splitAddresses(headers["to"]), CC: splitAddresses(headers["cc"]),
		Subject: headers["subject"], Body: payloadText(message.Payload), ReceivedAt: received,
		Unread: hasLabel(message.LabelIds, "UNREAD"),
	}
}

func splitAddresses(value string) []string {
	addresses, err := mail.ParseAddressList(value)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(addresses))
	for _, address := range addresses {
		out = append(out, address.Address)
	}
	return out
}

func hasLabel(labels []string, want string) bool {
	for _, label := range labels {
		if label == want {
			return true
		}
	}
	return false
}

func payloadText(part *googlemail.MessagePart) string {
	if part == nil {
		return ""
	}
	if strings.HasPrefix(part.MimeType, "text/plain") && part.Body != nil {
		body, err := base64.RawURLEncoding.DecodeString(part.Body.Data)
		if err == nil {
			return string(body)
		}
	}
	for _, child := range part.Parts {
		if body := payloadText(child); body != "" {
			return body
		}
	}
	return ""
}

func buildMessage(req provider.SendEmailRequest) string {
	var b strings.Builder
	b.WriteString("To: ")
	b.WriteString(strings.Join(req.To, ", "))
	b.WriteString("\r\n")
	if len(req.CC) > 0 {
		b.WriteString("Cc: ")
		b.WriteString(strings.Join(req.CC, ", "))
		b.WriteString("\r\n")
	}
	if len(req.BCC) > 0 {
		b.WriteString("Bcc: ")
		b.WriteString(strings.Join(req.BCC, ", "))
		b.WriteString("\r\n")
	}
	b.WriteString("Subject: ")
	b.WriteString(mime.QEncoding.Encode("UTF-8", req.Subject))
	b.WriteString("\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n")
	b.WriteString(req.Body)
	return b.String()
}
