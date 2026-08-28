// Package gmail adapts the official Gmail SDK to email.Provider.
package gmail

import (
	"context"
	"encoding/base64"
	"fmt"
	"mime"
	"net/mail"
	"slices"
	"strings"

	googlemail "google.golang.org/api/gmail/v1"

	provider "github.com/ryanaldo34/tacklr/email"
)

// Provider implements email.Provider with google.golang.org/api/gmail/v1.
// Construct the Gmail service with the OAuth scopes your host permits.
type Provider struct {
	service *googlemail.Service
}

// New returns a Gmail provider backed by service.
func New(service *googlemail.Service) *Provider { return &Provider{service: service} }

func (p *Provider) Kind() provider.ProviderKind { return provider.ProviderGmail }

// Validate checks that the official Gmail SDK client is present.
func (p *Provider) Validate(context.Context) error {
	if p == nil || p.service == nil {
		return fmt.Errorf("email/%s: service is required", p.Kind())
	}
	return nil
}

// ReadInbox lists full messages through the Gmail API.
func (p *Provider) ReadInbox(ctx context.Context, req provider.ReadInboxRequest) (provider.Inbox, error) {
	if err := p.Validate(ctx); err != nil {
		return provider.Inbox{}, err
	}
	if err := req.Validate(); err != nil {
		return provider.Inbox{}, err
	}
	call := p.service.Users.Messages.List("me").MaxResults(int64(req.Limit)).Context(ctx)
	if query := gmailQuery(req); query != "" {
		call.Q(query)
	}
	if req.Mailbox != "" {
		call.LabelIds(req.Mailbox)
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
	sent, err := p.service.Users.Messages.Send("me", &googlemail.Message{
		Raw: base64.RawURLEncoding.EncodeToString([]byte(buildMessage(req))),
	}).Context(ctx).Do()
	if err != nil {
		return provider.SentEmail{}, fmt.Errorf("email/gmail: send message: %w", err)
	}
	return provider.SentEmail{ID: sent.Id, ThreadID: sent.ThreadId}, nil
}

func gmailQuery(req provider.ReadInboxRequest) string {
	terms := make([]string, 0, 7)
	if req.From != "" {
		terms = append(terms, "from:"+gmailQuoted(req.From))
	}
	if req.To != "" {
		terms = append(terms, "to:"+gmailQuoted(req.To))
	}
	if req.Subject != "" {
		terms = append(terms, "subject:"+gmailQuoted(req.Subject))
	}
	if req.ReceivedAfter != "" {
		terms = append(terms, "after:"+strings.ReplaceAll(req.ReceivedAfter, "-", "/"))
	}
	if req.ReceivedBefore != "" {
		terms = append(terms, "before:"+strings.ReplaceAll(req.ReceivedBefore, "-", "/"))
	}
	if req.HasAttachment != nil {
		if *req.HasAttachment {
			terms = append(terms, "has:attachment")
		} else {
			terms = append(terms, "-has:attachment")
		}
	}
	if req.UnreadOnly {
		terms = append(terms, "is:unread")
	}
	return strings.Join(terms, " ")
}

func gmailQuoted(value string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
}

func messageFromSDK(message *googlemail.Message) provider.Message {
	headers := map[string]string{}
	if message.Payload != nil {
		for _, header := range message.Payload.Headers {
			headers[strings.ToLower(header.Name)] = header.Value
		}
	}
	received, _ := mail.ParseDate(headers["date"])
	return provider.Message{
		ID: message.Id, ThreadID: message.ThreadId, From: headers["from"],
		To: splitAddresses(headers["to"]), CC: splitAddresses(headers["cc"]),
		Subject: headers["subject"], Body: payloadText(message.Payload), ReceivedAt: received,
		Unread: slices.Contains(message.LabelIds, "UNREAD"),
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

func payloadText(part *googlemail.MessagePart) string {
	if part == nil {
		return ""
	}
	if strings.HasPrefix(part.MimeType, "text/plain") && part.Body != nil {
		body, _ := base64.RawURLEncoding.DecodeString(part.Body.Data)
		return string(body)
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
	if req.ReplyToMessageID != "" {
		b.WriteString("In-Reply-To: ")
		b.WriteString(req.ReplyToMessageID)
		b.WriteString("\r\n")
	}
	b.WriteString("Subject: ")
	b.WriteString(mime.QEncoding.Encode("UTF-8", req.Subject))
	b.WriteString("\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n")
	b.WriteString(req.Body)
	return b.String()
}
