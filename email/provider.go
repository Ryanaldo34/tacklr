// Package email defines provider-neutral email capabilities for Tacklr agents.
package email

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ProviderKind identifies an email service supported by the harness.
type ProviderKind string

const (
	ProviderGmail   ProviderKind = "gmail"
	ProviderOutlook ProviderKind = "outlook"
)

// Valid reports whether the provider kind is supported by the harness.
func (k ProviderKind) Valid() bool {
	switch k {
	case ProviderGmail, ProviderOutlook:
		return true
	default:
		return false
	}
}

// Provider supplies the operations exposed by the built-in email tools.
// Implementations own authentication and provider-specific API behavior and
// must support concurrent calls when an agent uses workers.
type Provider interface {
	Kind() ProviderKind
	Validate(context.Context) error
	ReadInbox(context.Context, ReadInboxRequest) (Inbox, error)
	SendEmail(context.Context, SendEmailRequest) (SentEmail, error)
}

// ValidateProvider checks that p is usable as a supported provider capability.
// A nil interface means that email integration is disabled.
func ValidateProvider(p Provider) error {
	if p == nil {
		return nil
	}
	v := reflect.ValueOf(p)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if !v.IsNil() {
			break
		}
		return fmt.Errorf("email: provider must not be nil")
	}
	if !p.Kind().Valid() {
		return fmt.Errorf("email: unsupported provider %q", p.Kind())
	}
	return nil
}

// ReadInboxRequest selects messages from the configured inbox.
type ReadInboxRequest struct {
	From           string `json:"from,omitempty"`
	To             string `json:"to,omitempty"`
	Subject        string `json:"subject,omitempty"`
	ReceivedAfter  string `json:"received_after,omitempty"`
	ReceivedBefore string `json:"received_before,omitempty"`
	HasAttachment  *bool  `json:"has_attachment,omitempty"`
	Mailbox        string `json:"mailbox,omitempty"`
	UnreadOnly     bool   `json:"unread_only,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
}

// Validate checks portable inbox filter values. Dates use YYYY-MM-DD.
func (r ReadInboxRequest) Validate() error {
	var after, before time.Time
	for _, filter := range []struct {
		name, value string
		target      *time.Time
	}{{"received_after", r.ReceivedAfter, &after}, {"received_before", r.ReceivedBefore, &before}} {
		if filter.value == "" {
			continue
		}
		parsed, err := time.Parse(time.DateOnly, filter.value)
		if err != nil {
			return fmt.Errorf("email: %s must use YYYY-MM-DD", filter.name)
		}
		*filter.target = parsed
	}
	if !after.IsZero() && !before.IsZero() && after.After(before) {
		return fmt.Errorf("email: received_after must not be after received_before")
	}
	return nil
}

// Inbox is one provider page of messages.
type Inbox struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// Message is a provider-neutral email returned to the agent.
type Message struct {
	ID         string    `json:"id"`
	ThreadID   string    `json:"thread_id,omitempty"`
	From       string    `json:"from"`
	To         []string  `json:"to,omitempty"`
	CC         []string  `json:"cc,omitempty"`
	Subject    string    `json:"subject,omitempty"`
	Body       string    `json:"body,omitempty"`
	ReceivedAt time.Time `json:"received_at"`
	Unread     bool      `json:"unread"`
}

// SendEmailRequest is an outbound email composed by an agent.
type SendEmailRequest struct {
	To               []string `json:"to"`
	CC               []string `json:"cc,omitempty"`
	BCC              []string `json:"bcc,omitempty"`
	Subject          string   `json:"subject"`
	Body             string   `json:"body"`
	ReplyToMessageID string   `json:"reply_to_message_id,omitempty"`
}

// Validate checks the provider-neutral requirements for an outbound email.
func (r SendEmailRequest) Validate() error {
	if len(r.To) == 0 {
		return fmt.Errorf("email: at least one recipient is required")
	}
	recipients := append(append(append([]string{}, r.To...), r.CC...), r.BCC...)
	for _, recipient := range recipients {
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("email: recipients must not be empty")
		}
	}
	if strings.TrimSpace(r.Subject) == "" {
		return fmt.Errorf("email: subject is required")
	}
	if strings.TrimSpace(r.Body) == "" {
		return fmt.Errorf("email: body is required")
	}
	return nil
}

// SentEmail identifies the message accepted by the provider.
type SentEmail struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id,omitempty"`
}
