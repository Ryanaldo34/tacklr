package builtins

import (
	"context"
	"fmt"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

const maxInboxMessages = 100

type readInboxArgs struct {
	From           string `json:"from,omitempty" desc:"Optional sender email address."`
	To             string `json:"to,omitempty" desc:"Optional recipient email address."`
	Subject        string `json:"subject,omitempty" desc:"Optional text that must occur in the subject."`
	ReceivedAfter  string `json:"received_after,omitempty" desc:"Optional inclusive earliest received date in YYYY-MM-DD format."`
	ReceivedBefore string `json:"received_before,omitempty" desc:"Optional inclusive latest received date in YYYY-MM-DD format."`
	HasAttachment  *bool  `json:"has_attachment,omitempty" desc:"Optional attachment filter. null means do not filter by attachments."`
	Mailbox        string `json:"mailbox,omitempty" desc:"Optional mailbox or folder. Omit to use the provider inbox."`
	UnreadOnly     bool   `json:"unread_only,omitempty" desc:"Return only unread messages when true."`
	Limit          int    `json:"limit,omitempty" desc:"Maximum messages to return. Default 20, maximum 100."`
	Cursor         string `json:"cursor,omitempty" desc:"Provider cursor returned by a previous read_inbox call."`
}

type sendEmailArgs struct {
	To               []string `json:"to" desc:"Primary recipient email addresses."`
	CC               []string `json:"cc,omitempty" desc:"Optional carbon-copy recipient email addresses."`
	BCC              []string `json:"bcc,omitempty" desc:"Optional blind-carbon-copy recipient email addresses."`
	Subject          string   `json:"subject" desc:"Email subject."`
	Body             string   `json:"body" desc:"Plaintext email body."`
	ReplyToMessageID string   `json:"reply_to_message_id,omitempty" desc:"Optional provider message ID to reply to."`
}

// ReadInbox returns the read_inbox tool closed over provider.
func ReadInbox(provider EmailProvider) *tacklr.Tool {
	if provider == nil {
		panic("builtins: read_inbox requires an email provider")
	}
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "read_inbox",
		DisplayName: "Read Email Inbox",
		Description: "Read and search messages in the configured email inbox.",
		Category:    streaming.ToolCategoryRead,
		Access:      tacklr.ToolReadAccess,
		Handler: func(ctx context.Context, args readInboxArgs) (Inbox, error) {
			return runReadInbox(ctx, provider, args)
		},
	})
}

// SendEmail returns the permission-gated send_email tool closed over provider.
func SendEmail(provider EmailProvider) *tacklr.Tool {
	if provider == nil {
		panic("builtins: send_email requires an email provider")
	}
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "send_email",
		DisplayName: "Send Email: {subject}",
		Description: "Send an email through the configured email account. Confirm recipients and content before sending.",
		Category:    streaming.ToolCategoryEdit,
		Access:      tacklr.ToolWriteAccess,
		OnCall:      []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context, args sendEmailArgs) (SentEmail, error) {
			return runSendEmail(ctx, provider, args)
		},
	})
}

func runReadInbox(ctx context.Context, provider EmailProvider, args readInboxArgs) (Inbox, error) {
	if provider == nil {
		return Inbox{}, fmt.Errorf("builtins: email provider is required")
	}
	limit := args.Limit
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > maxInboxMessages {
		return Inbox{}, fmt.Errorf("read_inbox: limit must be between 1 and %d", maxInboxMessages)
	}
	req := ReadInboxRequest{
		From: args.From, To: args.To, Subject: args.Subject,
		ReceivedAfter: args.ReceivedAfter, ReceivedBefore: args.ReceivedBefore,
		HasAttachment: args.HasAttachment, Mailbox: args.Mailbox,
		UnreadOnly: args.UnreadOnly, Limit: limit, Cursor: args.Cursor,
	}
	if err := req.Validate(); err != nil {
		return Inbox{}, fmt.Errorf("read_inbox: %w", err)
	}
	return provider.ReadInbox(ctx, req)
}

func runSendEmail(ctx context.Context, provider EmailProvider, args sendEmailArgs) (SentEmail, error) {
	if provider == nil {
		return SentEmail{}, fmt.Errorf("builtins: email provider is required")
	}
	req := SendEmailRequest(args)
	if err := req.Validate(); err != nil {
		return SentEmail{}, fmt.Errorf("send_email: %w", err)
	}
	return provider.SendEmail(ctx, req)
}
