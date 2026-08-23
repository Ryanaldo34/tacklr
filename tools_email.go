package tacklr

import (
	"context"
	"fmt"

	mail "github.com/ryanaldo34/tacklr/email"
	"github.com/ryanaldo34/tacklr/streaming"
)

const maxInboxMessages = 100

type readInboxArgs struct {
	Query      string `json:"query,omitempty" desc:"Optional provider search query."`
	Mailbox    string `json:"mailbox,omitempty" desc:"Optional mailbox or folder. Omit to use the provider inbox."`
	UnreadOnly bool   `json:"unread_only,omitempty" desc:"Return only unread messages when true."`
	Limit      int    `json:"limit,omitempty" desc:"Maximum messages to return. Default 20, maximum 100."`
	Cursor     string `json:"cursor,omitempty" desc:"Provider cursor returned by a previous read_inbox call."`
}

type sendEmailArgs struct {
	To               []string `json:"to" desc:"Primary recipient email addresses."`
	CC               []string `json:"cc,omitempty" desc:"Optional carbon-copy recipient email addresses."`
	BCC              []string `json:"bcc,omitempty" desc:"Optional blind-carbon-copy recipient email addresses."`
	Subject          string   `json:"subject" desc:"Email subject."`
	Body             string   `json:"body" desc:"Plaintext email body."`
	ReplyToMessageID string   `json:"reply_to_message_id,omitempty" desc:"Optional provider message ID to reply to."`
}

func newEmailTools(provider mail.Provider) []*Tool {
	return []*Tool{
		NewTool(ToolConfig{
			Name:        "read_inbox",
			DisplayName: "Read Email Inbox",
			Description: "Read and search messages in the configured email inbox.",
			Category:    streaming.ToolCategoryRead,
			Access:      ToolReadAccess,
			Handler: func(ctx context.Context, args readInboxArgs) (mail.Inbox, error) {
				limit := args.Limit
				if limit == 0 {
					limit = 20
				}
				if limit < 1 || limit > maxInboxMessages {
					return mail.Inbox{}, fmt.Errorf("read_inbox: limit must be between 1 and %d", maxInboxMessages)
				}
				return provider.ReadInbox(ctx, mail.ReadInboxRequest{
					Query:      args.Query,
					Mailbox:    args.Mailbox,
					UnreadOnly: args.UnreadOnly,
					Limit:      limit,
					Cursor:     args.Cursor,
				})
			},
		}),
		NewTool(ToolConfig{
			Name:        "send_email",
			DisplayName: "Send Email: {subject}",
			Description: "Send an email through the configured email account. Confirm recipients and content before sending.",
			Category:    streaming.ToolCategoryEdit,
			Access:      ToolWriteAccess,
			OnCall:      []OnCallFunc{ToolPermissionOnCall},
			Handler: func(ctx context.Context, args sendEmailArgs) (mail.SentEmail, error) {
				req := mail.SendEmailRequest{
					To:               args.To,
					CC:               args.CC,
					BCC:              args.BCC,
					Subject:          args.Subject,
					Body:             args.Body,
					ReplyToMessageID: args.ReplyToMessageID,
				}
				if err := req.Validate(); err != nil {
					return mail.SentEmail{}, fmt.Errorf("send_email: %w", err)
				}
				return provider.SendEmail(ctx, req)
			},
		}),
	}
}
