// Package outlook adapts the official Microsoft Graph SDK to email.Provider.
package outlook

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/microsoftgraph/msgraph-sdk-go/users"
	provider "github.com/ryanaldo34/tacklr/email"
)

// Provider implements email.Provider with msgraph-sdk-go. The host constructs
// the Graph client with the scopes and authentication flow it permits.
type Provider struct {
	client *msgraphsdk.GraphServiceClient
}

// New returns an Outlook provider backed by client.
func New(client *msgraphsdk.GraphServiceClient) *Provider { return &Provider{client: client} }

func (*Provider) Kind() provider.ProviderKind { return provider.ProviderOutlook }

// Validate checks that the official Graph SDK client is present.
func (p *Provider) Validate(context.Context) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("email/outlook: Graph client is required")
	}
	return nil
}

// ReadInbox lists messages from the selected Graph mail folder. Mailbox accepts
// Graph well-known folder names, such as inbox, or a Graph folder ID.
func (p *Provider) ReadInbox(ctx context.Context, req provider.ReadInboxRequest) (provider.Inbox, error) {
	if err := p.Validate(ctx); err != nil {
		return provider.Inbox{}, err
	}
	if err := req.Validate(); err != nil {
		return provider.Inbox{}, err
	}
	top := int32(req.Limit)
	filter := odataFilter(req)
	if req.Cursor != "" {
		if err := p.validCursor(req.Cursor); err != nil {
			return provider.Inbox{}, err
		}
		page, err := p.client.Me().Messages().WithUrl(req.Cursor).Get(ctx, nil)
		if err != nil {
			return provider.Inbox{}, fmt.Errorf("email/outlook: list next page: %w", err)
		}
		return inboxFromPage(page), nil
	}
	if req.Mailbox == "" {
		page, err := p.client.Me().Messages().Get(ctx, messageConfig(top, filter))
		if err != nil {
			return provider.Inbox{}, fmt.Errorf("email/outlook: list messages: %w", err)
		}
		return inboxFromPage(page), nil
	}
	page, err := p.client.Me().MailFolders().ByMailFolderId(req.Mailbox).Messages().Get(ctx, folderMessageConfig(top, filter))
	if err != nil {
		return provider.Inbox{}, fmt.Errorf("email/outlook: list mailbox %q: %w", req.Mailbox, err)
	}
	return inboxFromPage(page), nil
}

func (p *Provider) validCursor(cursor string) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("email/outlook: Graph client is required")
	}
	next, err := url.Parse(cursor)
	if err != nil || next.Scheme == "" || next.Host == "" {
		return fmt.Errorf("email/outlook: invalid pagination cursor")
	}
	base, err := url.Parse(p.client.GetAdapter().GetBaseUrl())
	if err != nil || !strings.EqualFold(next.Scheme, base.Scheme) || !strings.EqualFold(next.Host, base.Host) {
		return fmt.Errorf("email/outlook: pagination cursor is outside the Graph API")
	}
	return nil
}

// SendEmail sends a Graph message. Graph sendMail returns no message ID, so
// SentEmail is empty after a successful accepted request.
func (p *Provider) SendEmail(ctx context.Context, req provider.SendEmailRequest) (provider.SentEmail, error) {
	if err := p.Validate(ctx); err != nil {
		return provider.SentEmail{}, err
	}
	if req.ReplyToMessageID != "" {
		return provider.SentEmail{}, fmt.Errorf("email/outlook: replies are not supported")
	}
	message := models.NewMessage()
	message.SetToRecipients(recipients(req.To))
	message.SetCcRecipients(recipients(req.CC))
	message.SetBccRecipients(recipients(req.BCC))
	message.SetSubject(&req.Subject)
	body := models.NewItemBody()
	body.SetContent(&req.Body)
	contentType := models.TEXT_BODYTYPE
	body.SetContentType(&contentType)
	message.SetBody(body)
	payload := users.NewItemSendMailPostRequestBody()
	payload.SetMessage(message)
	save := true
	payload.SetSaveToSentItems(&save)
	if err := p.client.Me().SendMail().Post(ctx, payload, nil); err != nil {
		return provider.SentEmail{}, fmt.Errorf("email/outlook: send message: %w", err)
	}
	return provider.SentEmail{}, nil
}

func messageConfig(top int32, filter string) *users.ItemMessagesRequestBuilderGetRequestConfiguration {
	query := &users.ItemMessagesRequestBuilderGetQueryParameters{Top: &top, Filter: stringPtr(filter), Orderby: []string{"receivedDateTime DESC"}, Select: selectedFields}
	return &users.ItemMessagesRequestBuilderGetRequestConfiguration{QueryParameters: query}
}

func folderMessageConfig(top int32, filter string) *users.ItemMailFoldersItemMessagesRequestBuilderGetRequestConfiguration {
	query := &users.ItemMailFoldersItemMessagesRequestBuilderGetQueryParameters{Top: &top, Filter: stringPtr(filter), Orderby: []string{"receivedDateTime DESC"}, Select: selectedFields}
	return &users.ItemMailFoldersItemMessagesRequestBuilderGetRequestConfiguration{QueryParameters: query}
}

func odataFilter(req provider.ReadInboxRequest) string {
	filters := make([]string, 0, 7)
	if req.From != "" {
		filters = append(filters, "from/emailAddress/address eq '"+odataString(req.From)+"'")
	}
	if req.To != "" {
		filters = append(filters, "toRecipients/any(r:r/emailAddress/address eq '"+odataString(req.To)+"')")
	}
	if req.Subject != "" {
		filters = append(filters, "contains(subject, '"+odataString(req.Subject)+"')")
	}
	if req.ReceivedAfter != "" {
		filters = append(filters, "receivedDateTime ge "+req.ReceivedAfter+"T00:00:00Z")
	}
	if req.ReceivedBefore != "" {
		before, _ := time.Parse(time.DateOnly, req.ReceivedBefore)
		filters = append(filters, "receivedDateTime lt "+before.AddDate(0, 0, 1).Format(time.RFC3339))
	}
	if req.HasAttachment != nil {
		filters = append(filters, fmt.Sprintf("hasAttachments eq %t", *req.HasAttachment))
	}
	if req.UnreadOnly {
		filters = append(filters, "isRead eq false")
	}
	return strings.Join(filters, " and ")
}

func odataString(value string) string { return strings.ReplaceAll(value, "'", "''") }

var selectedFields = []string{"id", "conversationId", "from", "toRecipients", "ccRecipients", "subject", "body", "receivedDateTime", "isRead"}

func inboxFromPage(page models.MessageCollectionResponseable) provider.Inbox {
	if page == nil {
		return provider.Inbox{}
	}
	out := provider.Inbox{}
	if next := page.GetOdataNextLink(); next != nil {
		out.NextCursor = *next
	}
	for _, message := range page.GetValue() {
		out.Messages = append(out.Messages, messageFromSDK(message))
	}
	return out
}

func messageFromSDK(message models.Messageable) provider.Message {
	if message == nil {
		return provider.Message{}
	}
	out := provider.Message{ID: value(message.GetId()), ThreadID: value(message.GetConversationId()), From: recipient(message.GetFrom()), To: recipientList(message.GetToRecipients()), CC: recipientList(message.GetCcRecipients()), Subject: value(message.GetSubject()), Unread: !boolValue(message.GetIsRead())}
	if body := message.GetBody(); body != nil {
		out.Body = value(body.GetContent())
	}
	if received := message.GetReceivedDateTime(); received != nil {
		out.ReceivedAt = received.UTC()
	}
	return out
}

func recipients(addresses []string) []models.Recipientable {
	out := make([]models.Recipientable, 0, len(addresses))
	for _, address := range addresses {
		email := models.NewEmailAddress()
		email.SetAddress(&address)
		recipient := models.NewRecipient()
		recipient.SetEmailAddress(email)
		out = append(out, recipient)
	}
	return out
}

func recipientList(items []models.Recipientable) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if address := recipient(item); address != "" {
			out = append(out, address)
		}
	}
	return out
}

func recipient(item models.Recipientable) string {
	if item == nil || item.GetEmailAddress() == nil {
		return ""
	}
	return value(item.GetEmailAddress().GetAddress())
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func value(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func boolValue(value *bool) bool { return value != nil && *value }
