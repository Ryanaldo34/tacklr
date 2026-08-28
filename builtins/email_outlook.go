package builtins

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/microsoftgraph/msgraph-sdk-go/users"
)

const maxPage = 100

// outlookProvider implements EmailProvider with msgraph-sdk-go.
type outlookProvider struct {
	client *msgraphsdk.GraphServiceClient
}

// Outlook returns an EmailProvider backed by the official Graph SDK. The host
// constructs the client with the scopes and authentication flow it permits.
func Outlook(client *msgraphsdk.GraphServiceClient) EmailProvider {
	return &outlookProvider{client: client}
}

func (p *outlookProvider) Kind() ProviderKind { return ProviderOutlook }

// Validate checks that the official Graph SDK client is present.
func (p *outlookProvider) Validate(context.Context) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("builtins: outlook Graph client is required")
	}
	return nil
}

// ReadInbox lists messages from the selected Graph mail folder. Mailbox accepts
// Graph well-known folder names, such as inbox, or a Graph folder ID.
func (p *outlookProvider) ReadInbox(ctx context.Context, req ReadInboxRequest) (Inbox, error) {
	if err := p.Validate(ctx); err != nil {
		return Inbox{}, err
	}
	if err := req.Validate(); err != nil {
		return Inbox{}, err
	}
	page, err := p.listMessages(ctx, req)
	if err != nil {
		return Inbox{}, fmt.Errorf("builtins: outlook: list messages: %w", err)
	}
	return inboxFromPage(page), nil
}

func (p *outlookProvider) listMessages(ctx context.Context, req ReadInboxRequest) (models.MessageCollectionResponseable, error) {
	if req.Cursor != "" {
		if err := validGraphCursor(req.Cursor, p.client.GetAdapter().GetBaseUrl()); err != nil {
			return nil, err
		}
		return p.client.Me().Messages().WithUrl(req.Cursor).Get(ctx, nil)
	}
	top := pageSize(req.Limit)
	filter := odataFilter(req)
	if req.Mailbox != "" {
		return p.client.Me().MailFolders().ByMailFolderId(req.Mailbox).Messages().Get(ctx, folderMessageConfig(top, filter))
	}
	return p.client.Me().Messages().Get(ctx, messageConfig(top, filter))
}

func pageSize(limit int) int32 {
	if limit < 1 {
		return 20
	}
	if limit > maxPage {
		return maxPage
	}
	return int32(limit) //nolint:gosec // G115: clamped to maxPage
}

func validGraphCursor(cursor, base string) error {
	next, err := url.Parse(cursor)
	if err != nil || next.Scheme == "" || next.Host == "" {
		return fmt.Errorf("builtins: outlook: invalid pagination cursor")
	}
	u, err := url.Parse(base)
	if err != nil || !strings.EqualFold(next.Scheme, u.Scheme) || !strings.EqualFold(next.Host, u.Host) {
		return fmt.Errorf("builtins: outlook: pagination cursor is outside the Graph API")
	}
	return nil
}

// SendEmail sends a Graph message. Graph sendMail returns no message ID, so
// SentEmail is empty after a successful accepted request.
func (p *outlookProvider) SendEmail(ctx context.Context, req SendEmailRequest) (SentEmail, error) {
	if err := p.Validate(ctx); err != nil {
		return SentEmail{}, err
	}
	if strings.TrimSpace(req.ReplyToMessageID) != "" {
		return SentEmail{}, fmt.Errorf("builtins: outlook: replies are not supported")
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
		return SentEmail{}, fmt.Errorf("builtins: outlook: send message: %w", err)
	}
	return SentEmail{}, nil
}

func messageConfig(top int32, filter string) *users.ItemMessagesRequestBuilderGetRequestConfiguration {
	query := &users.ItemMessagesRequestBuilderGetQueryParameters{Top: &top, Filter: stringPtr(filter), Orderby: []string{"receivedDateTime DESC"}, Select: selectedFields}
	return &users.ItemMessagesRequestBuilderGetRequestConfiguration{QueryParameters: query}
}

func folderMessageConfig(top int32, filter string) *users.ItemMailFoldersItemMessagesRequestBuilderGetRequestConfiguration {
	query := &users.ItemMailFoldersItemMessagesRequestBuilderGetQueryParameters{Top: &top, Filter: stringPtr(filter), Orderby: []string{"receivedDateTime DESC"}, Select: selectedFields}
	return &users.ItemMailFoldersItemMessagesRequestBuilderGetRequestConfiguration{QueryParameters: query}
}

func odataFilter(req ReadInboxRequest) string {
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

func inboxFromPage(page models.MessageCollectionResponseable) Inbox {
	out := Inbox{}
	if next := page.GetOdataNextLink(); next != nil {
		out.NextCursor = *next
	}
	for _, message := range page.GetValue() {
		out.Messages = append(out.Messages, messageFromOutlook(message))
	}
	return out
}

func messageFromOutlook(message models.Messageable) Message {
	out := Message{ID: value(message.GetId()), ThreadID: value(message.GetConversationId()), From: recipient(message.GetFrom()), To: recipientList(message.GetToRecipients()), CC: recipientList(message.GetCcRecipients()), Subject: value(message.GetSubject()), Unread: !boolValue(message.GetIsRead())}
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

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func boolValue(v *bool) bool { return v != nil && *v }
