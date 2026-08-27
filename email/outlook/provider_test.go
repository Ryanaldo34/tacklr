package outlook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	abstractions "github.com/microsoft/kiota-abstractions-go"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	provider "github.com/ryanaldo34/tacklr/email"
)

func TestMessageFromSDK_mapsGraphMessage(t *testing.T) {
	// Arrange
	message := models.NewMessage()
	id, thread, subject := "id", "thread", "subject"
	message.SetId(&id)
	message.SetConversationId(&thread)
	message.SetSubject(&subject)
	read := false
	message.SetIsRead(&read)
	received := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	message.SetReceivedDateTime(&received)
	bodyText := "body"
	body := models.NewItemBody()
	body.SetContent(&bodyText)
	message.SetBody(body)
	message.SetFrom(newRecipient("sender@example.com"))
	message.SetToRecipients([]models.Recipientable{newRecipient("to@example.com")})
	message.SetCcRecipients([]models.Recipientable{newRecipient("cc@example.com")})

	// Act
	got := messageFromSDK(message)

	// Assert
	if got.ID != id || got.ThreadID != thread || got.From != "sender@example.com" || len(got.To) != 1 || len(got.CC) != 1 || got.Subject != subject || got.Body != bodyText || got.Unread != true || !got.ReceivedAt.Equal(received) {
		t.Fatalf("message = %+v", got)
	}
}

func TestProvider_usesOfficialGraphSDKForInboxAndSend(t *testing.T) {
	// Arrange
	var sent map[string]any
	var listFilter string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := strings.TrimSuffix(r.URL.Path, "/")
		switch {
		case r.Method == http.MethodGet && path == "/users/me-token-to-replace/messages":
			listFilter = r.URL.Query().Get("$filter")
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []map[string]any{{
				"id": "message", "conversationId": "thread", "subject": "Status", "isRead": false,
				"from":         map[string]any{"emailAddress": map[string]string{"address": "sender@example.com"}},
				"toRecipients": []map[string]any{{"emailAddress": map[string]string{"address": "recipient@example.com"}}},
				"body":         map[string]string{"content": "hello", "contentType": "text"},
			}}})
		case r.Method == http.MethodPost && path == "/users/me-token-to-replace/sendMail":
			_ = json.NewDecoder(r.Body).Decode(&sent)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	adapter, err := msgraphsdk.NewGraphRequestAdapterWithParseNodeFactoryAndSerializationWriterFactoryAndHttpClient(testAuth{}, nil, nil, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	adapter.SetBaseUrl(server.URL)
	p := New(msgraphsdk.NewGraphServiceClient(adapter))

	// Act
	hasAttachment := true
	inbox, readErr := p.ReadInbox(t.Context(), provider.ReadInboxRequest{From: "sender@example.com", To: "recipient@example.com", Subject: "Status", ReceivedAfter: "2026-08-01", ReceivedBefore: "2026-08-31", HasAttachment: &hasAttachment, UnreadOnly: true, Limit: 5})
	_, sendErr := p.SendEmail(t.Context(), provider.SendEmailRequest{To: []string{"recipient@example.com"}, Subject: "Status", Body: "ready"})

	// Assert
	if readErr != nil || len(inbox.Messages) != 1 || inbox.Messages[0].ID != "message" || inbox.Messages[0].Body != "hello" || !inbox.Messages[0].Unread {
		t.Fatalf("inbox = %+v, err = %v", inbox, readErr)
	}
	for _, want := range []string{"from/emailAddress/address eq 'sender@example.com'", "toRecipients/any(r:r/emailAddress/address eq 'recipient@example.com')", "contains(subject, 'Status')", "receivedDateTime ge 2026-08-01T00:00:00Z", "receivedDateTime lt 2026-09-01T00:00:00Z", "hasAttachments eq true", "isRead eq false"} {
		if !strings.Contains(listFilter, want) {
			t.Fatalf("filter = %q, want %q", listFilter, want)
		}
	}
	message, ok := sent["Message"].(map[string]any)
	if sendErr != nil || !ok || message["subject"] != "Status" {
		t.Fatalf("send payload = %+v, err = %v", sent, sendErr)
	}
}

func TestODataFilter_escapesStructuredFilters(t *testing.T) {
	got := odataFilter(provider.ReadInboxRequest{From: "o'hara@example.com", Subject: "status's update"})
	for _, want := range []string{"from/emailAddress/address eq 'o''hara@example.com'", "contains(subject, 'status''s update')"} {
		if !strings.Contains(got, want) {
			t.Fatalf("filter = %q, want %q", got, want)
		}
	}
}

func TestProvider_rejectsMissingGraphClientAndInvalidCursor(t *testing.T) {
	p := New(nil)
	if err := p.Validate(context.Background()); err == nil {
		t.Fatal("nil client was accepted")
	}
	if err := p.validCursor("https://example.com/next"); err == nil {
		t.Fatal("foreign cursor was accepted")
	}
}

func newRecipient(address string) models.Recipientable {
	email := models.NewEmailAddress()
	email.SetAddress(&address)
	recipient := models.NewRecipient()
	recipient.SetEmailAddress(email)
	return recipient
}

type testAuth struct{}

func (testAuth) AuthenticateRequest(_ context.Context, request *abstractions.RequestInformation, _ map[string]any) error {
	request.Headers.Add("Authorization", "Bearer test")
	return nil
}
