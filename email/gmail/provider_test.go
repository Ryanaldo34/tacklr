package gmail

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	provider "github.com/ryanaldo34/tacklr/email"
	"golang.org/x/oauth2"
	googlemail "google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

func TestProvider_usesOfficialGmailSDKForInboxAndSend(t *testing.T) {
	// Arrange
	var listQuery string
	var sentRaw string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/gmail/v1/users/me/messages":
			listQuery = r.URL.RawQuery
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": []map[string]string{{"id": "m1"}}, "nextPageToken": "next"})
		case r.Method == http.MethodGet && r.URL.Path == "/gmail/v1/users/me/messages/m1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "m1", "threadId": "t1", "labelIds": []string{"UNREAD"}, "internalDate": "1760000000000",
				"payload": map[string]any{"mimeType": "text/plain", "headers": []map[string]string{
					{"name": "From", "value": "sender@example.com"}, {"name": "To", "value": "recipient@example.com"},
					{"name": "Subject", "value": "Status"}, {"name": "Date", "value": "Thu, 9 Oct 2025 08:53:20 +0000"},
				}, "body": map[string]string{"data": base64.RawURLEncoding.EncodeToString([]byte("hello"))}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/gmail/v1/users/me/messages/send":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			sentRaw = body["raw"]
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "sent", "threadId": "thread"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	service, err := googlemail.NewService(t.Context(), option.WithHTTPClient(oauth2.NewClient(t.Context(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}))), option.WithEndpoint(server.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	p := New(service)

	// Act
	inbox, err := p.ReadInbox(t.Context(), provider.ReadInboxRequest{Query: "from:sender", Mailbox: "INBOX", UnreadOnly: true, Limit: 5, Cursor: "cursor"})
	sent, sendErr := p.SendEmail(t.Context(), provider.SendEmailRequest{To: []string{"recipient@example.com"}, Subject: "Status", Body: "ready"})

	// Assert
	if err != nil || len(inbox.Messages) != 1 || inbox.Messages[0].Body != "hello" || !inbox.Messages[0].Unread || inbox.NextCursor != "next" {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	for _, want := range []string{"q=from%3Asender+is%3Aunread", "labelIds=INBOX", "maxResults=5", "pageToken=cursor"} {
		if !strings.Contains(listQuery, want) {
			t.Fatalf("list query = %q, want %q", listQuery, want)
		}
	}
	raw, decodeErr := base64.RawURLEncoding.DecodeString(sentRaw)
	if sendErr != nil || decodeErr != nil || sent.ID != "sent" || !strings.Contains(string(raw), "To: recipient@example.com") || !strings.Contains(string(raw), "Subject: Status") {
		t.Fatalf("send = %+v, raw = %q, err = %v decode = %v", sent, raw, sendErr, decodeErr)
	}
}

func TestProvider_rejectsMissingSDKService(t *testing.T) {
	if err := New(nil).Validate(context.Background()); err == nil {
		t.Fatal("nil service was accepted")
	}
}
