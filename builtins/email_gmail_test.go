package builtins

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	googlemail "google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

func TestProvider_usesOfficialGmailSDKForInboxAndSend(t *testing.T) {
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
				"id": "m1", "threadId": "t1", "labelIds": []string{"UNREAD"},
				"payload": map[string]any{
					"mimeType": "multipart/alternative",
					"headers": []map[string]string{
						{"name": "From", "value": "sender@example.com"}, {"name": "To", "value": "recipient@example.com"},
						{"name": "Subject", "value": "Status"}, {"name": "Date", "value": "Thu, 9 Oct 2025 08:53:20 +0000"},
					},
					"parts": []map[string]any{{
						"mimeType": "text/plain",
						"body":     map[string]string{"data": base64.RawURLEncoding.EncodeToString([]byte("hello"))},
					}},
				},
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
	p := Gmail(service)
	if p.Kind() != ProviderGmail {
		t.Fatal(p.Kind())
	}

	hasAttachment := true
	inbox, err := p.ReadInbox(t.Context(), ReadInboxRequest{From: "sender@example.com", To: "recipient@example.com", Subject: "Status", ReceivedAfter: "2026-08-01", ReceivedBefore: "2026-08-31", HasAttachment: &hasAttachment, Mailbox: "INBOX", UnreadOnly: true, Limit: 5, Cursor: "cursor"})
	sent, sendErr := p.SendEmail(t.Context(), SendEmailRequest{
		To: []string{"recipient@example.com"}, CC: []string{"cc@example.com"}, BCC: []string{"bcc@example.com"},
		Subject: "Status", Body: "ready", ReplyToMessageID: "m1",
	})

	if err != nil || len(inbox.Messages) != 1 || inbox.Messages[0].Body != "hello" || !inbox.Messages[0].Unread || inbox.NextCursor != "next" {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	for _, want := range []string{"from%3A%22sender%40example.com%22", "to%3A%22recipient%40example.com%22", "subject%3A%22Status%22", "after%3A2026%2F08%2F01", "before%3A2026%2F08%2F31", "has%3Aattachment", "is%3Aunread", "labelIds=INBOX", "maxResults=5", "pageToken=cursor"} {
		if !strings.Contains(listQuery, want) {
			t.Fatalf("list query = %q, want %q", listQuery, want)
		}
	}
	raw, decodeErr := base64.RawURLEncoding.DecodeString(sentRaw)
	rfc := string(raw)
	if sendErr != nil || decodeErr != nil || sent.ID != "sent" || !strings.Contains(rfc, "To: recipient@example.com") || !strings.Contains(rfc, "Cc: cc@example.com") || !strings.Contains(rfc, "Bcc: bcc@example.com") || !strings.Contains(rfc, "In-Reply-To: m1") || !strings.Contains(rfc, "Subject: Status") {
		t.Fatalf("send = %+v, raw = %q, err = %v decode = %v", sent, raw, sendErr, decodeErr)
	}
	if _, err := p.ReadInbox(t.Context(), ReadInboxRequest{ReceivedAfter: "nope"}); err == nil {
		t.Fatal("invalid date was accepted")
	}
	if _, err := p.ReadInbox(t.Context(), ReadInboxRequest{}); err != nil {
		t.Fatalf("unfiltered inbox: %v", err)
	}
}

func TestGmailProvider_surfacesSDKErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gmail/v1/users/me/messages" {
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": []map[string]string{{"id": "m1"}}})
			return
		}
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	service, err := googlemail.NewService(t.Context(), option.WithHTTPClient(oauth2.NewClient(t.Context(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}))), option.WithEndpoint(server.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	p := Gmail(service)
	if _, err := p.ReadInbox(t.Context(), ReadInboxRequest{}); err == nil || !strings.Contains(err.Error(), "get message") {
		t.Fatalf("get: %v", err)
	}
	failList := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(failList.Close)
	listSvc, err := googlemail.NewService(t.Context(), option.WithHTTPClient(oauth2.NewClient(t.Context(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}))), option.WithEndpoint(failList.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Gmail(listSvc).ReadInbox(t.Context(), ReadInboxRequest{}); err == nil || !strings.Contains(err.Error(), "list messages") {
		t.Fatalf("list: %v", err)
	}
	if _, err := Gmail(listSvc).SendEmail(t.Context(), SendEmailRequest{To: []string{"a@b.c"}, Subject: "s", Body: "b"}); err == nil || !strings.Contains(err.Error(), "send message") {
		t.Fatalf("send: %v", err)
	}
}

func TestGmailQuery_escapesStructuredFilters(t *testing.T) {
	hasAttachment := false
	got := gmailQuery(ReadInboxRequest{From: "a'@example.com", Subject: `weekly "review"`, HasAttachment: &hasAttachment})
	for _, want := range []string{`from:"a'@example.com"`, `subject:"weekly \"review\""`, "-has:attachment"} {
		if !strings.Contains(got, want) {
			t.Fatalf("query = %q, want %q", got, want)
		}
	}
}

func TestPayloadText_walksPartsAndEmptyBodies(t *testing.T) {
	if payloadText(nil) != "" {
		t.Fatal("nil part")
	}
	if payloadText(&googlemail.MessagePart{MimeType: "text/html", Parts: []*googlemail.MessagePart{{MimeType: "application/octet-stream"}}}) != "" {
		t.Fatal("no plaintext")
	}
}

func TestProvider_rejectsMissingSDKService(t *testing.T) {
	p := Gmail(nil)
	if err := p.Validate(context.Background()); err == nil {
		t.Fatal("nil service was accepted")
	}
	if _, err := p.ReadInbox(context.Background(), ReadInboxRequest{}); err == nil {
		t.Fatal("nil service listed mail")
	}
	if _, err := p.SendEmail(context.Background(), SendEmailRequest{To: []string{"a@b.c"}, Subject: "s", Body: "b"}); err == nil {
		t.Fatal("nil service sent mail")
	}
}
