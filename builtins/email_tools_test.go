package builtins

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"

	"github.com/ryanaldo34/tacklr"
)

func outlookOverHTTP(t *testing.T, handler http.HandlerFunc) EmailProvider {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	adapter, err := msgraphsdk.NewGraphRequestAdapterWithParseNodeFactoryAndSerializationWriterFactoryAndHttpClient(testAuth{}, nil, nil, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	adapter.SetBaseUrl(server.URL)
	return Outlook(msgraphsdk.NewGraphServiceClient(adapter))
}

func TestEmailTools_inboxAndSendOverGraph(t *testing.T) {
	var listFilter string
	var sent map[string]any
	page := map[string]any{
		"value": []map[string]any{{
			"id": "message-1", "conversationId": "thread", "subject": "Status", "isRead": false,
			"from": map[string]any{"emailAddress": map[string]string{"address": "sender@example.com"}},
			"body": map[string]string{"content": "hello", "contentType": "text"},
		}},
	}
	p := outlookOverHTTP(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := strings.TrimSuffix(r.URL.Path, "/")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/messages"):
			listFilter = r.URL.Query().Get("$filter")
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/sendMail"):
			_ = json.NewDecoder(r.Body).Decode(&sent)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	})

	hasAttachment := true
	inbox, err := runReadInbox(t.Context(), p, readInboxArgs{
		From: "sender@example.com", Subject: "Status", UnreadOnly: true, HasAttachment: &hasAttachment,
	})
	if err != nil || len(inbox.Messages) != 1 || inbox.Messages[0].ID != "message-1" {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	if !strings.Contains(listFilter, "from/emailAddress/address eq 'sender@example.com'") {
		t.Fatalf("filter = %q", listFilter)
	}

	if _, err := runSendEmail(t.Context(), p, sendEmailArgs{
		To: []string{"owner@example.com"}, Subject: "Status", Body: "Ready",
	}); err != nil {
		t.Fatal(err)
	}
	message, _ := sent["Message"].(map[string]any)
	if message["subject"] != "Status" {
		t.Fatalf("send payload = %+v", sent)
	}

	tool := ReadInbox(p)
	if tool.Name() != "read_inbox" || tool.Access() != tacklr.ToolReadAccess || tool.Category() != tacklr.ToolCategoryRead {
		t.Fatalf("tool = %s access=%v category=%s", tool.Name(), tool.Access(), tool.Category())
	}
	props, _ := tool.AsJson()["parameters"].(map[string]any)["properties"].(map[string]any)
	if _, exists := props["query"]; exists {
		t.Fatal("read_inbox schema exposes a provider query parameter")
	}
	for _, name := range []string{"from", "to", "subject", "received_after", "received_before", "has_attachment"} {
		if _, exists := props[name]; !exists {
			t.Fatalf("read_inbox schema missing %q", name)
		}
	}
	send := SendEmail(p)
	if send.Name() != "send_email" || send.Access() != tacklr.ToolWriteAccess {
		t.Fatalf("tool = %s access=%v", send.Name(), send.Access())
	}
}

func TestEmailTools_validateRequestsAndProviderErrors(t *testing.T) {
	p := outlookOverHTTP(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "inbox unavailable", http.StatusInternalServerError)
	})
	if _, err := runReadInbox(t.Context(), p, readInboxArgs{Limit: 101}); err == nil || !strings.Contains(err.Error(), "between 1 and 100") {
		t.Fatalf("read limit error = %v", err)
	}
	if _, err := runReadInbox(t.Context(), p, readInboxArgs{ReceivedAfter: "not-a-date"}); err == nil || !strings.Contains(err.Error(), "YYYY-MM-DD") {
		t.Fatalf("read filter validation error = %v", err)
	}
	if _, err := runSendEmail(t.Context(), p, sendEmailArgs{}); err == nil || !strings.Contains(err.Error(), "recipient") {
		t.Fatalf("send validation error = %v", err)
	}
	if _, err := runReadInbox(t.Context(), p, readInboxArgs{}); err == nil {
		t.Fatal("provider error was swallowed")
	}
	if _, err := runSendEmail(t.Context(), p, sendEmailArgs{To: []string{"a@example.com"}, Subject: "S", Body: "B"}); err == nil {
		t.Fatal("send provider error was swallowed")
	}
	if _, err := runReadInbox(t.Context(), nil, readInboxArgs{}); err == nil || !strings.Contains(err.Error(), "email provider is required") {
		t.Fatalf("nil read provider = %v", err)
	}
	if _, err := runSendEmail(t.Context(), nil, sendEmailArgs{To: []string{"a@example.com"}, Subject: "S", Body: "B"}); err == nil || !strings.Contains(err.Error(), "email provider is required") {
		t.Fatalf("nil send provider = %v", err)
	}
}

func TestEmailConstructors_panicWithoutProvider(t *testing.T) {
	for _, fn := range []func(){
		func() { ReadInbox(nil) },
		func() { SendEmail(nil) },
	} {
		var panicked bool
		func() {
			defer func() { panicked = recover() != nil }()
			fn()
		}()
		if !panicked {
			t.Fatal("nil provider constructor did not panic")
		}
	}
}
