package durableobject_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
)

// answerWith is a platform's network that answers every request the same
// way: an error page from the platform itself, or a body that is not the
// object protocol.
type answerWith struct {
	status int
	body   string
}

func (a answerWith) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: a.status, Status: http.StatusText(a.status), Body: io.NopCloser(strings.NewReader(a.body)), Request: req}, nil
}

// TestRemote_aCallerGetsWhyTheObjectOrItsNetworkFailed: a request the
// object cannot read is invalid; an object whose storage is down answers
// why; a platform error page, an answer or an event that is not the
// protocol, a value that cannot be sent, and a caller that left each fail
// the call with their own error.
func TestRemote_aCallerGetsWhyTheObjectOrItsNetworkFailed(t *testing.T) {
	f := newWireFixture(t, callThenAnswer("none", `{}`), nil)
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(method, target, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		durableobject.Handler(f.p.Object(id)).ServeHTTP(rec, httptest.NewRequestWithContext(ctx, method, target, strings.NewReader(body)))
		return rec
	}
	bad := map[string]*httptest.ResponseRecorder{
		"init":  serve(http.MethodPost, "/create", "{"),
		"wake":  serve(http.MethodPost, "/wake", "{"),
		"after": serve(http.MethodGet, "/subscribe?after=x", ""),
	}
	f.p.FailWrites(id, always)
	outage := f.c.Prompt(ctx, id, session.Prompt{Text: "hi"})
	f.p.FailWrites(id, nil)
	page := durableobject.Remote(&http.Client{Transport: answerWith{http.StatusBadGateway, "<html>bad gateway</html>"}})
	garbled := durableobject.Remote(&http.Client{Transport: answerWith{http.StatusOK, "not json\n"}})
	_, pageErr := page.Status(ctx)
	_, garbledAnswer := garbled.Head(ctx)
	sub, err := garbled.Subscribe(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	for range sub.Events() {
	}
	garbledEvent := sub.Err()
	unsendable := page.Create(ctx, durableobject.Init{SessionID: "x", State: map[string]any{"f": func() {}}})
	left, cancel := context.WithCancel(ctx)
	cancel()
	_, leftErr := f.p.Object(id).Jobs(left)

	for name, rec := range bad {
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error":"invalid"`) {
			t.Fatalf("a bad %s: %d %s; want it invalid", name, rec.Code, rec.Body)
		}
	}
	if outage == nil || errors.Is(outage, session.ErrNotFound) || !strings.Contains(outage.Error(), "storage unavailable") {
		t.Fatalf("prompt in an outage: %v; want the object's own error", outage)
	}
	if pageErr == nil || !strings.Contains(pageErr.Error(), "Bad Gateway") {
		t.Fatalf("an error page: %v; want its status", pageErr)
	}
	for name, err := range map[string]error{"answer": garbledAnswer, "event": garbledEvent, "value": unsendable} {
		if err == nil {
			t.Fatalf("a bad %s went through; want an error", name)
		}
	}
	if !errors.Is(leftErr, context.Canceled) {
		t.Fatalf("a caller that left: %v; want its context's error", leftErr)
	}
}
