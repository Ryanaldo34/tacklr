package durableobject_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

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
	t.Parallel()
	// Arrange
	f := newWireFixture(t, callThenAnswer("none", `{}`), nil)
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	serveBody := func(method, target string, body io.Reader) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		durableobject.Handler(f.p.Object(id)).ServeHTTP(rec, httptest.NewRequestWithContext(ctx, method, target, body))
		return rec
	}
	serve := func(method, target, body string) *httptest.ResponseRecorder {
		return serveBody(method, target, strings.NewReader(body))
	}

	// Act
	bad := map[string]*httptest.ResponseRecorder{
		"init":  serve(http.MethodPost, "/create", "{"),
		"wake":  serve(http.MethodPost, "/wake", "{"),
		"after": serve(http.MethodGet, "/subscribe?after=x", ""),
		"wire":  serveBody(http.MethodPut, "/wire", iotest.ErrReader(errors.New("the body broke"))),
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

	// Assert
	for name, rec := range bad {
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error":"invalid"`) {
			t.Fatalf("a bad %s: %d %s; want it invalid", name, rec.Code, rec.Body)
		}
	}
	if outage == nil || !strings.Contains(outage.Error(), "storage unavailable") {
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

// TestWireStore_keepsEachSessionsWireStateInItsObject: a protocol's wire
// state for a session is kept in the session's object, across the object
// protocol: it reads back as it was put, an empty one too; a delete or the
// session's close deletes it; a session that is not there has none to
// read or put, and nothing to delete; and an object whose storage is down
// fails the read and the put.
func TestWireStore_keepsEachSessionsWireStateInItsObject(t *testing.T) {
	t.Parallel()
	// Arrange
	f := newWireFixture(t, callThenAnswer("none", `{}`), nil)
	ctx := t.Context()
	wire := f.c.WireStore()
	newSession := func() string {
		t.Helper()
		id, err := f.c.CreateSession(ctx, session.CreateSession{})
		if err != nil {
			t.Fatal(err)
		}
		return string(id)
	}
	kept, emptied, closed, broken := newSession(), newSession(), newSession(), newSession()

	// Act
	_, before := wire.Get(ctx, kept)
	putErr := errors.Join(wire.Put(ctx, kept, []byte(`{"cwd":"/a"}`)), wire.Put(ctx, emptied, nil), wire.Put(ctx, closed, []byte(`{}`)))
	got, getErr := wire.Get(ctx, kept)
	empty, emptyErr := wire.Get(ctx, emptied)
	deleteErr := wire.Delete(ctx, emptied)
	_, deleted := wire.Get(ctx, emptied)
	if err := f.c.Close(ctx, session.ID(closed)); err != nil {
		t.Fatal(err)
	}
	if err := f.p.Idle(ctx, session.ID(closed)); err != nil {
		t.Fatal(err)
	}
	_, missingGet := wire.Get(ctx, "missing")
	missingPut := wire.Put(ctx, "missing", []byte(`{}`))
	missingDelete := wire.Delete(ctx, "missing")
	f.p.FailReads(session.ID(broken), func(string) bool { return true })
	_, readOutage := wire.Get(ctx, broken)
	f.p.FailReads(session.ID(broken), nil)
	f.p.FailWrites(session.ID(broken), always)
	writeOutage := wire.Put(ctx, broken, []byte(`{}`))
	f.p.FailWrites(session.ID(broken), nil)

	// Assert
	if !errors.Is(before, session.ErrNotFound) || putErr != nil || getErr != nil || string(got) != `{"cwd":"/a"}` {
		t.Fatalf("before = %v, put = %v, get = %q (%v); want none, then what was put", before, putErr, got, getErr)
	}
	if emptyErr != nil || len(empty) != 0 || deleteErr != nil || !errors.Is(deleted, session.ErrNotFound) {
		t.Fatalf("empty = %q (%v), delete = %v, then %v; want it empty, then gone", empty, emptyErr, deleteErr, deleted)
	}
	for _, k := range f.p.Keys(session.ID(closed)) {
		if strings.HasPrefix(k, "wire/") {
			t.Fatalf("a closed session's object keeps %q, want its wire state deleted", k)
		}
	}
	if !errors.Is(missingGet, session.ErrNotFound) || !errors.Is(missingPut, session.ErrNotFound) || missingDelete != nil {
		t.Fatalf("missing session: get = %v, put = %v, delete = %v; want not found, not found, nil", missingGet, missingPut, missingDelete)
	}
	if readOutage == nil || writeOutage == nil {
		t.Fatalf("storage down: read = %v, write = %v; want both to fail", readOutage, writeOutage)
	}
}
