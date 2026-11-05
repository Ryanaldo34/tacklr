package web

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestExa_rejectsARequestItCannotSend: a request with no query, no URLs or
// IDs, both URLs and IDs, no client, or a base URL that does not parse fails
// before the client sends it.
func TestExa_rejectsARequestItCannotSend(t *testing.T) {
	ctx := t.Context()
	unparsable := NewExa("k")
	unparsable.BaseURL = "://bad"
	var none *Exa

	_, noQuery := NewExa("k").Search(ctx, SearchRequest{})
	_, noURLs := NewExa("k").Contents(ctx, ContentsRequest{})
	_, urlsAndIDs := NewExa("k").Contents(ctx, ContentsRequest{URLs: []string{"https://a.com"}, IDs: []string{"id1"}})
	_, noClient := none.Contents(ctx, ContentsRequest{URLs: []string{"https://a.com"}})
	_, badBase := unparsable.Search(ctx, SearchRequest{Query: "q"})

	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"no query":          {noQuery, "query is required"},
		"no urls or ids":    {noURLs, "urls or ids is required"},
		"urls and ids":      {urlsAndIDs, "not both"},
		"no client":         {noClient, "client is nil"},
		"an unparsable url": {badBase, "build request"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want %q", name, tc.err, tc.want)
		}
	}
}

// TestExa_reportsWhatTheServerAnswers: an empty base URL calls Exa's own
// API, a nil HTTP client uses a default one, and an error status reports its
// body, cut to a short length, or the status text when the body is empty or
// breaks off.
func TestExa_reportsWhatTheServerAnswers(t *testing.T) {
	ctx := t.Context()
	exa := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "api.exa.ai" || r.URL.Path != "/search" || r.Header.Get("x-api-key") != "k" {
			http.Error(w, "wrong endpoint", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"From Exa"}]}`))
	}))
	t.Cleanup(exa.Close)
	// The transport dials the test server for api.exa.ai and trusts its certificate.
	toExa := exa.Client().Transport.(*http.Transport).Clone()
	toExa.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, exa.Listener.Addr().String())
	}
	toExa.TLSClientConfig.ServerName = "example.com"
	noBase := NewExa("k")
	noBase.BaseURL = ""
	noBase.HTTPClient = &http.Client{Transport: toExa}
	noHTTPClient := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"results":[{"title":"Default client"}]}`))
	})
	noHTTPClient.HTTPClient = nil
	emptyBody := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	longBody := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, strings.Repeat("x", 600), http.StatusBadRequest)
	})
	cutBody := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_, _ = buf.WriteString("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 100\r\n\r\npartial")
		_ = buf.Flush()
		_ = conn.Close()
	})
	search := func(c *Exa) (string, error) {
		resp, err := c.Search(ctx, SearchRequest{Query: "q"})
		if err != nil {
			return "", err
		}
		return resp.Results[0].Title, nil
	}

	fromExa, exaErr := search(noBase)
	fromDefault, defaultErr := search(noHTTPClient)
	_, emptyErr := search(emptyBody)
	_, longErr := search(longBody)
	_, cutErr := search(cutBody)
	var noStatus *StatusError

	if exaErr != nil || fromExa != "From Exa" || defaultErr != nil || fromDefault != "Default client" {
		t.Errorf("no base url = %q %v, no http client = %q %v, want both answered", fromExa, exaErr, fromDefault, defaultErr)
	}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"an empty body":      {emptyErr, "status 500: 500 Internal Server Error"},
		"a long body":        {longErr, strings.Repeat("x", maxErrorBody) + "…"},
		"a body that breaks": {cutErr, "read body"},
		"a nil status error": {noStatus, "exa: status error"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want %q", name, tc.err, tc.want)
		}
	}
	if noStatus.PublicationDomain() {
		t.Error("a nil status error names a publication domain")
	}
}
