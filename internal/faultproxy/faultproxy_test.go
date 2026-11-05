package faultproxy_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/faultproxy"
)

// TestProxy_appliesTheRuleToTheRequestsItMatches: with no rule the proxy
// forwards; a rule answers, refuses, resets, cuts or hangs the requests it
// matches and forwards the others; a Once rule applies to one request.
func TestProxy_appliesTheRuleToTheRequestsItMatches(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, r.Body)
		_, _ = io.WriteString(w, "-backend")
	}))
	t.Cleanup(backend.Close)
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	p := faultproxy.Start(t, target)
	post := func(ctx context.Context, path, body string) (string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL()+path, strings.NewReader(body))
		if err != nil {
			return "", err
		}
		// A new connection each time: a fault closes the one it hits.
		req.Close = true
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer res.Body.Close()
		got, err := io.ReadAll(res.Body)
		return res.Status + " " + string(got), err
	}
	onlyFault := func(r *http.Request) bool {
		b, _ := io.ReadAll(r.Body)
		return string(b) == "fault"
	}

	forwarded, forwardErr := post(t.Context(), "/", "plain")
	p.Set(faultproxy.Rule{Match: onlyFault, Fault: faultproxy.Answer(http.StatusTeapot, "short"), Once: true})
	answered, answerErr := post(t.Context(), "/", "fault")
	afterOnce, afterOnceErr := post(t.Context(), "/", "fault")
	p.Set(faultproxy.Rule{Match: onlyFault, Fault: faultproxy.Refuse()})
	unmatched, unmatchedErr := post(t.Context(), "/", "other")
	_, refuseErr := post(t.Context(), "/", "fault")
	p.Set(faultproxy.Rule{Fault: faultproxy.Reset()})
	_, resetErr := post(t.Context(), "/", "x")
	p.Set(faultproxy.Rule{Fault: faultproxy.Cut(3)})
	cut, cutErr := post(t.Context(), "/", "abcdef")
	p.Set(faultproxy.Rule{Fault: faultproxy.Hang()})
	short, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	_, hangErr := post(short, "/", "x")
	cancel()
	p.Clear()
	cleared, clearedErr := post(t.Context(), "/", "back")

	for name, tc := range map[string]struct {
		got, want string
		err       error
	}{
		"no rule":            {forwarded, "200 OK plain-backend", forwardErr},
		"a matched answer":   {answered, "418 I'm a teapot short", answerErr},
		"after a Once rule":  {afterOnce, "200 OK fault-backend", afterOnceErr},
		"an unmatched rule":  {unmatched, "200 OK other-backend", unmatchedErr},
		"a cleared rule":     {cleared, "200 OK back-backend", clearedErr},
		"a cut body, so far": {cut, "200 OK abc", nil},
	} {
		if tc.got != tc.want || tc.err != nil {
			t.Errorf("%s = %q %v, want %q", name, tc.got, tc.err, tc.want)
		}
	}
	if !errors.Is(refuseErr, io.EOF) {
		t.Errorf("refused request = %v, want EOF", refuseErr)
	}
	if !errors.Is(resetErr, syscall.ECONNRESET) {
		t.Errorf("reset request = %v, want connection reset", resetErr)
	}
	if !errors.Is(cutErr, io.ErrUnexpectedEOF) {
		t.Errorf("cut body = %v, want an unexpected EOF", cutErr)
	}
	if !errors.Is(hangErr, context.DeadlineExceeded) {
		t.Errorf("hung request = %v, want the caller's deadline", hangErr)
	}
}
