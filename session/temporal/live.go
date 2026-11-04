package temporal

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
)

// Live is how a Client reaches the live endpoints of the workers
// (worker.Config.Live), where the deltas of their model calls stream.
// Deltas are never stored, so Subscribe dials the worker that holds the
// session's harness.
type Live struct {
	// Key is the HMAC key the workers have. A zero Key dials no worker.
	Key []byte
	// HTTP is the client that dials them; zero is http.DefaultClient.
	HTTP *http.Client
}

// The waits between asks for a session's live URL: while it has none, and
// after a connection ends.
var (
	liveRetry    = 100 * time.Millisecond
	liveRetryMax = time.Second
)

// liveLineMax is the longest line a live stream may send: one delta.
const liveLineMax = 1 << 20

// deltas is the session's deltas until ctx ends. It asks the workflow for
// its worker's live URL and reads that worker's stream, and asks again
// when the stream ends (the session moved to another worker).
func (r *Client) deltas(ctx context.Context, id session.ID) <-chan session.Event {
	out := make(chan session.Event, 64)
	go func() {
		defer close(out)
		wait := liveRetry
		for {
			if base := r.liveURL(ctx, id); base != "" && r.readLive(ctx, base, id, out) {
				wait = liveRetry
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
			wait = min(wait*2, liveRetryMax)
		}
	}()
	return out
}

// liveURL is the live URL the session's workflow has; "" when it has none
// or does not answer.
func (r *Client) liveURL(ctx context.Context, id session.ID) string {
	val, err := r.client.QueryWorkflow(ctx, string(id), "", protocol.QueryLive)
	if err != nil {
		return ""
	}
	var u string
	_ = val.Get(&u)
	return u
}

// readLive sends the deltas of id's stream at base to out until the
// stream or ctx ends. It reports whether the worker accepted the request.
func (r *Client) readLive(ctx context.Context, base string, id session.ID, out chan<- session.Event) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(base, "/")+protocol.LivePath+url.PathEscape(string(id)), nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+protocol.LiveToken(r.live.Key, id))
	hc := r.live.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	lines := bufio.NewScanner(resp.Body)
	lines.Buffer(nil, liveLineMax)
	for lines.Scan() {
		var ev session.Event
		if len(lines.Bytes()) == 0 || json.Unmarshal(lines.Bytes(), &ev) != nil {
			continue // a line this client cannot read
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			return true
		}
	}
	return true
}
