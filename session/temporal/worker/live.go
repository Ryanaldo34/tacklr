package worker

import (
	"crypto/hmac"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
)

// LiveHandler serves the deltas of the sessions whose harnesses this
// worker holds: GET protocol.LivePath plus the session id streams one JSON
// event per line until the client goes away. The request must carry the
// session's bearer token (temporal.Config.Live has the key). The host
// serves the handler at Config.Live.URL; without Config.Live it refuses
// every request.
func (w *Worker) LiveHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		id, ok := strings.CutPrefix(r.URL.Path, protocol.LivePath)
		if r.Method != http.MethodGet || !ok || id == "" {
			http.NotFound(rw, r)
			return
		}
		token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if len(w.liveKey) == 0 || !hmac.Equal([]byte(token), []byte(protocol.LiveToken(w.liveKey, session.ID(id)))) {
			http.Error(rw, "unauthorized", http.StatusUnauthorized)
			return
		}
		rw.Header().Set("Content-Type", "application/x-ndjson")
		rw.WriteHeader(http.StatusOK)
		deltas := w.live.Subscribe(r.Context(), session.ID(id))
		flush := http.NewResponseController(rw)
		_ = flush.Flush() // the client knows it is listening
		enc := json.NewEncoder(rw)
		// The stream ends when the client goes away. It sends nothing between
		// turns, and a proxy may close it then: the client dials again.
		for ev := range deltas {
			if enc.Encode(ev) != nil || flush.Flush() != nil { // strings and numbers only; Error is not encoded
				return
			}
		}
	})
}
