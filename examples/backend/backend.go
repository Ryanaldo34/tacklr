// Package backend is the examples' backend: a small HTTP API over a
// session.Client, behind one bearer token. It is how a program drives
// Tacklr sessions: yours has its own routes, users and clients, over the
// same session.Client. The Temporal example serves it with net/http
// (examples/temporal/cmd/backend), and the Cloudflare example serves it
// from its Worker (examples/cloudflare).
//
//	POST /sessions                  creates a session: {"id"}
//	POST /sessions/{id}/prompt      sends {"text"}: 202
//	POST /sessions/{id}/resume      answers interrupts {"responses": {messageID: answer}}: 202
//	POST /sessions/{id}/cancel      cancels the turn: 202
//	GET  /sessions/{id}/events      follows the session's events from now (or ?after=seq)
//	                                as server-sent events, one JSON session.Event each
//
// A request's traceparent header makes the session's work part of the
// caller's trace.
package backend

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/propagation"

	"github.com/ryanaldo34/tacklr/session"
)

// maxBody is the largest request body the backend reads.
const maxBody = 1 << 20

// New serves sessions behind token, which each request must carry as its
// bearer.
func New(sessions session.Client, token string) http.Handler {
	b := backend{sessions: sessions}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sessions", b.create)
	mux.HandleFunc("POST /sessions/{id}/prompt", b.prompt)
	mux.HandleFunc("POST /sessions/{id}/resume", b.resume)
	mux.HandleFunc("POST /sessions/{id}/cancel", b.cancel)
	mux.HandleFunc("GET /sessions/{id}/events", b.events)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(bearer), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

type backend struct {
	sessions session.Client
}

func (b backend) create(w http.ResponseWriter, r *http.Request) {
	id, err := b.sessions.CreateSession(r.Context(), session.CreateSession{})
	answer(w, http.StatusCreated, map[string]session.ID{"id": id}, err)
}

func (b backend) prompt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	if !read(w, r, &body) {
		return
	}
	answer(w, http.StatusAccepted, nil, b.sessions.Prompt(r.Context(), session.ID(r.PathValue("id")), session.Prompt{Text: body.Text}))
}

func (b backend) resume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Responses map[string]json.RawMessage `json:"responses"`
	}
	if !read(w, r, &body) {
		return
	}
	responses := make(map[string][]byte, len(body.Responses))
	for id, answer := range body.Responses {
		responses[id] = answer
	}
	answer(w, http.StatusAccepted, nil, b.sessions.Resume(r.Context(), session.ID(r.PathValue("id")), session.Resume{Responses: responses}))
}

func (b backend) cancel(w http.ResponseWriter, r *http.Request) {
	answer(w, http.StatusAccepted, nil, b.sessions.Cancel(r.Context(), session.ID(r.PathValue("id"))))
}

// events streams the session's events until the caller leaves, which ends
// the subscription, or the session's event stream ends.
func (b backend) events(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), session.ID(r.PathValue("id"))
	after, err := b.after(ctx, id, r.URL.Query().Get("after"))
	if err != nil {
		fail(w, err)
		return
	}
	sub, err := b.sessions.Subscribe(ctx, id, after)
	if err != nil {
		fail(w, err)
		return
	}
	defer sub.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	stream := http.NewResponseController(w)
	_ = stream.Flush()
	for ev := range sub.Events() {
		data, _ := json.Marshal(ev)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		_ = stream.Flush()
	}
}

// after is the sequence the events start after: the after query, or the
// session's head.
func (b backend) after(ctx context.Context, id session.ID, query string) (session.Seq, error) {
	if query == "" {
		return b.sessions.Head(ctx, id)
	}
	after, err := strconv.ParseUint(query, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: after: %w", session.ErrInvalid, err)
	}
	return session.Seq(after), nil
}

// read decodes the request's JSON body into v, or answers 400.
func read(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(v); err != nil {
		http.Error(w, "the body is not valid JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// answer answers status with v as JSON (none when v is nil), or the status
// of err.
func answer(w http.ResponseWriter, status int, v any, err error) {
	if err != nil {
		fail(w, err)
		return
	}
	if v == nil {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// fail answers err with its status: 404 for a session that is not there,
// 400 for a request the session refuses, else 500.
func fail(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, session.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, session.ErrInvalid):
		status = http.StatusBadRequest
	}
	http.Error(w, err.Error(), status)
}
