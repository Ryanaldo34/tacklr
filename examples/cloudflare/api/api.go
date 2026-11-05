// Package api is the example Worker's front door: a small HTTP API over a
// session.Client, behind one bearer token. It is the example's own, not a
// Tacklr protocol; ACP is the protocol for real clients.
//
//	POST   /sessions                 {"id"?, "specialist"?}  -> 201 {"id"}
//	POST   /sessions/{id}/prompt     {"text"}                -> 202
//	POST   /sessions/{id}/resume     {"responses": {id: any}} -> 202
//	POST   /sessions/{id}/cancel                             -> 202
//	DELETE /sessions/{id}                                    -> 204
//	GET    /sessions/{id}                                    -> the status
//	GET    /sessions/{id}/jobs                               -> its jobs' statuses
//	GET    /sessions/{id}/events?after=N                     -> NDJSON events
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/session"
)

// Handler serves the API over c. A request needs "Authorization: Bearer
// token"; an empty token refuses every request.
func Handler(c session.Client, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sessions", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ID         session.ID `json:"id"`
			Specialist string     `json:"specialist"`
		}
		if !decode(w, r, &in) {
			return
		}
		id, err := c.CreateSession(r.Context(), session.CreateSession{SessionID: in.ID, Specialist: in.Specialist})
		answer(w, http.StatusCreated, map[string]session.ID{"id": id}, err)
	})
	mux.HandleFunc("POST /sessions/{id}/prompt", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Text string `json:"text"`
		}
		if !decode(w, r, &in) {
			return
		}
		answer(w, http.StatusAccepted, nil, c.Prompt(r.Context(), id(r), session.Prompt{Text: in.Text}))
	})
	mux.HandleFunc("POST /sessions/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Responses map[string]json.RawMessage `json:"responses"`
		}
		if !decode(w, r, &in) {
			return
		}
		responses := make(map[string][]byte, len(in.Responses))
		for k, v := range in.Responses {
			responses[k] = v
		}
		answer(w, http.StatusAccepted, nil, c.Resume(r.Context(), id(r), session.Resume{Responses: responses}))
	})
	mux.HandleFunc("POST /sessions/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		answer(w, http.StatusAccepted, nil, c.Cancel(r.Context(), id(r)))
	})
	mux.HandleFunc("DELETE /sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		answer(w, http.StatusNoContent, nil, c.Close(r.Context(), id(r)))
	})
	mux.HandleFunc("GET /sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		st, err := c.Status(r.Context(), id(r))
		answer(w, http.StatusOK, st, err)
	})
	mux.HandleFunc("GET /sessions/{id}/jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs, err := c.Jobs(r.Context(), id(r))
		answer(w, http.StatusOK, jobs, err)
	})
	mux.HandleFunc("GET /sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if err != nil {
			answer(w, 0, nil, fmt.Errorf("%w: after: %w", session.ErrInvalid, err))
			return
		}
		sub, err := c.Subscribe(r.Context(), id(r), session.Seq(after))
		if err != nil {
			answer(w, 0, nil, err)
			return
		}
		defer func() { _ = sub.Close() }()
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flush := http.NewResponseController(w)
		enc := json.NewEncoder(w)
		// A write fails only once the caller left, which ends the
		// subscription.
		for ev := range sub.Events() {
			_ = enc.Encode(ev)
			_ = flush.Flush()
		}
	})
	return authorized(token, mux)
}

// authorized serves next only to requests with the bearer token.
func authorized(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func id(r *http.Request) session.ID { return session.ID(r.PathValue("id")) }

// decode reads r's JSON body into v, or answers that it is invalid. An
// empty body is an empty value.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.ContentLength == 0 {
		return true
	}
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		answer(w, 0, nil, fmt.Errorf("%w: %w", session.ErrInvalid, err))
		return false
	}
	return true
}

// answer writes v with status, or err with its status.
func answer(w http.ResponseWriter, status int, v any, err error) {
	if err != nil {
		code := http.StatusInternalServerError
		switch {
		case errors.Is(err, session.ErrNotFound):
			code = http.StatusNotFound
		case errors.Is(err, session.ErrExists):
			code = http.StatusConflict
		case errors.Is(err, session.ErrInvalid):
			code = http.StatusBadRequest
		}
		http.Error(w, err.Error(), code)
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
