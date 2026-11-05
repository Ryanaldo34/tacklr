package durableobject

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ryanaldo34/tacklr/session"
)

// The object protocol is how objects, and the platform's front, reach an
// object over HTTP: Handler serves one object's Ref and Remote is the
// caller's side. The protocol has no authentication. A platform keeps it
// inside its own network (on Cloudflare, a Durable Object stub's fetch),
// and the bodies carry credentials in clear text.

// Error kinds of the object protocol: each is a session sentinel the
// caller gets back.
const (
	wireNotFound = "not_found"
	wireExists   = "exists"
	wireInvalid  = "invalid"
)

var wireKinds = []struct {
	kind   string
	status int
	err    error
}{
	{wireNotFound, http.StatusNotFound, session.ErrNotFound},
	{wireExists, http.StatusConflict, session.ErrExists},
	{wireInvalid, http.StatusBadRequest, session.ErrInvalid},
}

// wireError is an error as the protocol sends it.
type wireError struct {
	Kind    string `json:"error,omitempty"`
	Message string `json:"message"`
}

// Handler serves ref over the object protocol.
func Handler(ref Ref) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /create", func(w http.ResponseWriter, r *http.Request) {
		var init Init
		if !decodeBody(w, r, &init) {
			return
		}
		reply(w, nil, ref.Create(r.Context(), init))
	})
	mux.HandleFunc("POST /wake", func(w http.ResponseWriter, r *http.Request) {
		var wake session.Wake
		if !decodeBody(w, r, &wake) {
			return
		}
		reply(w, nil, ref.Wake(r.Context(), wake))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st, err := ref.Status(r.Context())
		reply(w, st, err)
	})
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs, err := ref.Jobs(r.Context())
		reply(w, jobs, err)
	})
	mux.HandleFunc("GET /head", func(w http.ResponseWriter, r *http.Request) {
		head, err := ref.Head(r.Context())
		reply(w, head, err)
	})
	mux.HandleFunc("GET /subscribe", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if err != nil {
			reply(w, nil, fmt.Errorf("%w: after: %w", session.ErrInvalid, err))
			return
		}
		sub, err := ref.Subscribe(r.Context(), session.Seq(after))
		if err != nil {
			reply(w, nil, err)
			return
		}
		defer func() { _ = sub.Close() }()
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flush := http.NewResponseController(w)
		enc := json.NewEncoder(w)
		// A write fails only once the caller left, which ends the
		// request's context and so the subscription.
		for ev := range sub.Events() {
			_ = enc.Encode(ev)
			_ = flush.Flush()
		}
	})
	return mux
}

// decodeBody decodes r's body into v, or answers that it is invalid.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		reply(w, nil, fmt.Errorf("%w: %w", session.ErrInvalid, err))
		return false
	}
	return true
}

// reply answers v as JSON, or err with its kind's status.
func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err == nil {
		_ = json.NewEncoder(w).Encode(v)
		return
	}
	status, kind := http.StatusInternalServerError, ""
	for _, k := range wireKinds {
		if errors.Is(err, k.err) {
			status, kind = k.status, k.kind
			break
		}
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(wireError{Kind: kind, Message: err.Error()})
}

// remote is a Ref over the object protocol.
type remote struct{ c *http.Client }

// Remote is a Ref over the object protocol: c's Transport delivers each
// request to the one object it reaches. A platform's Platform.Object
// returns one per object.
func Remote(c *http.Client) Ref { return remote{c: c} }

// Create implements Ref.
func (r remote) Create(ctx context.Context, init Init) error {
	return r.call(ctx, http.MethodPost, "/create", init, nil)
}

// Wake implements Ref.
func (r remote) Wake(ctx context.Context, w session.Wake) error {
	return r.call(ctx, http.MethodPost, "/wake", w, nil)
}

// Status implements Ref.
func (r remote) Status(ctx context.Context) (session.Status, error) {
	var st session.Status
	if err := r.call(ctx, http.MethodGet, "/status", nil, &st); err != nil {
		return session.Status{State: session.Unknown}, err
	}
	return st, nil
}

// Jobs implements Ref.
func (r remote) Jobs(ctx context.Context) ([]session.ID, error) {
	var jobs []session.ID
	err := r.call(ctx, http.MethodGet, "/jobs", nil, &jobs)
	return jobs, err
}

// Head implements Ref.
func (r remote) Head(ctx context.Context) (session.Seq, error) {
	var head session.Seq
	err := r.call(ctx, http.MethodGet, "/head", nil, &head)
	return head, err
}

// Subscribe implements Ref: the object's events after after, as they
// come.
func (r remote) Subscribe(ctx context.Context, after session.Seq) (session.Subscription, error) {
	ctx, cancel := context.WithCancel(ctx)
	resp, err := r.send(ctx, http.MethodGet, "/subscribe", "after="+strconv.FormatUint(uint64(after), 10), nil) //nolint:bodyclose // the subscription's reader closes it

	if err != nil {
		cancel()
		return nil, err
	}
	sub := &remoteSubscription{ch: make(chan session.Event), cancel: cancel, done: make(chan struct{})}
	go sub.read(ctx, resp.Body)
	return sub, nil
}

// call sends one request and decodes its answer into out, when out is set.
func (r remote) call(ctx context.Context, method, path string, in, out any) error {
	resp, err := r.send(ctx, method, path, "", in)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("durableobject: %s answer: %w", path, err)
	}
	return nil
}

// send sends one request and returns its response once it is a success;
// an error response is its kind's sentinel. Every request goes to the one
// host "object": the client's Transport picks the object.
func (r remote) send(ctx context.Context, method, path, query string, in any) (*http.Response, error) {
	req := (&http.Request{
		Method: method,
		URL:    &url.URL{Scheme: "http", Host: "object", Path: path, RawQuery: query},
		Header: http.Header{},
	}).WithContext(ctx)
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		req.Body, req.ContentLength = io.NopCloser(bytes.NewReader(raw)), int64(len(raw))
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.c.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	defer func() { _ = resp.Body.Close() }()
	var we wireError
	if err := json.NewDecoder(resp.Body).Decode(&we); err != nil {
		return nil, fmt.Errorf("durableobject: %s: %s", path, resp.Status)
	}
	for _, k := range wireKinds {
		if k.kind == we.Kind {
			return nil, fmt.Errorf("%w: %s", k.err, we.Message)
		}
	}
	return nil, errors.New(we.Message)
}

// remoteSubscription is a Subscription over an NDJSON stream.
type remoteSubscription struct {
	ch     chan session.Event
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (s *remoteSubscription) read(ctx context.Context, body io.ReadCloser) {
	defer close(s.done)
	defer close(s.ch)
	defer func() { _ = body.Close() }()
	sc := bufio.NewScanner(body)
	sc.Buffer(nil, 64<<20)
	for sc.Scan() {
		var ev session.Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			s.err = fmt.Errorf("durableobject: event: %w", err)
			return
		}
		select {
		case s.ch <- ev:
		case <-ctx.Done():
			return
		}
	}
	if ctx.Err() == nil && sc.Err() != nil {
		s.err = fmt.Errorf("durableobject: events: %w", sc.Err())
	}
}

// Events implements session.Subscription.
func (s *remoteSubscription) Events() <-chan session.Event { return s.ch }

// Err implements session.Subscription: nil when the subscriber closed it
// or the object ended the stream, else why the stream broke.
func (s *remoteSubscription) Err() error {
	<-s.done
	return s.err
}

// Close implements session.Subscription.
func (s *remoteSubscription) Close() error {
	s.cancel()
	<-s.done
	return nil
}
