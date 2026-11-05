package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

// A session binds two points of one provider with one shared token holder.
// Bindings never show the token, a refresh rotates the shared holder, and
// the token stays until the last point is unbound.
func TestSessionAuth_bindRefreshUnbind(t *testing.T) {
	t.Parallel()
	auth := vfs.NewSessionAuth()
	tok := "secret-access-token-xyz"
	if err := auth.Bind("sess-1", mount.Binding{
		Provider: mount.ProviderGoogleDrive,
		Point:    "/contracts",
		Auth:     mount.Credential{Token: tok},
		Params:   map[string]string{mount.ParamFolderID: "fld-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("sess-1", mount.Binding{
		Provider: mount.ProviderGoogleDrive,
		Point:    "/notes",
		Auth:     mount.Credential{Token: tok},
		Params:   map[string]string{mount.ParamFolderID: "fld-b"},
	}); err != nil {
		t.Fatal(err)
	}

	got := auth.Bindings("sess-1")
	if len(got) != 2 {
		t.Fatalf("bindings = %d", len(got))
	}
	aliases := map[string]bool{}
	for _, b := range got {
		if b.Auth.Token != "" {
			t.Fatalf("Bindings leaked token: %+v", b)
		}
		if b.Provider != mount.ProviderGoogleDrive {
			t.Fatalf("provider = %q", b.Provider)
		}
		if b.Point != mount.WorkspacePoint {
			t.Fatalf("point = %q", b.Point)
		}
		aliases[b.Params[mount.ParamName]] = true
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), tok) {
			t.Fatalf("Binding JSON contained token: %s", raw)
		}
	}
	if !aliases["contracts"] || !aliases["notes"] {
		t.Fatalf("aliases = %v", aliases)
	}

	c, ok := auth.Credential("sess-1", mount.ProviderGoogleDrive)
	if !ok || c.Token != tok {
		t.Fatalf("Credential = %+v ok=%v", c, ok)
	}
	h1 := auth.Holder("sess-1", mount.ProviderGoogleDrive)
	if h1 == nil {
		t.Fatal("missing holder")
	}

	next := mount.Credential{Token: "rotated-token"}
	if err := auth.Refresh("sess-1", mount.ProviderGoogleDrive, next); err != nil {
		t.Fatal(err)
	}
	if auth.Holder("sess-1", mount.ProviderGoogleDrive) != h1 {
		t.Fatal("refresh must keep the shared holder")
	}
	if got := h1.Current().Token; got != "rotated-token" {
		t.Fatalf("holder token = %q", got)
	}

	if err := auth.Unbind("sess-1", "/contracts"); err != nil {
		t.Fatal(err)
	}
	if !auth.HasBindings("sess-1") {
		t.Fatal("notes should remain")
	}
	if _, ok := auth.Credential("sess-1", mount.ProviderGoogleDrive); !ok {
		t.Fatal("token should remain while notes is bound")
	}
	if err := auth.Unbind("sess-1", "/notes"); err != nil {
		t.Fatal(err)
	}
	if auth.HasBindings("sess-1") {
		t.Fatal("expected empty session")
	}
	if _, ok := auth.Credential("sess-1", mount.ProviderGoogleDrive); ok {
		t.Fatal("holder should be gone after last unbind")
	}

	if err := auth.Bind("sess-2", mount.Binding{
		Provider: "gdrive",
		Auth:     mount.Credential{Token: "t"},
		Params:   map[string]string{mount.ParamName: "legal"},
	}); err != nil {
		t.Fatal(err)
	}
	auth.Clear("sess-2")
	if auth.HasBindings("sess-2") {
		t.Fatal("Clear must drop bindings")
	}
}

// waitCtx is a context that reports when a caller first waits on Done, so a
// test can end it while that caller waits.
type waitCtx struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *waitCtx) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

// Callers that find a token near expiry share one refresh and all get the
// fresh token, also a caller that waits for the refresh another caller
// runs. A caller that waits stops when its own context ends.
func TestTokenHolder_proactiveRefreshCoalescesConcurrentCallers(t *testing.T) {
	t.Parallel()
	// Arrange
	holder := vfs.NewTokenHolder(mount.Credential{
		Token:     "expiring",
		ExpiresAt: time.Now().Add(10 * time.Second),
	})
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	holder.SetRefresh(func(context.Context) (mount.Credential, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return mount.Credential{
			Token:     "fresh",
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	})
	inner, cancel := context.WithCancel(t.Context())
	waiter := &waitCtx{Context: inner, waiting: make(chan struct{})}
	sharer := &waitCtx{Context: t.Context(), waiting: make(chan struct{})}

	// Act
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, readers+1)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- holder.EnsureValid(t.Context())
		}()
	}
	<-started
	waited := make(chan error, 1)
	go func() { waited <- holder.RefreshIfCurrent(waiter, "expiring") }()
	<-waiter.waiting
	cancel()
	waitErr := <-waited
	wg.Go(func() { errs <- holder.EnsureValid(sharer) })
	<-sharer.waiting
	close(release)
	wg.Wait()
	close(errs)

	// Assert
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("a waiter whose context ends = %v, want context.Canceled", waitErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d", got)
	}
	if got := holder.Current().Token; got != "fresh" {
		t.Fatalf("token = %q", got)
	}
}

// Bind refuses an empty session, token or provider and a point that is not
// one free alias.
func TestSessionAuth_bindRejects(t *testing.T) {
	t.Parallel()
	auth := vfs.NewSessionAuth()
	tok := mount.Credential{Token: "t"}
	cases := []struct {
		name string
		sid  string
		b    mount.Binding
		want error
	}{
		{"empty session", "", mount.Binding{Provider: "gdrive", Point: "/a", Auth: tok}, vfs.ErrInvalidPath},
		{"empty token", "s", mount.Binding{Provider: "gdrive", Point: "/a"}, mount.ErrInvalidBinding},
		{"empty provider", "s", mount.Binding{Point: "/a", Auth: tok}, mount.ErrInvalidBinding},
		{"multi segment", "s", mount.Binding{Provider: "gdrive", Point: "/a/b", Auth: tok}, mount.ErrInvalidBinding},
		{"relative point", "s", mount.Binding{Provider: "gdrive", Point: "contracts", Auth: tok}, mount.ErrInvalidBinding},
		{"workspace without name", "s", mount.Binding{Provider: "gdrive", Point: mount.WorkspacePoint, Auth: tok}, mount.ErrInvalidBinding},
		{"root point", "s", mount.Binding{Provider: "gdrive", Point: "/", Auth: tok}, mount.ErrInvalidBinding},
		{"backslash alias", "s", mount.Binding{Provider: "gdrive", Auth: tok, Params: map[string]string{mount.ParamName: `a\b`}}, mount.ErrInvalidBinding},
		{"reserved alias", "s", mount.Binding{Provider: "gdrive", Auth: tok, Params: map[string]string{mount.ParamName: "work"}}, mount.ErrInvalidBinding},
	}
	for _, tc := range cases {
		if err := auth.Bind(tc.sid, tc.b); !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v want %v", tc.name, err, tc.want)
		}
	}
}

// RefreshOnce installs the token the refresh returns and fails when there
// is no refresh, the refresh fails or returns no token, or the context ends.
// A refresh for a token that is no longer current keeps the current one.
// A nil holder has no token.
func TestTokenHolder_refreshOnce(t *testing.T) {
	t.Parallel()
	h := vfs.NewTokenHolder(mount.Credential{Token: "old"})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("no refresh func: %v", err)
	}
	n := 0
	h.SetRefresh(func(ctx context.Context) (mount.Credential, error) {
		n++
		return mount.Credential{Token: "new"}, nil
	})
	if err := h.RefreshOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.Current().Token != "new" || n != 1 {
		t.Fatalf("token=%q n=%d", h.Current().Token, n)
	}
	if err := h.RefreshIfCurrent(t.Context(), "old"); err != nil || h.Current().Token != "new" {
		t.Fatalf("refresh of a replaced token: token=%q err=%v", h.Current().Token, err)
	}

	h.SetRefresh(func(context.Context) (mount.Credential, error) {
		return mount.Credential{}, vfs.ErrAuthExpired
	})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("refresh expired: %v", err)
	}
	h.SetRefresh(func(context.Context) (mount.Credential, error) {
		return mount.Credential{}, errors.New("client down")
	})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("wrap client error: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.RefreshOnce(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled refresh: %v", err)
	}
	if err := h.EnsureValid(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled EnsureValid: %v", err)
	}
	h.SetRefresh(func(context.Context) (mount.Credential, error) {
		return mount.Credential{Token: "   "}, nil
	})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("empty token: %v", err)
	}

	var nilH *vfs.TokenHolder
	nilH.Set(mount.Credential{Token: "x"})
	nilH.SetRefresh(nil)
	if nilH.Current().Token != "" {
		t.Fatal("nil holder Current")
	}
	if err := nilH.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("nil holder refresh: %v", err)
	}
	if err := nilH.EnsureValid(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("nil holder EnsureValid: %v", err)
	}
}

// A second bind of one point replaces it and keeps the shared holder, and
// UnbindProvider drops every point of the provider. Refresh, Unbind and
// UnbindProvider fail for a session, provider or point that is not bound,
// and a nil SessionAuth has nothing.
func TestSessionAuth_replaceAndUnbindProvider(t *testing.T) {
	t.Parallel()
	auth := vfs.NewSessionAuth()
	first := mount.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: mount.Credential{Token: "t1"}, Params: map[string]string{mount.ParamFolderID: "a"},
	}
	if err := auth.Bind("s", first); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("s", mount.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: mount.Credential{Token: "t2"}, Params: map[string]string{mount.ParamFolderID: "b"},
	}); err != nil {
		t.Fatal(err)
	}
	got := auth.Bindings("s")
	if len(got) != 1 || got[0].Params[mount.ParamFolderID] != "b" {
		t.Fatalf("replace bind = %+v", got)
	}
	if tok, _ := auth.Credential("s", "gdrive"); tok.Token != "t2" {
		t.Fatalf("shared holder after replace = %q", tok.Token)
	}
	if err := auth.Refresh("s", "msgraph", mount.Credential{Token: "x"}); err == nil || !strings.Contains(err.Error(), "msgraph") {
		t.Fatalf("refresh of a provider the session has not bound: %v", err)
	}
	if err := auth.Unbind("s", "/nope"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("unbind of a point the session has not bound: %v", err)
	}
	if got := auth.Bindings("unknown"); got != nil {
		t.Fatalf("bindings of an unknown session = %+v", got)
	}
	if err := auth.Bind("s", mount.Binding{
		Provider: "gdrive", Point: "/notes", Auth: mount.Credential{Token: "t2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.UnbindProvider("s", "gdrive"); err != nil {
		t.Fatal(err)
	}
	if auth.HasBindings("s") {
		t.Fatal("UnbindProvider must drop every gdrive point")
	}
	if err := auth.UnbindProvider("s", "gdrive"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("second UnbindProvider: %v", err)
	}
	if err := auth.Refresh("s", "gdrive", mount.Credential{Token: "x"}); err == nil {
		t.Fatal("refresh after unbind")
	}
	if err := auth.Refresh("", "gdrive", mount.Credential{Token: "x"}); err == nil {
		t.Fatal("refresh empty session")
	}
	if err := auth.Refresh("s", "gdrive", mount.Credential{}); err == nil {
		t.Fatal("refresh empty token")
	}
	if err := auth.Unbind("", "/contracts"); err == nil {
		t.Fatal("unbind empty session")
	}
	if err := auth.Unbind("s", "/a/b"); !errors.Is(err, mount.ErrInvalidBinding) {
		t.Fatalf("unbind multi-segment: %v", err)
	}
	if err := auth.UnbindProvider("", "gdrive"); err == nil {
		t.Fatal("UnbindProvider empty session")
	}

	var nilA *vfs.SessionAuth
	if err := nilA.Bind("s", first); err == nil {
		t.Fatal("nil Bind")
	}
	if err := nilA.Refresh("s", "gdrive", mount.Credential{Token: "t"}); err == nil {
		t.Fatal("nil Refresh")
	}
	if err := nilA.Unbind("s", "/contracts"); err == nil {
		t.Fatal("nil Unbind")
	}
	if err := nilA.UnbindProvider("s", "gdrive"); err == nil {
		t.Fatal("nil UnbindProvider")
	}
	nilA.Clear("s")
	if nilA.HasBindings("s") || nilA.Bindings("s") != nil || nilA.Holder("s", "gdrive") != nil {
		t.Fatal("nil SessionAuth accessors")
	}
}
