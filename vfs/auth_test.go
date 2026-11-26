package vfs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

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
		wg.Go(func() {
			errs <- holder.EnsureValid(t.Context())
		})
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

// TestRequest_holderRefreshesThroughTheRequestsRefresh: a request's holder
// gets a new token of its binding from Refresh; without Refresh it keeps
// the binding's token and cannot refresh.
func TestRequest_holderRefreshesThroughTheRequestsRefresh(t *testing.T) {
	t.Parallel()
	binding := mount.Binding{Provider: mount.ProviderGoogleDrive, Auth: mount.Credential{Token: "stale"}}
	var asked mount.Binding
	refreshing := vfs.Request{Refresh: func(_ context.Context, b mount.Binding) (mount.Credential, error) {
		asked = b
		return mount.Credential{Token: "fresh"}, nil
	}}.Holder(binding)
	fixed := vfs.Request{}.Holder(binding)

	refreshErr := refreshing.RefreshOnce(t.Context())
	fixedErr := fixed.RefreshOnce(t.Context())

	if refreshErr != nil || refreshing.Current().Token != "fresh" || asked.Provider != mount.ProviderGoogleDrive {
		t.Fatalf("refresh = %v, token %q, asked for %+v; want fresh for the binding", refreshErr, refreshing.Current().Token, asked)
	}
	if !errors.Is(fixedErr, vfs.ErrAuthExpired) || fixed.Current().Token != "stale" {
		t.Fatalf("refresh without Refresh = %v, token %q; want ErrAuthExpired and the binding's token", fixedErr, fixed.Current().Token)
	}
}
