package vfs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/internal/rest"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// Microsoft account kinds for Graph / ParamAccount.
// Organization (SharePoint / OneDrive for Business) is the default.
const (
	AccountOrganization = "organization"
	AccountPersonal     = "personal"
)

// TokenRefreshFunc fetches a new access token (IdP or host callback).
type TokenRefreshFunc func(ctx context.Context) (mount.Credential, error)

// TokenHolder is the live access token for one (session, provider).
// All folder mounts for that pair share the same holder. Refresh updates every mount.
type TokenHolder struct {
	mu          sync.Mutex
	cred        mount.Credential
	refresh     TokenRefreshFunc
	refreshing  bool
	refreshDone chan struct{}
	refreshErr  error
}

// DefaultTokenRefreshSkew refreshes a short-lived token before its hard expiry.
const DefaultTokenRefreshSkew = time.Minute

// NewTokenHolder returns a holder with the initial credential.
func NewTokenHolder(c mount.Credential) *TokenHolder {
	return &TokenHolder{cred: c}
}

// Set replaces the current credential.
func (h *TokenHolder) Set(c mount.Credential) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.cred = c
	h.mu.Unlock()
}

// Current returns a copy of the credential. Token is present; callers must not log it.
func (h *TokenHolder) Current() mount.Credential {
	if h == nil {
		return mount.Credential{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cred
}

// SetRefresh installs the optional client token callback used after a 401.
func (h *TokenHolder) SetRefresh(fn TokenRefreshFunc) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.refresh = fn
	h.mu.Unlock()
}

// RefreshOnce calls the refresh func once and stores the new token.
// Returns ErrAuthExpired when no refresh func is set or the client fails.
func (h *TokenHolder) RefreshOnce(ctx context.Context) error {
	return h.refreshCurrent(ctx, "")
}

// EnsureValid proactively refreshes a token that is expired or near expiry.
// A zero ExpiresAt selects reactive refresh after a provider 401.
func (h *TokenHolder) EnsureValid(ctx context.Context) error {
	if h == nil {
		return ErrAuthExpired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	cred := h.cred
	h.mu.Unlock()
	if strings.TrimSpace(cred.Token) == "" {
		return ErrAuthExpired
	}
	if cred.ExpiresAt.IsZero() || time.Until(cred.ExpiresAt) > DefaultTokenRefreshSkew {
		return nil
	}
	return h.refreshCurrent(ctx, cred.Token)
}

// RefreshIfCurrent refreshes only when staleToken is still installed. Parallel
// callers that observed the same rejected token wait for one shared refresh.
func (h *TokenHolder) RefreshIfCurrent(ctx context.Context, staleToken string) error {
	return h.refreshCurrent(ctx, staleToken)
}

func (h *TokenHolder) refreshCurrent(ctx context.Context, staleToken string) error {
	if h == nil {
		return ErrAuthExpired
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	if staleToken != "" && h.cred.Token != staleToken {
		h.mu.Unlock()
		return nil
	}
	if h.refreshing {
		done := h.refreshDone
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			h.mu.Lock()
			err := h.refreshErr
			h.mu.Unlock()
			return err
		}
	}
	fn := h.refresh
	if fn == nil {
		h.mu.Unlock()
		return ErrAuthExpired
	}
	h.refreshing = true
	h.refreshDone = make(chan struct{})
	done := h.refreshDone
	h.mu.Unlock()

	cred, err := fn(ctx)
	if err != nil {
		if !errors.Is(err, ErrAuthExpired) {
			err = fmt.Errorf("%w: %w", ErrAuthExpired, err)
		}
	} else if strings.TrimSpace(cred.Token) == "" || (!cred.ExpiresAt.IsZero() && !cred.ExpiresAt.After(time.Now())) {
		err = ErrAuthExpired
	}

	h.mu.Lock()
	if err == nil {
		h.cred = cred
	}
	h.refreshErr = err
	h.refreshing = false
	close(done)
	h.mu.Unlock()
	return err
}

// apiClient is a REST API client (Google, Graph) at base (def when base is
// empty) that sends holder's current token.
func apiClient(holder *TokenHolder, base, def string, httpClient *http.Client) rest.Client {
	if holder == nil {
		panic("vfs: an API client requires a TokenHolder")
	}
	return rest.Client{
		Base:  cmp.Or(base, def),
		HTTP:  cmp.Or(httpClient, http.DefaultClient),
		Token: func() string { return holder.Current().Token },
	}
}
