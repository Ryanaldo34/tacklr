package vfs

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

// ProviderGoogleDrive is the factory / bind id for Drive.
const (
	ProviderGoogleDrive = "gdrive"
	ParamFolderID       = "folderId"
)

// Credential is a session-scoped access token. Never store this on MountSpec
// or in a checkpoint / wire envelope.
type Credential struct {
	Token     string
	ExpiresAt time.Time
}

// Binding attaches one user-owned backend folder to a virtual mount point.
// Provider is the BackendRegistry profile id (and the SessionAuth key).
type Binding struct {
	Provider string
	Point    string
	Auth     Credential
	Params   map[string]string // non-secret (folderId, …)
}

// BindingSpec is the secret-free MountSpec for a binding. Always read-only.
func BindingSpec(b Binding) MountSpec {
	return MountSpec{
		Point:    b.Point,
		Profile:  b.Provider,
		ReadOnly: true,
		Params:   maps.Clone(b.Params),
	}
}

// ValidMountPoint reports whether point is a single-segment virtual path
// (/contracts). FUSE and client binds require this shape.
func ValidMountPoint(point string) error {
	if point == "" || !path.IsAbs(point) || strings.ContainsAny(point, "\\\x00") {
		return ErrInvalidPath
	}
	cleaned := path.Clean(point)
	if cleaned == "/" || strings.Count(cleaned, "/") != 1 {
		return fmt.Errorf("%w: mount point must be one segment", ErrInvalidPath)
	}
	return nil
}

// ValidateBinding checks provider, point, and access token. folderId and other
// backend params are validated when the factory opens.
func ValidateBinding(b Binding) error {
	if strings.TrimSpace(b.Provider) == "" {
		return fmt.Errorf("vfs: provider required")
	}
	if err := ValidMountPoint(b.Point); err != nil {
		return err
	}
	if strings.TrimSpace(b.Auth.Token) == "" {
		return fmt.Errorf("vfs: access token required")
	}
	return nil
}

// TokenRefreshFunc fetches a new access token from the client (ACP _tacklr/vfs/token).
type TokenRefreshFunc func(ctx context.Context) (Credential, error)

// TokenHolder is the live access token for one (session, provider).
// All folder mounts for that pair share the same holder. Refresh updates every mount.
type TokenHolder struct {
	mu          sync.Mutex
	cred        Credential
	refresh     TokenRefreshFunc
	refreshing  bool
	refreshDone chan struct{}
	refreshErr  error
}

// DefaultTokenRefreshSkew refreshes a short-lived token before its hard expiry.
const DefaultTokenRefreshSkew = time.Minute

// NewTokenHolder returns a holder with the initial credential.
func NewTokenHolder(c Credential) *TokenHolder {
	return &TokenHolder{cred: c}
}

// Set replaces the current credential.
func (h *TokenHolder) Set(c Credential) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.cred = c
	h.mu.Unlock()
}

// Current returns a copy of the credential. Token is present; callers must not log it.
func (h *TokenHolder) Current() Credential {
	if h == nil {
		return Credential{}
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
// A zero ExpiresAt preserves compatibility with clients that only support
// reactive refresh after a provider 401.
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

// Token implements oauth2.TokenSource. Expiry is left zero so the SDK always
// sends the current holder token; 401 handling lives in the Drive adapter.
func (h *TokenHolder) Token() (*oauth2.Token, error) {
	if h == nil {
		return nil, ErrAuthExpired
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cred.Token == "" {
		return nil, ErrAuthExpired
	}
	return &oauth2.Token{
		AccessToken: h.cred.Token,
		TokenType:   "Bearer",
		Expiry:      h.cred.ExpiresAt,
	}, nil
}

type sessionBindings struct {
	holders map[string]*TokenHolder // provider → holder
	list    []Binding
}

// SessionAuth is the in-memory store of user-owned backend credentials.
// Tokens live on TokenHolder only; Binding.Auth is not the live copy.
type SessionAuth struct {
	mu   sync.Mutex
	byID map[string]*sessionBindings
}

// NewSessionAuth returns an empty store.
func NewSessionAuth() *SessionAuth {
	return &SessionAuth{byID: make(map[string]*sessionBindings)}
}

func (s *SessionAuth) ensure(sessionID string) *sessionBindings {
	sess, ok := s.byID[sessionID]
	if !ok {
		sess = &sessionBindings{holders: make(map[string]*TokenHolder)}
		s.byID[sessionID] = sess
	}
	return sess
}

// Bind records a folder mount and its access token. A second bind to the same
// point replaces it. A new token for the same provider updates the shared holder.
func (s *SessionAuth) Bind(sessionID string, b Binding) error {
	if s == nil {
		return fmt.Errorf("vfs: session auth required")
	}
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalidPath)
	}
	if err := ValidateBinding(b); err != nil {
		return err
	}
	b.Point = path.Clean(b.Point)
	b.Params = maps.Clone(b.Params)
	token := b.Auth
	b.Auth = Credential{} // live token lives on the holder only

	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.ensure(sessionID)
	if holder := sess.holders[b.Provider]; holder != nil {
		holder.Set(token)
	} else {
		sess.holders[b.Provider] = NewTokenHolder(token)
	}
	for i, existing := range sess.list {
		if existing.Point == b.Point {
			sess.list[i] = b
			return nil
		}
	}
	sess.list = append(sess.list, b)
	return nil
}

// Refresh replaces the access token for provider on the session.
func (s *SessionAuth) Refresh(sessionID, provider string, c Credential) error {
	if s == nil {
		return fmt.Errorf("vfs: session auth required")
	}
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(provider) == "" {
		return fmt.Errorf("vfs: session id and provider required")
	}
	if strings.TrimSpace(c.Token) == "" {
		return fmt.Errorf("vfs: access token required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byID[sessionID]
	if !ok {
		return fmt.Errorf("vfs: no bindings for session")
	}
	holder := sess.holders[provider]
	if holder == nil {
		return fmt.Errorf("vfs: no bindings for provider %q", provider)
	}
	holder.Set(c)
	return nil
}

// Unbind removes the binding at point. The provider token is dropped when no
// binding remains for that provider.
func (s *SessionAuth) Unbind(sessionID, point string) error {
	if s == nil {
		return fmt.Errorf("vfs: session auth required")
	}
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalidPath)
	}
	if err := ValidMountPoint(point); err != nil {
		return err
	}
	return s.drop(sessionID, path.Clean(point), "")
}

// UnbindProvider removes every binding for provider on the session.
func (s *SessionAuth) UnbindProvider(sessionID, provider string) error {
	if s == nil {
		return fmt.Errorf("vfs: session auth required")
	}
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(provider) == "" {
		return fmt.Errorf("vfs: session id and provider required")
	}
	return s.drop(sessionID, "", provider)
}

func (s *SessionAuth) drop(sessionID, point, provider string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byID[sessionID]
	if !ok {
		return ErrNotMounted
	}
	kept := sess.list[:0]
	removed := map[string]struct{}{}
	for _, b := range sess.list {
		match := (point != "" && b.Point == point) || (provider != "" && b.Provider == provider)
		if match {
			removed[b.Provider] = struct{}{}
			continue
		}
		kept = append(kept, b)
	}
	if len(removed) == 0 {
		return ErrNotMounted
	}
	sess.list = kept
	still := map[string]struct{}{}
	for _, b := range sess.list {
		still[b.Provider] = struct{}{}
	}
	for p := range removed {
		if _, ok := still[p]; !ok {
			delete(sess.holders, p)
		}
	}
	if len(sess.list) == 0 {
		delete(s.byID, sessionID)
	}
	return nil
}

// Clear drops every binding and token for the session.
func (s *SessionAuth) Clear(sessionID string) {
	if s == nil || sessionID == "" {
		return
	}
	s.mu.Lock()
	delete(s.byID, sessionID)
	s.mu.Unlock()
}

// Credential returns the current credential for provider, or false.
func (s *SessionAuth) Credential(sessionID, provider string) (Credential, bool) {
	h := s.Holder(sessionID, provider)
	if h == nil {
		return Credential{}, false
	}
	c := h.Current()
	return c, c.Token != ""
}

// Holder returns the shared token holder, or nil.
func (s *SessionAuth) Holder(sessionID, provider string) *TokenHolder {
	if s == nil || sessionID == "" || provider == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byID[sessionID]
	if !ok {
		return nil
	}
	return sess.holders[provider]
}

// Bindings returns a copy of the session bindings (no tokens).
func (s *SessionAuth) Bindings(sessionID string) []Binding {
	if s == nil || sessionID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byID[sessionID]
	if !ok {
		return nil
	}
	out := make([]Binding, len(sess.list))
	for i, b := range sess.list {
		b.Params = maps.Clone(b.Params)
		out[i] = b
	}
	return out
}

// HasBindings reports whether the session has at least one user-owned binding.
func (s *SessionAuth) HasBindings(sessionID string) bool {
	if s == nil || sessionID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byID[sessionID]
	return ok && len(sess.list) > 0
}
