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

// ProviderGoogleDrive is the bind kind for Drive.
// ProviderMicrosoft is the bind kind for OneDrive and SharePoint libraries.
const (
	ProviderGoogleDrive = "gdrive"
	ProviderMicrosoft   = "msgraph"
	ParamFolderID       = "folderId"
	ParamName           = "name"
	ParamDriveID        = "driveId"
	ParamItemID         = "itemId"
	ParamSiteID         = "siteId"
	// ParamAccount selects a Microsoft account kind on a bind ("organization" or "personal").
	ParamAccount = "account"
)

// Microsoft account kinds for Graph / ParamAccount.
// Organization (SharePoint / OneDrive for Business) is the default.
const (
	AccountOrganization = "organization"
	AccountPersonal     = "personal"
)

// Credential is a session-scoped access token. Never store this on MountSpec
// or in a checkpoint / SnapshotStore. Work-item payloads (Prompt/Resume) may
// carry it; backends must not persist it with recipes.
type Credential struct {
	Token     string    `json:"token,omitempty"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

// Binding is one user-owned cloud folder under /workspace/<alias>.
// Alias is params["name"] or a leftover single-segment Point (not /workspace).
// Provider is the bind kind (gdrive, msgraph). Writable is opt-in; the Go
// zero value stays read-only.
type Binding struct {
	Provider string            `json:"provider"`
	Point    string            `json:"point,omitempty"`
	Auth     Credential        `json:"auth,omitempty"`
	Params   map[string]string `json:"params,omitempty"`
	Writable bool              `json:"writable,omitempty"`
	// Live is the process token bag for this bind (not serialized). OpenVFS
	// copies it so 401 refresh shares the holder.
	Live *TokenHolder `json:"-"`
}

// ValidateBinding checks provider, alias, and access token. Optional backend
// params (gdrive folderId, Graph site/drive) are applied when the factory
// opens; omitted gdrive folderId is My Drive. Point may be empty when
// params["name"] is set; leftover "/contracts" becomes alias contracts.
func ValidateBinding(b Binding) error {
	if strings.TrimSpace(b.Provider) == "" {
		return fmt.Errorf("vfs: provider required")
	}
	if _, err := resolveBindingAlias(b); err != nil {
		return err
	}
	if strings.TrimSpace(b.Auth.Token) == "" {
		return fmt.Errorf("vfs: access token required")
	}
	return nil
}

func resolveBindingAlias(b Binding) (string, error) {
	name := strings.TrimSpace(b.Params[ParamName])
	if name != "" {
		if err := validAlias(name); err != nil {
			return "", err
		}
		return name, nil
	}
	point := strings.TrimSpace(b.Point)
	if point == "" || point == WorkspacePoint {
		return "", fmt.Errorf("%w: name required", ErrInvalidPath)
	}
	if err := ValidMountPoint(point); err != nil {
		return "", err
	}
	alias := strings.TrimPrefix(path.Clean(point), "/")
	if err := validAlias(alias); err != nil {
		return "", err
	}
	return alias, nil
}

func bindingAlias(b Binding) string {
	return strings.TrimSpace(b.Params[ParamName])
}

func unbindAlias(point string) (string, error) {
	point = strings.TrimSpace(point)
	if point == "" {
		return "", ErrInvalidPath
	}
	if !strings.HasPrefix(point, "/") {
		if err := validAlias(point); err != nil {
			return "", err
		}
		return point, nil
	}
	if err := ValidMountPoint(point); err != nil {
		return "", err
	}
	alias := strings.TrimPrefix(path.Clean(point), "/")
	if err := validAlias(alias); err != nil {
		return "", err
	}
	return alias, nil
}

// TokenRefreshFunc fetches a new access token (IdP or host callback).
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

// Bind records a folder mount and its access token. Cloud binds attach under
// /workspace/<alias>. A second bind to the same alias replaces it. A new token
// for the same provider updates the shared holder.
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
	alias, err := resolveBindingAlias(b)
	if err != nil {
		return err
	}
	params := maps.Clone(b.Params)
	if params == nil {
		params = make(map[string]string, 1)
	}
	params[ParamName] = alias
	b.Point = WorkspacePoint
	b.Params = params
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
		if bindingAlias(existing) == alias {
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

// Unbind removes the binding for alias. point may be leftover "/contracts" or
// the alias itself. The provider token is dropped when no binding remains for
// that provider. Unbind("/workspace") is invalid — pass the alias.
func (s *SessionAuth) Unbind(sessionID, point string) error {
	if s == nil {
		return fmt.Errorf("vfs: session auth required")
	}
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalidPath)
	}
	alias, err := unbindAlias(point)
	if err != nil {
		return err
	}
	return s.drop(sessionID, alias, "")
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
		match := (point != "" && bindingAlias(b) == point) || (provider != "" && b.Provider == provider)
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
