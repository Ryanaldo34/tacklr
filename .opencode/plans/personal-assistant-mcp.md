# Personal Assistant Agent with Google Gmail & Drive MCP Servers

## Overview

Replace the current weather "Hello World" agent with a Personal Assistant agent that connects to Google's official hosted Gmail and Drive MCP servers using per-user (anonymous for now) OAuth tokens.

## MCP Server Endpoints

- Gmail: `https://gmailmcp.googleapis.com/mcp/v1` (streamable HTTP)
- Drive: `https://drivemcp.googleapis.com/mcp/v1` (streamable HTTP)

Both use OAuth 2.0 bearer tokens — the MCP client sends `Authorization: Bearer <access_token>`. This maps directly to the existing `MCPConfig.AuthToken` field. No changes to `pkg/harness/mcp/` needed.

## OAuth Flow (Anonymous, Local Dev)

1. Client sends `POST /` with `agent_id: "personal_assistant"` (no user_id)
2. `loadAgent` resolves Google token under constant `"local"` ID
3. **No token found** → server auto-opens browser via `exec.Command` to Google consent URL, emits `{"type":"auth_required"}` event
4. User consents on Google → browser redirects to `GET /auth/google/callback?code=xxx&state=local`
5. Server exchanges code for tokens, stores in DB under `"local"` user ID
6. Callback returns HTML: "Authentication successful. Please retry your request."
7. Client retries `POST /` → token now exists → agent runs with Gmail + Drive MCP tools
8. **Token exists but expired** → `oauth2.TokenSource` auto-refreshes using stored refresh token (no user interaction)

## Files to Create

### `internal/auth/google.go`

`GoogleAuth` struct wrapping `oauth2.Config`:

```go
type GoogleAuth struct {
    Config *oauth2.Config
}

func NewGoogleAuth(clientID, clientSecret, redirectURL string) *GoogleAuth

// AuthURL generates the Google OAuth consent URL with state="local"
func (a *GoogleAuth) AuthURL() string

// Exchange exchanges the authorization code for tokens
func (a *GoogleAuth) Exchange(ctx context.Context, code string) (*oauth2.Token, error)

// TokenSource wraps a stored token with auto-refresh
func (a *GoogleAuth) TokenSource(ctx context.Context, token *oauth2.Token) oauth2.TokenSource

// OpenBrowser opens the default browser to the given URL (cross-platform)
func OpenBrowser(url string) error
```

Scopes: `gmail.readonly`, `gmail.compose`, `drive.readonly`

Uses `golang.org/x/oauth2` (already in go.mod) with `google.Endpoint` for the OAuth endpoints.

### `internal/auth/google_test.go`

- Test AuthURL contains expected params
- Test TokenSource with valid (non-expired) token returns token as-is
- Test TokenSource with expired token + mock HTTP server simulating refresh
- Test Exchange with mock HTTP server simulating token endpoint
- Test OpenBrowser doesn't panic on unsupported OS (graceful)

### `internal/db/inmemory_tokens.go`

`InMemoryTokenStore` implementing both `TokenResolver` and `TokenWriter`:
- In-memory map keyed by userID
- Used when Supabase/Vault isn't available (local dev, tests)
- `ResolveGoogleToken` returns from map or nil
- `StoreGoogleToken` writes to map
- Other `TokenResolver` methods (Microsoft, Salesforce) return nil

### `internal/server/auth.go`

Two new endpoints:

**`GET /auth/google`** — 302 redirect to Google consent URL:
```go
func (s *Server) handleGoogleAuth(w http.ResponseWriter, r *http.Request)
```

**`GET /auth/google/callback`** — OAuth callback:
```go
func (s *Server) handleGoogleCallback(w http.ResponseWriter, r *http.Request)
```
1. Read `code` and `state` query params
2. Exchange code for tokens via `GoogleAuth.Exchange`
3. Store tokens via `TokenWriter.StoreGoogleToken(ctx, "local", accessToken, refreshToken, expiry)`
4. Return HTML success page: `<h1>Authentication successful</h1><p>You can now retry your request.</p>`

## Files to Modify

### `internal/config/config.go`

Add fields:
```go
GoogleOAuthClientID    string
GoogleOAuthClientSecret string
GoogleOAuthRedirectURL string
GmailMCPURL            string
DriveMCPURL            string
```

Add env loading:
```go
GoogleOAuthClientID:    env("GOOGLE_OAUTH_CLIENT_ID", ""),
GoogleOAuthClientSecret: env("GOOGLE_OAUTH_CLIENT_SECRET", ""),
GoogleOAuthRedirectURL: env("GOOGLE_OAUTH_REDIRECT_URL", "http://localhost:6969/auth/google/callback"),
GmailMCPURL:            env("GMAIL_MCP_URL", "https://gmailmcp.googleapis.com/mcp/v1"),
DriveMCPURL:            env("DRIVE_MCP_URL", "https://drivemcp.googleapis.com/mcp/v1"),
```

### `internal/db/tokenstore.go`

Add new `TokenWriter` interface:
```go
type TokenWriter interface {
    StoreGoogleToken(ctx context.Context, userID, accessToken, refreshToken string, expiresAt time.Time) error
}
```

Add `StoreGoogleToken` to `TokenStore`:
- Creates two Vault secrets via `SELECT vault.create_secret($1, $2)`
- Upserts into `user_google_token(user_id, access_token_secret_id, refresh_token_secret_id, token_expires_at)`

### `internal/server/errors.go`

Add new sentinel error:
```go
ErrAuthRequired = errors.New("authentication required")
```

Add `IsAuthRequired` helper:
```go
func IsAuthRequired(err error) bool {
    return errors.Is(err, ErrAuthRequired)
}
```

### `internal/server/server.go`

Update `Server` struct:
```go
type Server struct {
    provider      AgentProvider
    store         db.BaseStore
    tokenResolver db.TokenResolver
    tokenWriter   db.TokenWriter
    googleAuth    *auth.GoogleAuth
}
```

Update `New` signature:
```go
func New(store db.BaseStore, provider AgentProvider, tr db.TokenResolver, tw db.TokenWriter, ga *auth.GoogleAuth) *Server
```

Register new routes in `Handler()`:
```go
mux.HandleFunc("GET /auth/google", s.handleGoogleAuth)
mux.HandleFunc("GET /auth/google/callback", s.handleGoogleCallback)
```

### `internal/server/agent.go`

Update `loadAgent` to resolve/refresh Google tokens and inject into MCPConfigs:

```go
func (s *Server) loadAgent(ctx context.Context, agentID, threadID string, load bool) (*harness.AgentHarness, *AgentSpec, error) {
    spec, err := s.provider.GetAgent(ctx, agentID)
    // ... existing logic ...

    // Inject per-request auth tokens into MCPConfigs
    if len(spec.MCPConfigs) > 0 && s.googleAuth != nil && s.tokenResolver != nil {
        token, err := s.resolveGoogleToken(ctx)
        if err != nil {
            return nil, nil, err  // ErrAuthRequired or refresh failure
        }
        // Deep-copy MCPConfigs and set AuthToken
        configs := make([]mcp.MCPConfig, len(spec.MCPConfigs))
        copy(configs, spec.MCPConfigs)
        for i := range configs {
            configs[i].AuthToken = token
        }
        h.MCPConfigs = configs
    } else {
        h.MCPConfigs = spec.MCPConfigs
    }
    // ... rest of existing logic ...
}
```

New helper `resolveGoogleToken`:
```go
func (s *Server) resolveGoogleToken(ctx context.Context) (string, error) {
    const localUserID = "local"
    stored, err := s.tokenResolver.ResolveGoogleToken(ctx, localUserID)
    if err != nil {
        return "", fmt.Errorf("resolve google token: %w", err)
    }
    if stored == nil {
        // No token — initiate OAuth flow
        authURL := s.googleAuth.AuthURL()
        OpenBrowser(authURL)  // auto-open browser on localhost
        return "", clientErrorf(ErrAuthRequired, "google authentication required; browser opened to %s", authURL)
    }
    // Check if token needs refresh
    token := &oauth2.Token{
        AccessToken:  stored.AccessToken,
        RefreshToken:  stored.RefreshToken,
        Expiry:         stored.ExpiresAt,
    }
    if !token.Valid() {
        ts := s.googleAuth.TokenSource(ctx, token)
        refreshed, err := ts.Token()
        if err != nil {
            // Refresh failed — re-auth needed
            authURL := s.googleAuth.AuthURL()
            OpenBrowser(authURL)
            return "", clientErrorf(ErrAuthRequired, "google token refresh failed; re-authentication required; browser opened to %s", authURL)
        }
        // Optionally persist refreshed token (skip for V1 — in-memory only)
        return refreshed.AccessToken, nil
    }
    return stored.AccessToken, nil
}
```

### `internal/server/sse.go` and `internal/server/websocket.go`

When `loadAgent` returns `ErrAuthRequired`:
- SSE: emit `{"type":"auth_required","auth_url":"..."}` event (reuse `sseEvent` with `Content` = auth URL)
- WS: write `{"type":"auth_required","auth_url":"..."}` (via `streamer.WriteEvent` or `wsjson.Write`)

Since `ErrAuthRequired` is a `clientError`, `IsClientError` already returns true. The handlers will call `sseClientError`/`writeWSClientError` which emit a generic error. We need to intercept `ErrAuthRequired` specifically and emit an `auth_required` event instead of a generic `error` event.

In the handlers, after `loadAgent` returns error:
```go
if IsAuthRequired(err) {
    // Emit auth_required event instead of generic error
    authData, _ := json.Marshal(sseEvent{Type: "auth_required", Content: err.Error()})
    writeSSEEvent(w, flusher, "auth_required", authData)
    return
}
```

### `internal/server/sse_test.go` and `websocket_test.go`

Update `mockAgentProvider` and test server construction for new `Server` fields:
```go
srv := &Server{
    provider:      &mockAgentProvider{...},
    store:         store,
    tokenResolver: &inMemoryTokenStore{},
    googleAuth:    auth.NewGoogleAuth("test-client-id", "test-secret", "http://localhost:6969/auth/google/callback"),
}
```

### `cmd/server/main.go`

```go
// Build Google OAuth config
googleAuth := auth.NewGoogleAuth(
    cfg.GoogleOAuthClientID,
    cfg.GoogleOAuthClientSecret,
    cfg.GoogleOAuthRedirectURL,
)

// Token store: use TokenStore if Supabase available, InMemoryTokenStore otherwise
var tokenResolver db.TokenResolver
var tokenWriter db.TokenWriter
if conn != nil {
    ts := db.NewTokenStore(conn)
    tokenResolver = ts
    tokenWriter = ts
} else {
    ts := db.NewInMemoryTokenStore()
    tokenResolver = ts
    tokenWriter = ts
}

// Register default agent (weather)
registry.Register("default", server.AgentSpec{
    Config: harness.Config{
        MaxWindowSize: cfg.AgentMaxWindowSize,
        SystemPrompt:  defaultSystemPrompt,
    },
    Model:             strategy,
    Tools:             tools.Default(),
    WatchDog:          telemetry.New(),
    StreamingStrategy: streamer,
})

// Register personal_assistant agent (Gmail + Drive MCP)
if cfg.GoogleOAuthClientID != "" {
    registry.Register("personal_assistant", server.AgentSpec{
        Config: harness.Config{
            MaxWindowSize: cfg.AgentMaxWindowSize,
            SystemPrompt:  personalAssistantPrompt,
        },
        Model:    strategy,
        WatchDog: telemetry.New(),
        StreamingStrategy: streamer,
        MCPConfigs: []mcp.MCPConfig{
            {Name: "gmail", Transport: mcp.TransportStreamable, URL: cfg.GmailMCPURL, AuthRequired: true},
            {Name: "drive", Transport: mcp.TransportStreamable, URL: cfg.DriveMCPURL, AuthRequired: true},
        },
    })
}

srv := server.New(baseStore, registry, tokenResolver, tokenWriter, googleAuth)
```

Personal assistant system prompt:
```
You are a personal assistant with access to the user's Gmail and Google Drive
through MCP tools. You can search emails, read threads, create drafts, manage
labels, search files, read file content, and more. Always be helpful, concise,
and respect the user's privacy. When creating email drafts, always confirm the
content with the user before proceeding.
```

## What Stays Unchanged

- `pkg/harness/mcp/` — no changes needed (bearer token auth already supported)
- `pkg/harness/harness.go` — MCP discovery already wired via `initMCP`
- `pkg/harness/control/` — no changes
- Existing "default" weather agent stays registered alongside "personal_assistant"

## Dependencies

No new Go dependencies needed:
- `golang.org/x/oauth2` — already in go.mod
- `github.com/google/uuid` — already in go.mod
- Google's MCP servers are remote (no SDK dependency needed)

## Verification

```bash
go build ./...
go vet ./...
go test -race -count=1 ./...
```