package acp

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
)

type vfsAuthWire struct {
	Token     string     `json:"token"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

func (a vfsAuthWire) credential() mount.Credential {
	credential := mount.Credential{Token: a.Token}
	if a.ExpiresAt != nil {
		credential.ExpiresAt = a.ExpiresAt.UTC()
	}
	return credential
}

type vfsBindItem struct {
	Provider string            `json:"provider"`
	Profile  string            `json:"profile"`
	Point    string            `json:"point"`
	Auth     vfsAuthWire       `json:"auth"`
	Params   map[string]string `json:"params"`
	ReadOnly *bool             `json:"readOnly"`
}

type vfsBindParams struct {
	SessionID string        `json:"sessionId"`
	Backends  []vfsBindItem `json:"backends"`
}

type vfsRefreshParams struct {
	SessionID string      `json:"sessionId"`
	Provider  string      `json:"provider"`
	Auth      vfsAuthWire `json:"auth"`
}

type vfsUnbindParams struct {
	SessionID string `json:"sessionId"`
	Point     string `json:"point"`
	Provider  string `json:"provider"`
	Name      string `json:"name"`
}

func (item vfsBindItem) binding() mount.Binding {
	provider := item.Provider
	if provider == "" {
		provider = item.Profile
	}
	writable := item.ReadOnly != nil && !*item.ReadOnly
	return mount.Binding{
		Provider: provider,
		Point:    item.Point,
		Auth:     item.Auth.credential(),
		Params:   item.Params,
		Writable: writable,
	}
}

// vfsBag is the credentials of a session the connection's principal owns,
// for its next turn.
func (c *conn) vfsBag(ctx context.Context, sessionID string) (*server.CredentialBag, error) {
	if sessionID == "" {
		return nil, server.Errorf(server.ErrInvalidRequest, "sessionId is required")
	}
	if _, err := c.ownedSession(ctx, sessionID, actionVFSCredentials); err != nil {
		return nil, err
	}
	return &c.nextOf(session.ID(sessionID)).creds, nil
}

// handleVFSBind parses the ACP bind payload into the connection's
// credentials for the session's next turn.
func (c *conn) handleVFSBind(ctx context.Context, pr *parsedRequest) (any, error) {
	var params vfsBindParams
	_ = json.Unmarshal(pr.Params, &params)
	bag, err := c.vfsBag(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	if len(params.Backends) == 0 {
		return nil, server.Errorf(server.ErrInvalidRequest, "backends is required")
	}
	type mounted struct {
		Point    string `json:"point"`
		Provider string `json:"provider"`
	}
	type itemErr struct {
		Point string `json:"point"`
		Error string `json:"error"`
	}
	var okItems []mounted
	var errs []itemErr
	for _, item := range params.Backends {
		b := item.binding()
		if !c.env.Workspace {
			errs = append(errs, itemErr{Point: item.Point, Error: "unknown vfs profile " + b.Provider})
			continue
		}
		// Bind validates b.
		if err := bag.Bind(b); err != nil {
			errs = append(errs, itemErr{Point: item.Point, Error: err.Error()})
			continue
		}
		okItems = append(okItems, mounted{Point: mount.WorkspacePoint, Provider: b.Provider})
	}
	return map[string]any{
		"mounted": okItems,
		"errors":  errs,
	}, nil
}

func (c *conn) handleVFSRefresh(ctx context.Context, pr *parsedRequest) (any, error) {
	var params vfsRefreshParams
	_ = json.Unmarshal(pr.Params, &params)
	if params.Provider == "" {
		return nil, server.Errorf(server.ErrInvalidRequest, "sessionId and provider are required")
	}
	bag, err := c.vfsBag(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	if !bag.Refresh(params.Provider, params.Auth.credential()) {
		return nil, server.Errorf(server.ErrInvalidRequest, "no vfs binding for provider")
	}
	return map[string]any{}, nil
}

func (c *conn) handleVFSUnbind(ctx context.Context, pr *parsedRequest) (any, error) {
	var params vfsUnbindParams
	_ = json.Unmarshal(pr.Params, &params)
	bag, err := c.vfsBag(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	point := params.Point
	if name := strings.TrimSpace(params.Name); name != "" {
		point = name
	}
	bag.Unbind(point, params.Provider)
	return map[string]any{}, nil
}
