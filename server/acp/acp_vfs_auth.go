package acp

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/server"
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

func (p *acpProtocol) ownedVFSSession(ctx context.Context, env server.ProtocolEnv, sessionID string) (*acpWireSession, error) {
	if sessionID == "" {
		return nil, server.Errorf(server.ErrInvalidRequest, "sessionId is required")
	}
	return p.resolveOwnedWireSession(ctx, env, sessionID, actionVFSCredentials)
}

// handleVFSBind parses the ACP bind payload. The credential bag is session.CredentialBag.
func (p *acpProtocol) handleVFSBind(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (any, error) {
	var params vfsBindParams
	_ = json.Unmarshal(pr.Params, &params)
	sess, err := p.ownedVFSSession(ctx, env, params.SessionID)
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
		if !env.Workspace {
			errs = append(errs, itemErr{Point: item.Point, Error: "unknown vfs profile " + b.Provider})
			continue
		}
		// Bind validates b.
		if err := sess.creds.Bind(b); err != nil {
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

func (p *acpProtocol) handleVFSRefresh(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (any, error) {
	var params vfsRefreshParams
	_ = json.Unmarshal(pr.Params, &params)
	if params.Provider == "" {
		return nil, server.Errorf(server.ErrInvalidRequest, "sessionId and provider are required")
	}
	sess, err := p.ownedVFSSession(ctx, env, params.SessionID)
	if err != nil {
		return nil, err
	}
	if !sess.creds.Refresh(params.Provider, params.Auth.credential()) {
		return nil, server.Errorf(server.ErrInvalidRequest, "no vfs binding for provider")
	}
	return map[string]any{}, nil
}

func (p *acpProtocol) handleVFSUnbind(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (any, error) {
	var params vfsUnbindParams
	_ = json.Unmarshal(pr.Params, &params)
	sess, err := p.ownedVFSSession(ctx, env, params.SessionID)
	if err != nil {
		return nil, err
	}
	point := params.Point
	if name := strings.TrimSpace(params.Name); name != "" {
		point = name
	}
	sess.creds.Unbind(point, params.Provider)
	return map[string]any{}, nil
}
