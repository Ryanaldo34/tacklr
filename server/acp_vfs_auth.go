package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

type vfsAuthWire struct {
	Token     string     `json:"token"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

func (a vfsAuthWire) credential() vfs.Credential {
	credential := vfs.Credential{Token: a.Token}
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

func (p *acpProtocol) sessionAgent(ctx context.Context, sessionID string) string {
	if p == nil || sessionID == "" {
		return ""
	}
	sess, err := p.resolveWireSession(ctx, sessionID)
	if err != nil || sess == nil {
		return ""
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.configValues != nil {
		return sess.configValues["agent"]
	}
	return ""
}

func (item vfsBindItem) binding() vfs.Binding {
	provider := item.Provider
	if provider == "" {
		provider = item.Profile
	}
	writable := item.ReadOnly != nil && !*item.ReadOnly
	return vfs.Binding{
		Provider: provider,
		Point:    item.Point,
		Auth:     item.Auth.credential(),
		Params:   item.Params,
		Writable: writable,
	}
}

// handleVFSBind stashes credentials on the ACP wire session. They are copied
// onto Prompt/Resume AuthContext in BindTurn. This is protocol state, not kernel.
func (p *acpProtocol) handleVFSBind(ctx context.Context, env ProtocolEnv, pr *parsedRequest) error {
	var params vfsBindParams
	if err := json.Unmarshal(pr.Params, &params); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "invalid bind params: %v", err))
	}
	if params.SessionID == "" {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "sessionId is required"))
	}
	sess, err := p.resolveOwnedWireSession(ctx, env, params.SessionID, actionVFSCredentials)
	if err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	if len(params.Backends) == 0 {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "backends is required"))
	}
	agentID := p.sessionAgent(ctx, params.SessionID)
	if agentID == "" {
		agentID = catalogDefault(env.Catalog)
	}
	spec, _ := env.Catalog.Lookup(agentID)

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
		if err := vfs.ValidateBinding(b); err != nil {
			errs = append(errs, itemErr{Point: item.Point, Error: err.Error()})
			continue
		}
		if spec.FSRegistry != nil && !spec.FSRegistry.HasProfile(b.Provider) {
			errs = append(errs, itemErr{Point: item.Point, Error: "unknown vfs profile " + b.Provider})
			continue
		}
		sess.stashBind(b)
		okItems = append(okItems, mounted{Point: vfs.WorkspacePoint, Provider: b.Provider})
	}
	return env.Conn.Writer.WriteResult(pr.ID, map[string]any{
		"mounted": okItems,
		"errors":  errs,
	})
}

func (p *acpProtocol) handleVFSRefresh(ctx context.Context, env ProtocolEnv, pr *parsedRequest) error {
	var params vfsRefreshParams
	if err := json.Unmarshal(pr.Params, &params); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "invalid refresh params: %v", err))
	}
	if params.SessionID == "" || params.Provider == "" {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "sessionId and provider are required"))
	}
	sess, err := p.resolveOwnedWireSession(ctx, env, params.SessionID, actionVFSCredentials)
	if err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	if !sess.stashRefresh(params.Provider, params.Auth.credential()) {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "no vfs binding for provider"))
	}
	return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
}

func (p *acpProtocol) handleVFSUnbind(ctx context.Context, env ProtocolEnv, pr *parsedRequest) error {
	var params vfsUnbindParams
	if err := json.Unmarshal(pr.Params, &params); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "invalid unbind params: %v", err))
	}
	if params.SessionID == "" {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "sessionId is required"))
	}
	sess, err := p.resolveOwnedWireSession(ctx, env, params.SessionID, actionVFSCredentials)
	if err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	point := params.Point
	if name := strings.TrimSpace(params.Name); name != "" {
		point = name
	}
	sess.stashUnbind(point, params.Provider)
	return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
}
