package server

import (
	"context"
	"encoding/json"

	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	methodVFSBind    = "_tacklr/vfs/bind"
	methodVFSRefresh = "_tacklr/vfs/refresh"
	methodVFSUnbind  = "_tacklr/vfs/unbind"
	methodVFSToken   = "_tacklr/vfs/token"
)

type vfsAuthWire struct {
	Token string `json:"token"`
}

func (a vfsAuthWire) credential() vfs.Credential {
	return vfs.Credential{Token: a.Token}
}

type vfsBindItem struct {
	Provider string            `json:"provider"`
	Profile  string            `json:"profile"`
	Point    string            `json:"point"`
	Auth     vfsAuthWire       `json:"auth"`
	Params   map[string]string `json:"params"`
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

func (p *acpProtocol) handleVFSBind(ctx context.Context, env ProtocolEnv, pr *parsedRequest) error {
	var params vfsBindParams
	if err := json.Unmarshal(pr.Params, &params); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "invalid bind params: %v", err))
	}
	if params.SessionID == "" {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "sessionId is required"))
	}
	if _, err := p.resolveWireSession(ctx, params.SessionID); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	if len(params.Backends) == 0 {
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrInvalidRequest, "backends is required"))
	}
	agentID := p.sessionAgent(ctx, params.SessionID)
	if agentID == "" && env.Registry != nil {
		agentID = env.Registry.DefaultAgent()
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
		provider := item.Provider
		if provider == "" {
			provider = item.Profile
		}
		b := vfs.Binding{
			Provider: provider,
			Point:    item.Point,
			Auth:     item.Auth.credential(),
			Params:   item.Params,
		}
		if err := env.Registry.BindVFS(ctx, params.SessionID, agentID, b); err != nil {
			errs = append(errs, itemErr{Point: item.Point, Error: err.Error()})
			continue
		}
		if env.Conn != nil && env.Conn.RPC != nil {
			env.Registry.SetVFSTokenRefresh(params.SessionID, b.Provider, vfsTokenRefresh(env.Conn.RPC, params.SessionID, b.Provider))
		}
		okItems = append(okItems, mounted{Point: vfs.BindingSpec(b).Point, Provider: b.Provider})
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
	if _, err := p.resolveWireSession(ctx, params.SessionID); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	if err := env.Registry.RefreshVFS(params.SessionID, params.Provider, params.Auth.credential()); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
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
	if _, err := p.resolveWireSession(ctx, params.SessionID); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	if err := env.Registry.UnbindVFS(params.SessionID, params.Point, params.Provider); err != nil {
		return env.Conn.Writer.WriteError(pr.ID, err)
	}
	return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
}

func installVFSRefresh(env ProtocolEnv, sessionID string, auth *vfs.SessionAuth) {
	if env.Registry == nil || env.Conn == nil || env.Conn.RPC == nil || auth == nil || sessionID == "" {
		return
	}
	seen := map[string]struct{}{}
	for _, b := range auth.Bindings(sessionID) {
		if _, ok := seen[b.Provider]; ok {
			continue
		}
		seen[b.Provider] = struct{}{}
		env.Registry.SetVFSTokenRefresh(sessionID, b.Provider, vfsTokenRefresh(env.Conn.RPC, sessionID, b.Provider))
	}
}
