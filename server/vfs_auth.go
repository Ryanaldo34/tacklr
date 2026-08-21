package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/ryanaldo34/tacklr/vfs"
)

// VFSAuth returns the session credential store.
func (r *Registry) VFSAuth() *vfs.SessionAuth {
	if r == nil {
		return nil
	}
	return r.vfsAuth
}

func (r *Registry) agentFS(agentID string) (*vfs.BackendRegistry, error) {
	if agentID == "" {
		agentID = r.defaultAgent
	}
	spec, ok := r.agents[agentID]
	if !ok {
		return nil, clientErrorf(ErrAgentNotFound, "agent %q not found", agentID)
	}
	if spec.FSRegistry == nil {
		return nil, clientErrorf(ErrInvalidRequest, "agent %q has no vfs registry", agentID)
	}
	return spec.FSRegistry, nil
}

// BindVFS records a user-owned backend for the next turn. The client supplies
// credentials before RunTurn; this does not remount a live tree. Cloud binds
// attach under /workspace/<alias> when openTurnVFS materializes. CheckMount
// of the inner factory runs before credentials are kept so a bad token does
// not drop sibling aliases.
func (r *Registry) BindVFS(ctx context.Context, sessionID, agentID string, b vfs.Binding) error {
	if r == nil {
		return clientErrorf(ErrInvalidRequest, "registry required")
	}
	if sessionID == "" {
		return clientErrorf(ErrInvalidRequest, "sessionId is required")
	}
	if err := vfs.ValidateBinding(b); err != nil {
		return clientErrorCause(ErrInvalidRequest, err, "invalid vfs binding")
	}
	fsReg, err := r.agentFS(agentID)
	if err != nil {
		return err
	}
	if !fsReg.HasProfile(b.Provider) {
		return clientErrorf(ErrInvalidRequest, "unknown vfs profile %q", b.Provider)
	}
	auth := r.VFSAuth()
	inner := vfs.BindingMember(b)
	alias := inner.Params[vfs.ParamName]
	prev, prevCred := snapshotBinding(auth, sessionID, b.Provider, alias)

	if err := auth.Bind(sessionID, b); err != nil {
		return clientErrorCause(ErrInvalidRequest, err, "bind vfs")
	}
	if err := vfs.CheckMount(ctx, fsReg, sessionID, inner); err != nil {
		restoreBinding(auth, sessionID, b.Provider, alias, prev, prevCred)
		return err
	}
	return nil
}

func snapshotBinding(auth *vfs.SessionAuth, sessionID, provider, alias string) (vfs.Binding, vfs.Credential) {
	cred, _ := auth.Credential(sessionID, provider)
	var prev vfs.Binding
	for _, existing := range auth.Bindings(sessionID) {
		if existing.Params[vfs.ParamName] == alias {
			prev = existing
			break
		}
	}
	return prev, cred
}

func restoreBinding(auth *vfs.SessionAuth, sessionID, provider, alias string, prev vfs.Binding, prevCred vfs.Credential) {
	if prev.Provider == "" {
		if err := auth.Unbind(sessionID, alias); err != nil {
			slog.Error("vfs bind rollback failed", "session_id", sessionID, "alias", alias, "error", err)
		}
		if prevCred.Token != "" && auth.Holder(sessionID, provider) != nil {
			if err := auth.Refresh(sessionID, provider, prevCred); err != nil {
				slog.Error("vfs bind rollback token restore failed", "session_id", sessionID, "provider", provider, "error", err)
			}
		}
		return
	}
	prev.Auth = prevCred
	if err := auth.Bind(sessionID, prev); err != nil {
		slog.Error("vfs bind rollback restore failed", "session_id", sessionID, "alias", alias, "error", err)
	}
}

// RefreshVFS replaces the access token for a provider on the session.
func (r *Registry) RefreshVFS(sessionID, provider string, c vfs.Credential) error {
	if r == nil || r.VFSAuth() == nil {
		return clientErrorf(ErrInvalidRequest, "vfs auth is not configured")
	}
	if err := r.vfsAuth.Refresh(sessionID, provider, c); err != nil {
		return clientErrorCause(ErrInvalidRequest, err, "refresh vfs token")
	}
	return nil
}

// UnbindVFS drops one alias or every binding for a provider. Takes effect on
// the next turn. The live tree is unchanged.
func (r *Registry) UnbindVFS(_ context.Context, sessionID, point, provider string) error {
	if r == nil || r.VFSAuth() == nil {
		return clientErrorf(ErrInvalidRequest, "vfs auth is not configured")
	}
	var err error
	if point != "" {
		err = r.vfsAuth.Unbind(sessionID, point)
	} else if provider != "" {
		err = r.vfsAuth.UnbindProvider(sessionID, provider)
	} else {
		return clientErrorf(ErrInvalidRequest, "point or provider is required")
	}
	if err != nil {
		return clientErrorCause(ErrInvalidRequest, err, "unbind vfs")
	}
	return nil
}

func workspaceMembers(binds []vfs.Binding) []vfs.MountSpec {
	members := make([]vfs.MountSpec, 0, len(binds))
	for _, b := range binds {
		members = append(members, vfs.BindingMember(b))
	}
	return members
}

// SetVFSTokenRefresh installs the client callback used after a 401.
func (r *Registry) SetVFSTokenRefresh(sessionID, provider string, fn vfs.TokenRefreshFunc) {
	if r == nil || r.vfsAuth == nil {
		return
	}
	if h := r.vfsAuth.Holder(sessionID, provider); h != nil {
		h.SetRefresh(fn)
	}
}

func vfsTokenRefresh(rpc *ClientBridge, sessionID, provider string) vfs.TokenRefreshFunc {
	if rpc == nil {
		return nil
	}
	return func(ctx context.Context) (vfs.Credential, error) {
		if !rpc.GetCaps().VFSTokenRefresh {
			return vfs.Credential{}, vfs.ErrAuthExpired
		}
		raw, err := rpc.Call(ctx, methodVFSToken, map[string]any{
			"sessionId": sessionID,
			"provider":  provider,
		})
		if err != nil {
			return vfs.Credential{}, fmt.Errorf("%w: %w", vfs.ErrAuthExpired, err)
		}
		var res vfsAuthWire
		if err := json.Unmarshal(raw, &res); err != nil {
			return vfs.Credential{}, fmt.Errorf("%w: %w", vfs.ErrAuthExpired, err)
		}
		if res.Token == "" {
			return vfs.Credential{}, vfs.ErrAuthExpired
		}
		return res.credential(), nil
	}
}
