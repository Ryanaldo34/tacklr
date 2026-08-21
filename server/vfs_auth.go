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

// BindVFS records a user-owned backend and mounts it when the session tree exists.
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
	if err := auth.Bind(sessionID, b); err != nil {
		return clientErrorCause(ErrInvalidRequest, err, "bind vfs")
	}
	spec := vfs.BindingSpec(b)

	r.mountsMu.Lock()
	ms := r.mounts[sessionID]
	r.mountsMu.Unlock()
	if ms != nil {
		if err := ms.Unmount(spec.Point); err != nil {
			slog.Warn("vfs remount: unmount previous", "session_id", sessionID, "point", spec.Point, "error", err)
		}
		if err := ms.Mount(ctx, spec); err != nil {
			if unbindErr := auth.Unbind(sessionID, spec.Point); unbindErr != nil {
				slog.Error("vfs bind rollback failed", "session_id", sessionID, "point", spec.Point, "error", unbindErr)
			}
			return err
		}
		return nil
	}
	if err := vfs.CheckMount(ctx, fsReg, sessionID, spec); err != nil {
		if unbindErr := auth.Unbind(sessionID, spec.Point); unbindErr != nil {
			slog.Error("vfs bind rollback failed", "session_id", sessionID, "point", spec.Point, "error", unbindErr)
		}
		return err
	}
	return nil
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

// UnbindVFS removes one mount point or every binding for a provider.
func (r *Registry) UnbindVFS(sessionID, point, provider string) error {
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
	r.mountsMu.Lock()
	ms := r.mounts[sessionID]
	r.mountsMu.Unlock()
	if ms != nil && point != "" {
		if unmountErr := ms.Unmount(point); unmountErr != nil {
			slog.Warn("vfs unbind: unmount failed", "session_id", sessionID, "point", point, "error", unmountErr)
		}
	}
	if ms != nil && point == "" && provider != "" {
		for _, spec := range ms.Specs() {
			if spec.Profile == provider {
				if unmountErr := ms.Unmount(spec.Point); unmountErr != nil {
					slog.Warn("vfs unbind: unmount failed", "session_id", sessionID, "point", spec.Point, "error", unmountErr)
				}
			}
		}
	}
	return nil
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
