// Package mount is how a host names a cloud folder for a session to mount:
// the binding, its credential, and the parameter names providers read.
package mount

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// Credential is a session-scoped access token. Never store this on MountSpec
// or in a checkpoint / session.Store. Work-item payloads (Prompt/Resume) may
// carry it; backends must not persist it with recipes. Workflow history
// must not contain Token; the runtime keeps it in SecretStorage.
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
}

// WorkspacePoint is the only top-level mount. Backends live at
// /workspace/<name> via Tree(At(...)).
const WorkspacePoint = "/workspace"

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

// ErrInvalidBinding is a binding or alias that Validate or ValidAlias
// rejects.
var ErrInvalidBinding = errors.New("mount: invalid binding")

// Alias is the binding's folder name under /workspace: its name parameter,
// else its absolute point without the leading "/" ("/contracts" is
// contracts). It is "" when the binding has neither.
func (b Binding) Alias() string {
	if name := strings.TrimSpace(b.Params[ParamName]); name != "" {
		return name
	}
	point := strings.TrimSpace(b.Point)
	if !path.IsAbs(point) {
		return ""
	}
	return strings.TrimPrefix(path.Clean(point), "/")
}

// Validate checks that the binding has a provider, an access token and a
// valid alias. The provider checks its own parameters (folderId, siteId)
// when it opens.
func (b Binding) Validate() error {
	if strings.TrimSpace(b.Provider) == "" {
		return fmt.Errorf("%w: provider required", ErrInvalidBinding)
	}
	if strings.TrimSpace(b.Auth.Token) == "" {
		return fmt.Errorf("%w: access token required", ErrInvalidBinding)
	}
	return ValidAlias(b.Alias())
}

// ValidAlias checks that alias is one path segment and not a name the
// workspace keeps for itself (work, skills, workspace).
func ValidAlias(alias string) error {
	switch {
	case alias == "":
		return fmt.Errorf("%w: a name or an absolute one-segment point is required", ErrInvalidBinding)
	case alias == "." || alias == ".." || strings.ContainsAny(alias, "/\\\x00"):
		return fmt.Errorf("%w: alias %q must be one path segment", ErrInvalidBinding, alias)
	case alias == "work" || alias == "skills" || alias == "workspace":
		return fmt.Errorf("%w: alias %q is reserved", ErrInvalidBinding, alias)
	}
	return nil
}
