// Package mount is how a host names a cloud folder for a session to mount:
// the binding, its credential, and the parameter names providers read.
package mount

import (
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

// Alias is the binding's folder name under /workspace: its name parameter,
// else its single-segment point.
func (b Binding) Alias() string {
	if name := strings.TrimSpace(b.Params[ParamName]); name != "" {
		return name
	}
	point := strings.TrimSpace(b.Point)
	if point == "" || point == WorkspacePoint {
		return ""
	}
	return strings.TrimPrefix(point, "/")
}
