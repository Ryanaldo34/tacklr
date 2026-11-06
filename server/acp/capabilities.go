package acp

import "encoding/json"

// ClientCapabilities captures client features from initialize.
type ClientCapabilities struct {
	ElicitationForm bool
	ElicitationURL  bool
	VFSTokenRefresh bool
}

// ParseClientCapabilities extracts elicitation mode support and Tacklr VFS
// token refresh from initialize params.
func ParseClientCapabilities(params json.RawMessage) ClientCapabilities {
	var p struct {
		ClientCapabilities *struct {
			Elicitation *struct {
				Form json.RawMessage `json:"form"`
				URL  json.RawMessage `json:"url"`
			} `json:"elicitation"`
			Meta *struct {
				Tacklr *struct {
					VFS *struct {
						TokenRefresh bool `json:"tokenRefresh"`
					} `json:"vfs"`
				} `json:"tacklr"`
			} `json:"_meta"`
		} `json:"clientCapabilities"`
	}
	if len(params) == 0 || json.Unmarshal(params, &p) != nil || p.ClientCapabilities == nil {
		return ClientCapabilities{}
	}
	var caps ClientCapabilities
	if el := p.ClientCapabilities.Elicitation; el != nil {
		// Mode is supported only when the field is explicitly present and non-null.
		caps.ElicitationForm = el.Form != nil && string(el.Form) != "null"
		caps.ElicitationURL = el.URL != nil && string(el.URL) != "null"
	}
	if p.ClientCapabilities.Meta != nil && p.ClientCapabilities.Meta.Tacklr != nil && p.ClientCapabilities.Meta.Tacklr.VFS != nil {
		caps.VFSTokenRefresh = p.ClientCapabilities.Meta.Tacklr.VFS.TokenRefresh
	}
	return caps
}
