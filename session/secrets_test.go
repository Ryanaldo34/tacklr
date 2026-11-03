package session

import (
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

func TestAuthContext_withoutSecrets(t *testing.T) {
	exp := time.Now().Add(time.Hour)
	in := AuthContext{
		Drop: []string{"scratch"},
		Bindings: []mount.Binding{{
			Provider: mount.ProviderGoogleDrive,
			Params:   map[string]string{mount.ParamName: "docs", mount.ParamFolderID: "fld"},
			Auth:     mount.Credential{Token: "secret", ExpiresAt: exp},
			Writable: true,
		}},
	}
	got := in.WithoutSecrets()
	if got.Bindings[0].Auth.Token != "" || !got.Bindings[0].Auth.ExpiresAt.IsZero() {
		t.Fatalf("secrets remain: %+v", got.Bindings[0].Auth)
	}
	if in.Bindings[0].Auth.Token != "secret" {
		t.Fatal("WithoutSecrets mutated input")
	}
	if got.Bindings[0].Provider != mount.ProviderGoogleDrive || got.Bindings[0].Params[mount.ParamFolderID] != "fld" || !got.Bindings[0].Writable {
		t.Fatalf("lost metadata: %+v", got.Bindings[0])
	}
	if got.Drop[0] != "scratch" {
		t.Fatalf("drop=%v", got.Drop)
	}
	got.Bindings[0].Params[mount.ParamName] = "other"
	if in.Bindings[0].Params[mount.ParamName] != "docs" {
		t.Fatal("params aliased")
	}
}
