package durable

import (
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMemorySecretStorage(t *testing.T) {
	s := NewMemorySecretStorage()
	if err := s.Put(t.Context(), "", Secrets{Auth: AuthContext{Bindings: []vfs.Binding{{
		Auth: vfs.Credential{Token: "x"},
	}}}}); err == nil {
		t.Fatal("empty session id")
	}

	id := SessionID("sess")
	in := Secrets{Auth: AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "tok-1", ExpiresAt: time.Now()},
	}}}}
	if err := s.Put(t.Context(), id, in); err != nil {
		t.Fatal(err)
	}
	in.Auth.Bindings[0].Auth.Token = "mutated"
	in.Auth.Bindings[0].Params[vfs.ParamName] = "other"
	got, err := s.Get(t.Context(), id)
	if err != nil || len(got.Auth.Bindings) != 1 || got.Auth.Bindings[0].Auth.Token != "tok-1" {
		t.Fatalf("get after put: %+v %v", got, err)
	}
	got.Auth.Bindings[0].Auth.Token = "from-get"
	again, _ := s.Get(t.Context(), id)
	if again.Auth.Bindings[0].Auth.Token != "tok-1" {
		t.Fatalf("store aliased get: %q", again.Auth.Bindings[0].Auth.Token)
	}

	if err := s.Put(t.Context(), id, Secrets{Auth: AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "tok-2"},
	}}}}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(t.Context(), id)
	if err != nil || got.Auth.Bindings[0].Auth.Token != "tok-2" {
		t.Fatalf("replace: %+v %v", got, err)
	}
	if err := s.Put(t.Context(), id, Secrets{Auth: AuthContext{Drop: []string{"docs"}}}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(t.Context(), id)
	if err != nil || got.Auth.Bindings[0].Auth.Token != "tok-2" {
		t.Fatalf("drop-only wiped bag: %+v %v", got, err)
	}
	if err := s.Delete(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(t.Context(), id)
	if err != nil || len(got.Auth.Bindings) != 0 {
		t.Fatalf("after delete: %+v %v", got, err)
	}
}

func TestAuthContext_withoutSecrets(t *testing.T) {
	exp := time.Now().Add(time.Hour)
	in := AuthContext{
		Drop: []string{"scratch"},
		Bindings: []vfs.Binding{{
			Provider: vfs.ProviderGoogleDrive,
			Params:   map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld"},
			Auth:     vfs.Credential{Token: "secret", ExpiresAt: exp},
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
	if got.Bindings[0].Provider != vfs.ProviderGoogleDrive || got.Bindings[0].Params[vfs.ParamFolderID] != "fld" || !got.Bindings[0].Writable {
		t.Fatalf("lost metadata: %+v", got.Bindings[0])
	}
	if got.Drop[0] != "scratch" {
		t.Fatalf("drop=%v", got.Drop)
	}
	got.Bindings[0].Params[vfs.ParamName] = "other"
	if in.Bindings[0].Params[vfs.ParamName] != "docs" {
		t.Fatal("params aliased")
	}
}
