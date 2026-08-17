package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestSessionAuth_bindRefreshUnbind(t *testing.T) {
	ctx := t.Context()
	_ = ctx
	auth := vfs.NewSessionAuth()
	tok := "secret-access-token-xyz"
	if err := auth.Bind("sess-1", vfs.Binding{
		Provider: vfs.ProviderGoogleDrive,
		Point:    "/contracts",
		Auth:     vfs.Credential{Token: tok},
		Params:   map[string]string{vfs.ParamFolderID: "fld-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("sess-1", vfs.Binding{
		Provider: vfs.ProviderGoogleDrive,
		Point:    "/notes",
		Auth:     vfs.Credential{Token: tok},
		Params:   map[string]string{vfs.ParamFolderID: "fld-b"},
	}); err != nil {
		t.Fatal(err)
	}

	got := auth.Bindings("sess-1")
	if len(got) != 2 {
		t.Fatalf("bindings = %d", len(got))
	}
	for _, b := range got {
		if b.Auth.Token != "" {
			t.Fatalf("Bindings leaked token: %+v", b)
		}
		if b.Provider != vfs.ProviderGoogleDrive {
			t.Fatalf("provider = %q", b.Provider)
		}
		raw, err := json.Marshal(vfs.BindingSpec(b))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), tok) {
			t.Fatalf("MountSpec JSON contained token: %s", raw)
		}
		if !vfs.BindingSpec(b).ReadOnly {
			t.Fatal("BindingSpec must be read-only")
		}
	}

	c, ok := auth.Credential("sess-1", vfs.ProviderGoogleDrive)
	if !ok || c.Token != tok {
		t.Fatalf("Credential = %+v ok=%v", c, ok)
	}
	h1 := auth.Holder("sess-1", vfs.ProviderGoogleDrive)
	if h1 == nil {
		t.Fatal("missing holder")
	}

	next := vfs.Credential{Token: "rotated-token"}
	if err := auth.Refresh("sess-1", vfs.ProviderGoogleDrive, next); err != nil {
		t.Fatal(err)
	}
	if auth.Holder("sess-1", vfs.ProviderGoogleDrive) != h1 {
		t.Fatal("refresh must keep the shared holder")
	}
	if got := h1.Current().Token; got != "rotated-token" {
		t.Fatalf("holder token = %q", got)
	}

	if err := auth.Unbind("sess-1", "/contracts"); err != nil {
		t.Fatal(err)
	}
	if !auth.HasBindings("sess-1") {
		t.Fatal("notes should remain")
	}
	if _, ok := auth.Credential("sess-1", vfs.ProviderGoogleDrive); !ok {
		t.Fatal("token should remain while notes is bound")
	}
	if err := auth.Unbind("sess-1", "/notes"); err != nil {
		t.Fatal(err)
	}
	if auth.HasBindings("sess-1") {
		t.Fatal("expected empty session")
	}
	if _, ok := auth.Credential("sess-1", vfs.ProviderGoogleDrive); ok {
		t.Fatal("holder should be gone after last unbind")
	}

	if err := auth.Bind("sess-2", vfs.Binding{
		Provider: "gdrive",
		Point:    "/a",
		Auth:     vfs.Credential{Token: "t"},
	}); err != nil {
		t.Fatal(err)
	}
	auth.Clear("sess-2")
	if auth.HasBindings("sess-2") {
		t.Fatal("Clear must drop bindings")
	}
}

func TestSessionAuth_bindRejects(t *testing.T) {
	auth := vfs.NewSessionAuth()
	tok := vfs.Credential{Token: "t"}
	cases := []struct {
		name string
		sid  string
		b    vfs.Binding
		want error
	}{
		{"empty session", "", vfs.Binding{Provider: "gdrive", Point: "/a", Auth: tok}, vfs.ErrInvalidPath},
		{"empty token", "s", vfs.Binding{Provider: "gdrive", Point: "/a"}, nil},
		{"empty provider", "s", vfs.Binding{Point: "/a", Auth: tok}, nil},
		{"multi segment", "s", vfs.Binding{Provider: "gdrive", Point: "/a/b", Auth: tok}, vfs.ErrInvalidPath},
		{"relative point", "s", vfs.Binding{Provider: "gdrive", Point: "contracts", Auth: tok}, vfs.ErrInvalidPath},
	}
	for _, tc := range cases {
		err := auth.Bind(tc.sid, tc.b)
		if err == nil {
			t.Fatalf("%s: want error", tc.name)
		}
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v want %v", tc.name, err, tc.want)
		}
	}
}

func TestTokenHolder_refreshOnce(t *testing.T) {
	h := vfs.NewTokenHolder(vfs.Credential{Token: "old"})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("no refresh func: %v", err)
	}
	n := 0
	h.SetRefresh(func(ctx context.Context) (vfs.Credential, error) {
		n++
		return vfs.Credential{Token: "new"}, nil
	})
	if err := h.RefreshOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.Current().Token != "new" || n != 1 {
		t.Fatalf("token=%q n=%d", h.Current().Token, n)
	}
	tok, err := h.Token()
	if err != nil || tok.AccessToken != "new" || tok.TokenType != "Bearer" || !tok.Expiry.IsZero() {
		t.Fatalf("oauth token = %+v err=%v", tok, err)
	}
}
