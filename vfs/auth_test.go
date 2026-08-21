package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	aliases := map[string]bool{}
	for _, b := range got {
		if b.Auth.Token != "" {
			t.Fatalf("Bindings leaked token: %+v", b)
		}
		if b.Provider != vfs.ProviderGoogleDrive {
			t.Fatalf("provider = %q", b.Provider)
		}
		if b.Point != vfs.WorkspacePoint {
			t.Fatalf("point = %q", b.Point)
		}
		aliases[b.Params[vfs.ParamName]] = true
		raw, err := json.Marshal(vfs.BindingSpec(b))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), tok) {
			t.Fatalf("MountSpec JSON contained token: %s", raw)
		}
		if len(vfs.BindingSpec(b).Members) != 1 || !vfs.BindingSpec(b).Members[0].ReadOnly {
			t.Fatal("BindingSpec member must be read-only")
		}
	}
	if !aliases["contracts"] || !aliases["notes"] {
		t.Fatalf("aliases = %v", aliases)
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
		Auth:     vfs.Credential{Token: "t"},
		Params:   map[string]string{vfs.ParamName: "legal"},
	}); err != nil {
		t.Fatal(err)
	}
	auth.Clear("sess-2")
	if auth.HasBindings("sess-2") {
		t.Fatal("Clear must drop bindings")
	}
}

func TestTokenHolder_proactiveRefreshCoalescesConcurrentCallers(t *testing.T) {
	// Arrange
	holder := vfs.NewTokenHolder(vfs.Credential{
		Token:     "expiring",
		ExpiresAt: time.Now().Add(10 * time.Second),
	})
	release := make(chan struct{})
	var calls atomic.Int32
	holder.SetRefresh(func(context.Context) (vfs.Credential, error) {
		calls.Add(1)
		<-release
		return vfs.Credential{
			Token:     "fresh",
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	})

	// Act
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- holder.EnsureValid(t.Context())
		}()
	}
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	close(errs)

	// Assert
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d", got)
	}
	if got := holder.Current().Token; got != "fresh" {
		t.Fatalf("token = %q", got)
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
		{"workspace without name", "s", vfs.Binding{Provider: "gdrive", Point: vfs.WorkspacePoint, Auth: tok}, nil},
		{"reserved alias", "s", vfs.Binding{Provider: "gdrive", Auth: tok, Params: map[string]string{vfs.ParamName: "work"}}, vfs.ErrInvalidPath},
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

	h.SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{}, vfs.ErrAuthExpired
	})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("refresh expired: %v", err)
	}
	h.SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{}, errors.New("client down")
	})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("wrap client error: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.RefreshOnce(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled refresh: %v", err)
	}
	empty := vfs.NewTokenHolder(vfs.Credential{})
	if _, err := empty.Token(); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("empty token: %v", err)
	}
	h.SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{Token: "   "}, nil
	})
	if err := h.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("empty token: %v", err)
	}

	var nilH *vfs.TokenHolder
	nilH.Set(vfs.Credential{Token: "x"})
	nilH.SetRefresh(nil)
	if nilH.Current().Token != "" {
		t.Fatal("nil holder Current")
	}
	if err := nilH.RefreshOnce(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("nil holder refresh: %v", err)
	}
	if _, err := nilH.Token(); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("nil holder Token: %v", err)
	}
}

func TestSessionAuth_replaceAndUnbindProvider(t *testing.T) {
	auth := vfs.NewSessionAuth()
	first := vfs.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: vfs.Credential{Token: "t1"}, Params: map[string]string{vfs.ParamFolderID: "a"},
	}
	if err := auth.Bind("s", first); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: vfs.Credential{Token: "t2"}, Params: map[string]string{vfs.ParamFolderID: "b"},
	}); err != nil {
		t.Fatal(err)
	}
	got := auth.Bindings("s")
	if len(got) != 1 || got[0].Params[vfs.ParamFolderID] != "b" {
		t.Fatalf("replace bind = %+v", got)
	}
	if tok, _ := auth.Credential("s", "gdrive"); tok.Token != "t2" {
		t.Fatalf("shared holder after replace = %q", tok.Token)
	}
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Point: "/notes", Auth: vfs.Credential{Token: "t2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.UnbindProvider("s", "gdrive"); err != nil {
		t.Fatal(err)
	}
	if auth.HasBindings("s") {
		t.Fatal("UnbindProvider must drop every gdrive point")
	}
	if err := auth.UnbindProvider("s", "gdrive"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("second UnbindProvider: %v", err)
	}
	if err := auth.Refresh("s", "gdrive", vfs.Credential{Token: "x"}); err == nil {
		t.Fatal("refresh after unbind")
	}
	if err := auth.Refresh("", "gdrive", vfs.Credential{Token: "x"}); err == nil {
		t.Fatal("refresh empty session")
	}
	if err := auth.Refresh("s", "gdrive", vfs.Credential{}); err == nil {
		t.Fatal("refresh empty token")
	}
	if err := auth.Unbind("", "/contracts"); err == nil {
		t.Fatal("unbind empty session")
	}
	if err := auth.Unbind("s", "/a/b"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("unbind multi-segment: %v", err)
	}
	if err := auth.UnbindProvider("", "gdrive"); err == nil {
		t.Fatal("UnbindProvider empty session")
	}

	var nilA *vfs.SessionAuth
	if err := nilA.Bind("s", first); err == nil {
		t.Fatal("nil Bind")
	}
	if err := nilA.Refresh("s", "gdrive", vfs.Credential{Token: "t"}); err == nil {
		t.Fatal("nil Refresh")
	}
	if err := nilA.Unbind("s", "/contracts"); err == nil {
		t.Fatal("nil Unbind")
	}
	if err := nilA.UnbindProvider("s", "gdrive"); err == nil {
		t.Fatal("nil UnbindProvider")
	}
	nilA.Clear("s")
	if nilA.HasBindings("s") || nilA.Bindings("s") != nil || nilA.Holder("s", "gdrive") != nil {
		t.Fatal("nil SessionAuth accessors")
	}
}
