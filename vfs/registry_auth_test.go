package vfs_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestBackendRegistry_bindSessionClearsFactoryAuth(t *testing.T) {
	auth := vfs.NewSessionAuth()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: vfs.ProviderGoogleDrive, Auth: auth}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "local", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	b := vfs.Binding{
		Provider: vfs.ProviderGoogleDrive,
		Params:   map[string]string{vfs.ParamName: "docs", vfs.ParamFolderID: "fld"},
		Auth:     vfs.Credential{Token: "tok"},
	}
	if err := reg.BindSession("s1", []vfs.Binding{b}); err != nil {
		t.Fatal(err)
	}
	if !auth.HasBindings("s1") {
		t.Fatal("want tokens on shared factory auth")
	}
	if err := reg.BindSession("s1", []vfs.Binding{{Provider: "gdrive"}}); err == nil {
		t.Fatal("want bind validation error")
	}
	reg.ClearSession("s1")
	if auth.HasBindings("s1") {
		t.Fatal("want cleared")
	}
}
