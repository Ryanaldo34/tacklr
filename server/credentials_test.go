package server_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// TestCredentialBag_rejectsAnInvalidBinding: a binding the VFS cannot
// mount is not kept.
func TestCredentialBag_rejectsAnInvalidBinding(t *testing.T) {
	t.Parallel()
	var bag server.CredentialBag

	err := bag.Bind(mount.Binding{})

	if err == nil || len(bag.Take().Bindings) != 0 {
		t.Fatalf("bind = %v, want it rejected and nothing kept", err)
	}
}
