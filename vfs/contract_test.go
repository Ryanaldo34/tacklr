package vfs_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/internal/vfstest"
)

// Every built-in backend runs the same contract. A backend that shares one
// server across cases gives each case its own prefix, so cases never see
// each other's files and nothing is reset between them.

func TestLocal_contract(t *testing.T) {
	vfstest.Run(t, func(t *testing.T) vfstest.Backend {
		ms, err := vfs.Tree(vfs.At("data", vfs.Local(t.TempDir())))(t.Context(), "sess-local", vfs.Request{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ms.Close() })
		return vfstest.Backend{Session: ms, Root: "/workspace/data", ReadsAfterRemove: true}
	})
}

func TestS3_contract(t *testing.T) {
	client, bucket := startMinIO(t.Context(), t)
	vfstest.Run(t, func(t *testing.T) vfstest.Backend {
		return prefixedBackend(t, vfs.S3(vfs.AWSS3{Client: client}, bucket))
	})
}

func TestBlob_contract(t *testing.T) {
	client, container := startAzurite(t.Context(), t)
	vfstest.Run(t, func(t *testing.T) vfstest.Backend {
		return prefixedBackend(t, vfs.Blob(vfs.AzureBlob{Client: client}, container))
	})
}

func TestDrive_contract(t *testing.T) {
	vfstest.Run(t, func(t *testing.T) vfstest.Backend {
		return vfstest.Backend{Session: mountDrive(t, testdrive.Tree(), true), Root: "/workspace/contracts"}
	})
}

func TestGraph_contract(t *testing.T) {
	vfstest.Run(t, func(t *testing.T) vfstest.Backend {
		fx := newGraphFX().legalTree()
		srv := testhttp.New(t, fx)
		fx.base = srv.URL
		ms, _ := mountGraphHTTP(t, srv, true)
		return vfstest.Backend{Session: ms, Root: "/workspace/legal"}
	})
}

// prefixedBackend mounts open under a key prefix no other case uses.
func prefixedBackend(t *testing.T, open vfs.Open) vfstest.Backend {
	t.Helper()
	ms, err := vfs.Tree(vfs.At("data", open))(t.Context(), "sess-contract", vfs.Request{
		Bindings: []vfs.Binding{{Params: map[string]string{vfs.ParamName: "data", "prefix": uuid.NewString()}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return vfstest.Backend{Session: ms, Root: "/workspace/data"}
}
