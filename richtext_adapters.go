package tacklr

import (
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

func init() {
	// The harness default registry includes common editor formats. The vfs
	// package itself remains format-agnostic and can be used standalone.
	_ = adapters.RegisterCommon(vfs.DefaultContentRegistry())
}
