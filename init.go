package tacklr

import (
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

func init() {
	RegisterDefaults()
	_ = adapters.RegisterCommon(vfs.DefaultContentRegistry())
}
