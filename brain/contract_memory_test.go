package brain_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

func TestMemoryStore_isolationContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) storetest.Store { return brain.NewMemoryStore() })
}
