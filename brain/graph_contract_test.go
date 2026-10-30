package brain_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/graphtest"
)

func TestMemoryGraph_contract(t *testing.T) {
	graphtest.Run(t, func(*testing.T) graphtest.Graph { return brain.NewMemoryGraph() })
}
