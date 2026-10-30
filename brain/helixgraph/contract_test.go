package helixgraph_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain/graphtest"
)

func TestHelixGraph_contract(t *testing.T) {
	g := liveGraph(t)
	if err := g.EnsureEdgeTextIndex(t.Context(), graphtest.EdgeSearchLabel); err != nil {
		t.Fatal(err)
	}
	graphtest.Run(t, func(*testing.T) graphtest.Graph { return g })
}
