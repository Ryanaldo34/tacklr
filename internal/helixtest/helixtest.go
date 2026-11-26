// Package helixtest runs a Helix server for a live test and opens graphs on
// it. Each Start is a server of the test's own, removed when the test ends,
// so no test sees another's data.
package helixtest

import (
	"context"
	"testing"
	"time"

	helix "github.com/helixdb/helix-db/sdks/go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/brain/graphtest"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
)

// Image is the Helix server the live tests run against.
const Image = "ghcr.io/helixdb/helixdb:v0.0.12"

// Options are Graph's options: vector indexes of graphtest.Dimension, and an
// edge index on every relation a test searches. They have no Embedder, so a
// graph ranks text only; Open can add one.
func Options() helixgraph.Options {
	return helixgraph.Options{
		Dimension:     graphtest.Dimension,
		EdgeRelations: []string{graphtest.EdgeSearchLabel, graphtest.OtherEdgeSearchLabel, "about", "has_buyer"},
	}
}

// Start runs a Helix server for t and returns its base URL.
func Start(t testing.TB) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/readyz").WithPort("8080/tcp").WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("helix container: %v", err)
	}
	url, err := ctr.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		t.Fatalf("helix container: %v", err)
	}
	return url
}

// Open opens a graph over the server at url with Options as change sets
// them, such as an Embedding or a Ranking; a nil change keeps them.
func Open(t testing.TB, url string, change func(*helixgraph.Options)) *helixgraph.Graph {
	t.Helper()
	client, err := helix.NewClient(url)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options()
	if change != nil {
		change(&opts)
	}
	g, err := helixgraph.Open(t.Context(), client, opts)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
