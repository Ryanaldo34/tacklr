// Package helixtest runs the Helix server live tests use, and one graph
// opened on it. Every test package of one go test run shares one server: a
// reusable container by name, which Ryuk removes when the run ends. Tests
// share its data, so each writes under a tenant of its own.
package helixtest

import (
	"context"
	"sync"
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
// edge index on every relation a test searches. Graph has no Embedder, so it
// ranks text only; Open can add one.
var Options = helixgraph.Options{
	Dimension:     graphtest.Dimension,
	EdgeRelations: []string{graphtest.EdgeSearchLabel, graphtest.OtherEdgeSearchLabel, "about", "has_buyer"},
}

var (
	serverOnce     sync.Once
	serverURL      string
	serverStartErr error
	graphOnce      sync.Once
	graph          *helixgraph.Graph
	graphErr       error
)

// containerName is the reusable container every test package attaches to.
const containerName = "tacklr-helix-test"

// URL returns the shared Helix server's base URL, starting it on first use.
func URL(t testing.TB) string {
	t.Helper()
	serverOnce.Do(func() {
		ctx := context.Background()
		ctr, err := testcontainers.Run(ctx, Image,
			testcontainers.WithExposedPorts("8080/tcp"),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/readyz").WithPort("8080/tcp").WithStartupTimeout(2*time.Minute)),
			testcontainers.WithReuseByName(containerName),
		)
		if err != nil {
			serverStartErr = err
			return
		}
		serverURL, serverStartErr = ctr.PortEndpoint(ctx, "8080/tcp", "http")
	})
	if serverStartErr != nil {
		t.Fatalf("helix container: %v", serverStartErr)
	}
	return serverURL
}

// Open opens a graph over the container with Options as change sets them,
// such as an Embedding or a Ranking. The indexes exist after Graph, so it
// is cheap.
func Open(t testing.TB, change func(*helixgraph.Options)) *helixgraph.Graph {
	t.Helper()
	Graph(t)
	client, err := helix.NewClient(URL(t))
	if err != nil {
		t.Fatal(err)
	}
	opts := Options
	change(&opts)
	g, err := helixgraph.Open(t.Context(), client, opts)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// Graph is one text-only graph with Options, opened once over the container.
// Tests share it, so each writes under a tenant of its own.
func Graph(t testing.TB) *helixgraph.Graph {
	t.Helper()
	base := URL(t)
	graphOnce.Do(func() {
		client, err := helix.NewClient(base)
		if err != nil {
			graphErr = err
			return
		}
		graph, graphErr = helixgraph.Open(context.Background(), client, Options)
	})
	if graphErr != nil {
		t.Fatal(graphErr)
	}
	return graph
}
