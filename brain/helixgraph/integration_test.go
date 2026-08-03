package helixgraph_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// enterprise-dev in-memory mode (no PATH_TO_QUERIES / MinIO). Dynamic /v1/query.
// https://docs.helix-db.com/database/local-development
const helixDevImage = "ghcr.io/helixdb/enterprise-dev:latest"

var (
	helixOnce sync.Once
	helixURL  string
	helixErr  error
	helixSkip string
)

// TestGraph_liveNeighborsBoth is the real Helix outcome for Both-direction
// expand: outbound and inbound edges by object_id, multi-label, and limit.
func TestGraph_liveNeighborsBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t, ctx)

	a, b, c := uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{a, b, c} {
		if err := g.PutObject(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	// a --references--> b, c --references--> a (inbound to a)
	if err := g.AddEdge(ctx, a, b, "references"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, c, a, "references"); err != nil {
		t.Fatal(err)
	}
	// a --depends_on--> b (second label)
	if err := g.AddEdge(ctx, a, b, "depends_on"); err != nil {
		t.Fatal(err)
	}

	ns, err := g.Neighbors(ctx, a, []string{"references"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]string{}
	for _, n := range ns {
		got[n.ObjectID] = n.RelationType
	}
	if got[b] != "references" || got[c] != "references" {
		t.Fatalf("both directions for references: %+v", ns)
	}
	if len(ns) != 2 {
		t.Fatalf("want exactly b and c: %+v", ns)
	}

	// Label filter: depends_on should only surface b.
	dep, err := g.Neighbors(ctx, a, []string{"depends_on"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dep) != 1 || dep[0].ObjectID != b || dep[0].RelationType != "depends_on" {
		t.Fatalf("depends_on: %+v", dep)
	}

	// Multi-label still returns neighbors (deduped by object id across labels).
	multi, err := g.Neighbors(ctx, a, []string{"references", "depends_on"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	multiIDs := map[uuid.UUID]bool{}
	for _, n := range multi {
		multiIDs[n.ObjectID] = true
	}
	if !multiIDs[b] || !multiIDs[c] {
		t.Fatalf("multi label: %+v", multi)
	}

	// Limit truncates.
	lim, err := g.Neighbors(ctx, a, []string{"references"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(lim) != 1 {
		t.Fatalf("limit 1: %+v", lim)
	}

	// Unknown object → empty, not error.
	empty, err := g.Neighbors(ctx, uuid.New(), []string{"references"}, 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("unknown: %+v err=%v", empty, err)
	}
}

// TestGraph_livePutObjectIdempotent re-puts the same object_id without error.
func TestGraph_livePutObjectIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t, ctx)
	id := uuid.New()
	if err := g.PutObject(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := g.PutObject(ctx, id); err != nil {
		t.Fatal(err)
	}
	// Edge still resolvable after re-put of both endpoints.
	other := uuid.New()
	if err := g.PutObject(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, id, other, "references"); err != nil {
		t.Fatal(err)
	}
	ns, err := g.Neighbors(ctx, id, []string{"references"}, 5)
	if err != nil || len(ns) != 1 || ns[0].ObjectID != other {
		t.Fatalf("after re-put: %+v err=%v", ns, err)
	}
}

func liveGraph(t *testing.T, ctx context.Context) *helixgraph.Graph {
	t.Helper()
	base := sharedHelixURL(t, ctx)
	g, err := helixgraph.New(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObjectIndex(ctx); err != nil {
		t.Fatal(err)
	}
	return g
}

func sharedHelixURL(t *testing.T, ctx context.Context) string {
	t.Helper()
	helixOnce.Do(func() {
		ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        helixDevImage,
				ExposedPorts: []string{"8080/tcp"},
				WaitingFor: wait.ForHTTP("/health").
					WithPort("8080/tcp").
					WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
		if err != nil {
			helixSkip = fmt.Sprintf("%v (docker pull %s)", err, helixDevImage)
			return
		}
		host, err := ctr.Host(ctx)
		if err != nil {
			helixErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		port, err := ctr.MappedPort(ctx, "8080/tcp")
		if err != nil {
			helixErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		// Keep container for process lifetime; Ryuk reaps on exit.
		helixURL = fmt.Sprintf("http://%s:%s", host, port.Port())
	})
	if helixSkip != "" {
		t.Skipf("helix container unavailable: %s", helixSkip)
	}
	if helixErr != nil {
		t.Fatal(helixErr)
	}
	if helixURL == "" {
		t.Fatal("helix url empty")
	}
	return helixURL
}
