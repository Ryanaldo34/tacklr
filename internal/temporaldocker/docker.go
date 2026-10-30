// Package temporaldocker starts the same Temporal server the live tests use:
// Postgres and temporalio/server in Docker.
package temporaldocker

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	serverImage = "temporalio/server:latest"
	adminImage  = "temporalio/admin-tools:latest"
	pgImage     = "tacklr-pg-brain:test"
)

var (
	mu       sync.Mutex
	ctr      testcontainers.Container
	pg       testcontainers.Container
	netw     *testcontainers.DockerNetwork
	addr     string
	startErr error
	started  bool
)

// HostPort returns the shared Temporal frontend. The test skips when Docker
// or the Postgres image is not available.
func HostPort(t testing.TB) string {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if !started {
		started = true
		startErr = start()
	}
	if startErr != nil {
		t.Skipf("Temporal docker unavailable: %v", startErr)
	}
	return addr
}

// Stop terminates the shared containers. Call from TestMain.
func Stop() {
	mu.Lock()
	defer mu.Unlock()
	stop()
}

func stop() {
	if ctr != nil {
		_ = ctr.Terminate(context.Background())
		ctr = nil
	}
	if pg != nil {
		_ = pg.Terminate(context.Background())
		pg = nil
	}
	if netw != nil {
		_ = netw.Remove(context.Background())
		netw = nil
	}
}

// ipEndpoint is the container's mapped frontend port on an IP literal. gRPC
// looks up a DNS TXT record for every host name it dials, and a slow lookup
// can outlast the SDK's 5s connect check.
func ipEndpoint(ctx context.Context, c testcontainers.Container) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := c.MappedPort(ctx, "7233/tcp")
	if err != nil {
		return "", err
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(ips[0].String(), port.Port()), nil
}

func start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nw, err := network.New(ctx)
	if err != nil {
		return err
	}
	netw = nw
	db, err := postgres.Run(ctx, pgImage,
		postgres.WithDatabase("temporal"),
		postgres.WithUsername("temporal"),
		postgres.WithPassword("temporal"),
		postgres.BasicWaitStrategies(),
		network.WithNetwork([]string{"postgresql"}, nw),
	)
	if err != nil {
		stop()
		return fmt.Errorf("%w (build image: make brain-pg-image)", err)
	}
	pg = db
	if code, _, err := db.Exec(ctx, []string{"psql", "-U", "temporal", "-d", "temporal", "-c", "CREATE DATABASE temporal_visibility;"}); err != nil || code != 0 {
		stop()
		if err != nil {
			return err
		}
		return fmt.Errorf("create temporal_visibility: exit %d", code)
	}
	admin, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      adminImage,
			Env:        map[string]string{"SQL_PASSWORD": "temporal"},
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd: []string{`set -eu
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal setup-schema -v 0.0
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal update-schema -d /etc/temporal/schema/postgresql/v12/temporal/versioned
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal_visibility setup-schema -v 0.0
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal_visibility update-schema -d /etc/temporal/schema/postgresql/v12/visibility/versioned
`},
			Networks:   []string{nw.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		stop()
		return err
	}
	st, stErr := admin.State(ctx)
	_ = admin.Terminate(context.Background())
	if stErr != nil || st.ExitCode != 0 {
		stop()
		if stErr != nil {
			return stErr
		}
		return fmt.Errorf("temporal schema setup exit %d", st.ExitCode)
	}
	server, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        serverImage,
			ExposedPorts: []string{"7233/tcp"},
			Networks:     []string{nw.Name},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader("{}\n"),
				ContainerFilePath: "/etc/temporal/config/dynamicconfig/docker.yaml",
				FileMode:          0o644,
			}},
			Env: map[string]string{
				"DB":                   "postgres12",
				"DB_PORT":              "5432",
				"DBNAME":               "temporal",
				"VISIBILITY_DBNAME":    "temporal_visibility",
				"POSTGRES_USER":        "temporal",
				"POSTGRES_PWD":         "temporal",
				"POSTGRES_SEEDS":       "postgresql",
				"BIND_ON_IP":           "0.0.0.0",
				"TEMPORAL_ADDRESS":     "127.0.0.1:7233",
				"TEMPORAL_CLI_ADDRESS": "127.0.0.1:7233",
			},
			WaitingFor: wait.ForListeningPort("7233/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		stop()
		return err
	}
	got, err := ipEndpoint(ctx, server)
	if err != nil {
		_ = server.Terminate(context.Background())
		stop()
		return err
	}
	deadline := time.Now().Add(time.Minute)
	var last error
	for {
		c, dialErr := client.Dial(client.Options{HostPort: got})
		if dialErr == nil {
			hctx, hcancel := context.WithTimeout(ctx, 2*time.Second)
			_, last = c.CheckHealth(hctx, &client.CheckHealthRequest{})
			hcancel()
			c.Close()
			if last == nil {
				break
			}
		} else {
			last = dialErr
		}
		if time.Now().After(deadline) {
			_ = server.Terminate(context.Background())
			stop()
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	ns, err := client.NewNamespaceClient(client.Options{HostPort: got})
	if err != nil {
		_ = server.Terminate(context.Background())
		stop()
		return err
	}
	err = ns.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "default",
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	ns.Close()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		_ = server.Terminate(context.Background())
		stop()
		return err
	}
	ctr = server
	addr = got
	return nil
}
