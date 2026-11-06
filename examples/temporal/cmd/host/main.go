// Command host is the example's protocol server: ACP over WebSocket on the
// Temporal session client. It keeps no session state, so run as many as you
// need behind a load balancer; see README.md.
//
// Besides the variables of package stack it reads:
//
//	API_TOKEN  the bearer token every request must carry (required)
//	ADDR       where it listens, default :8080
package main

import (
	"cmp"
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryanaldo34/tacklr/examples/host"
	"github.com/ryanaldo34/tacklr/examples/temporal/cmd/internal/stack"
	serverpg "github.com/ryanaldo34/tacklr/server/postgres"
	"github.com/ryanaldo34/tacklr/session/temporal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	token := os.Getenv("API_TOKEN")
	if token == "" {
		return errors.New("API_TOKEN must be set")
	}
	st, err := stack.Open(ctx, "tacklr-host")
	if err != nil {
		return err
	}
	defer st.Close()
	wire := serverpg.NewWireStore(st.Pool, "acp")
	if err := wire.Setup(ctx); err != nil {
		return err
	}
	sessions := temporal.Open(st.Temporal, temporal.Config{
		TaskQueue:      st.TaskQueue,
		Store:          st.Store,
		Secrets:        st.Secrets,
		Fallback:       st.Events,
		DisableStreams: true,
		Live:           temporal.Live{Key: st.LiveKey},
	})
	srv := host.New(sessions, st.Model, wire, token)
	// The workers mount a workspace, so a client may bind its own sources.
	srv.Workspace = true
	return srv.ServeHTTP(ctx, cmp.Or(os.Getenv("ADDR"), ":8080"))
}
