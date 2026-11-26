// Command backend is the example's backend: the HTTP API of package
// examples/backend on the Temporal session client. It keeps no session
// state, so run as many as you need behind a load balancer; see README.md.
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
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr/examples/backend"
	"github.com/ryanaldo34/tacklr/examples/temporal/cmd/internal/stack"
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
	st, err := stack.Open(ctx, "tacklr-backend")
	if err != nil {
		return err
	}
	defer st.Close()
	sessions := temporal.Open(st.Temporal, temporal.Config{
		TaskQueue:      st.TaskQueue,
		Store:          st.Store,
		Secrets:        st.Secrets,
		Fallback:       st.Events,
		DisableStreams: true,
		Live:           temporal.Live{Key: st.LiveKey},
	})
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", cmp.Or(os.Getenv("ADDR"), ":8080"))
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           backend.New(sessions, token),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	stopped := context.AfterFunc(ctx, func() { _ = server.Shutdown(context.WithoutCancel(ctx)) })
	defer stopped()
	log.Printf("backend: serving on %s", listener.Addr())
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
