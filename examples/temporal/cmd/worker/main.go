// Command worker runs the example's sessions: the session workflow and its
// steps, the agent, and the live endpoint that streams a
// session's deltas to the backends. Run as many as you need; see README.md.
//
// Besides the variables of package stack, and the model's (stack.Model), it
// reads:
//
//	LIVE_URL       how backends reach this worker's live endpoint, default http://localhost:8081
//	LIVE_ADDR      where it listens, default :8081
//	MAX_WINDOW     the context window in tokens, default the model's
//	SANDBOX_ADDR   the sandbox supervisor (host:port); gives the agent run_command (optional)
//	SANDBOX_CERT, SANDBOX_KEY, SANDBOX_CA  PEM files of this worker's client certificate, its key, and the supervisor's roots (with SANDBOX_ADDR)
package main

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr/examples/temporal/agent"
	"github.com/ryanaldo34/tacklr/examples/temporal/cmd/internal/stack"
	"github.com/ryanaldo34/tacklr/sandbox"
	"github.com/ryanaldo34/tacklr/session/temporal/worker"
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
	st, err := stack.Open(ctx, "tacklr-worker")
	if err != nil {
		return err
	}
	defer st.Close()

	model, err := stack.Model()
	if err != nil {
		return err
	}
	opts := agent.Options(model)
	if n := os.Getenv("MAX_WINDOW"); n != "" {
		size, err := strconv.ParseUint(n, 10, 0)
		if err != nil {
			return errors.New("MAX_WINDOW must be a number of tokens")
		}
		opts.MaxWindowSize = uint(size)
	}
	var sandboxClient *sandbox.Client
	if addr := os.Getenv("SANDBOX_ADDR"); addr != "" {
		certificate, roots, err := stack.LoadTLS(os.Getenv("SANDBOX_CERT"), os.Getenv("SANDBOX_KEY"), os.Getenv("SANDBOX_CA"))
		if err != nil {
			return err
		}
		sandboxClient = sandbox.NewClient(addr, &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots, MinVersion: tls.VersionTLS13})
	}
	w := worker.New(st.Temporal, worker.Config{
		Agent:          opts,
		Sandbox:        sandboxClient,
		TaskQueue:      st.TaskQueue,
		Store:          st.Store,
		Secrets:        st.Secrets,
		Fallback:       st.Events,
		DisableStreams: true,
		Live:           worker.Live{URL: cmp.Or(os.Getenv("LIVE_URL"), "http://localhost:8081"), Key: st.LiveKey},
	})

	// Deltas are never stored: a host dials the worker that makes them.
	live := &http.Server{Addr: cmp.Or(os.Getenv("LIVE_ADDR"), ":8081"), Handler: w.LiveHandler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := live.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("live endpoint: %v", err)
		}
	}()
	defer func() { _ = live.Shutdown(context.Background()) }()

	interrupt := make(chan any)
	go func() {
		<-ctx.Done()
		close(interrupt)
	}()
	return w.Run(interrupt)
}
