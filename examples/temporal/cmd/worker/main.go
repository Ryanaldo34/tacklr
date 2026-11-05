// Command worker runs the example's sessions: the session workflow and its
// steps, the agent and its workspace, and the live endpoint that streams a
// session's deltas to the hosts. Run as many as you need; see README.md.
//
// Besides the variables of package stack it reads:
//
//	WORKSPACE_DIR  an absolute directory: each session's /workspace/work is under it (required)
//	LIVE_URL       how hosts reach this worker's live endpoint, default http://localhost:8081
//	LIVE_ADDR      where it listens, default :8081
//	FUSE_ROOT      a directory for FUSE: gives the agent run_command (optional)
//	MAX_WINDOW     the context window in tokens, default the model's
package main

import (
	"cmp"
	"context"
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
	dir := os.Getenv("WORKSPACE_DIR")
	if dir == "" {
		return errors.New("WORKSPACE_DIR must be set")
	}
	// WORKSPACE_DIR is the operator's own directory.
	if err := os.MkdirAll(dir, 0o750); err != nil { //nolint:gosec // G703: the path is the operator's setting
		return err
	}
	st, err := stack.Open(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	opts := agent.Options(st.Model)
	if n := os.Getenv("MAX_WINDOW"); n != "" {
		size, err := strconv.ParseUint(n, 10, 0)
		if err != nil {
			return errors.New("MAX_WINDOW must be a number of tokens")
		}
		opts.MaxWindowSize = uint(size)
	}
	w := worker.New(st.Temporal, worker.Config{
		Agent:          opts,
		Workspace:      agent.Workspace(dir),
		FuseRoot:       os.Getenv("FUSE_ROOT"),
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
