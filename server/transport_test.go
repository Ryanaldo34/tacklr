package server

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
)

func TestServeHTTP_respectsContextCancel(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(nil)).AllowAnonymousNetwork()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeHTTP(ctx, "127.0.0.1:0") }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not exit")
	}
}
