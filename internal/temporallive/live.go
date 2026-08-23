// Package temporallive starts one Temporal CLI dev server per test process.
package temporallive

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

var (
	mu       sync.Mutex
	dev      *testsuite.DevServer
	startErr error
	started  bool
)

// Stop shuts down the process-wide dev server. Call from TestMain after m.Run.
func Stop() {
	mu.Lock()
	defer mu.Unlock()
	if dev != nil {
		_ = dev.Stop()
		dev = nil
	}
}

func HostPort(t *testing.T) string {
	t.Helper()
	_ = Client(t)
	mu.Lock()
	defer mu.Unlock()
	if dev == nil {
		t.Fatal("temporal dev server not started")
	}
	return dev.FrontendHostPort()
}

func cliPath() (string, error) {
	if p, err := exec.LookPath("temporal"); err == nil {
		return p, nil
	}
	for _, p := range []string{"/opt/homebrew/bin/temporal", "/usr/local/bin/temporal"} {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("temporal CLI not found on PATH")
}

// Client returns the shared Temporal CLI dev-server client. Skips in -short
// or when the CLI cannot start.
func Client(t *testing.T) client.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("temporal live server")
	}
	mu.Lock()
	defer mu.Unlock()
	if !started {
		started = true
		path, err := cliPath()
		if err != nil {
			startErr = err
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			dev, startErr = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
				ExistingPath: path,
				LogLevel:     "error",
				EnableUI:     false,
				Stdout:       io.Discard,
				Stderr:       io.Discard,
			})
			cancel()
		}
	}
	if startErr != nil {
		t.Skipf("temporal dev server: %v", startErr)
	}
	return dev.Client()
}
