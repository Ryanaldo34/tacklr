// Command supervisor runs the example's sandbox supervisor on a sandbox
// host: the agent's run_command runs here, in Firecracker microVMs, over the
// example's tree. A host builds its own supervisor like this, with the tree
// its sessions use. See README.md.
//
// It reads:
//
//	SUPERVISOR_ADDR       where it serves mTLS, default :7443
//	SUPERVISOR_CERT       PEM file of its certificate chain (required)
//	SUPERVISOR_KEY        PEM file of its private key (required)
//	SUPERVISOR_CLIENT_CA  PEM file of the roots that verify the session workers (required)
//	FIRECRACKER           absolute path of the firecracker binary (required)
//	GUEST_KERNEL          absolute path of the guest kernel, from make sandbox-image (required)
//	GUEST_ROOTFS          absolute path of the guest root file system, from make sandbox-image (required)
//	WORKSPACE_DIR         an absolute directory: each session's /workspace/work is under it (required)
package main

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryanaldo34/tacklr/examples/temporal/agent"
	"github.com/ryanaldo34/tacklr/examples/temporal/cmd/internal/stack"
	"github.com/ryanaldo34/tacklr/sandbox/supervisor"
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
	if err := os.MkdirAll(dir, 0o750); err != nil { //nolint:gosec // G703: the path is the operator's setting
		return err
	}
	certificate, clientCAs, err := stack.LoadTLS(os.Getenv("SUPERVISOR_CERT"), os.Getenv("SUPERVISOR_KEY"), os.Getenv("SUPERVISOR_CLIENT_CA"))
	if err != nil {
		return err
	}
	sup, err := supervisor.New(supervisor.Config{
		Firecracker: os.Getenv("FIRECRACKER"),
		Kernel:      os.Getenv("GUEST_KERNEL"),
		Rootfs:      os.Getenv("GUEST_ROOTFS"),
		Workspace:   agent.Tree(dir),
		TLS:         &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: clientCAs, MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		return err
	}
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", cmp.Or(os.Getenv("SUPERVISOR_ADDR"), ":7443"))
	if err != nil {
		return err
	}
	log.Printf("supervisor: serving on %s", listener.Addr())
	return sup.Serve(ctx, listener)
}
