// Command tacklr-supervisor runs the sandbox supervisor on one worker host
// (package sandbox/supervisor). It serves mTLS on -listen and starts the
// worker binary (-worker) for each client stream.
//
//	tacklr-supervisor -listen :7443 -cert server.pem -key server-key.pem \
//		-client-ca clients-ca.pem -worker /usr/libexec/tacklr-sandbox-worker
//
// It stops on SIGINT or SIGTERM, after it has killed its workers.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr/sandbox/supervisor"
)

func main() {
	if err := run(); err != nil {
		slog.Error("tacklr-supervisor", "error", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":7443", "address for the mTLS listener")
	certFile := flag.String("cert", "", "PEM file with the server certificate chain")
	keyFile := flag.String("key", "", "PEM file with the server private key")
	clientCAFile := flag.String("client-ca", "", "PEM file with the roots that verify client certificates")
	workerPath := flag.String("worker", "", "absolute path of the sandbox worker binary")
	maxWorkers := flag.Int("max-workers", 64, "largest number of workers that run at one time")
	stopTimeout := flag.Duration("stop-timeout", 5*time.Second, "time a worker has to exit after stop")
	flag.Parse()

	certificate, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		return fmt.Errorf("load the server certificate: %w", err)
	}
	clientCAs, err := loadPool(*clientCAFile)
	if err != nil {
		return err
	}
	sup, err := supervisor.New(supervisor.Config{
		WorkerPath:  *workerPath,
		MaxWorkers:  *maxWorkers,
		StopTimeout: *stopTimeout,
		TLS: &tls.Config{
			Certificates: []tls.Certificate{certificate},
			ClientCAs:    clientCAs,
			MinVersion:   tls.VersionTLS13,
		},
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", *listen)
	if err != nil {
		return err
	}
	slog.Info("tacklr-supervisor: serving", "address", listener.Addr().String(), "worker", *workerPath)
	return sup.Serve(ctx, listener)
}

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the client CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("the client CA file has no PEM certificates")
	}
	return pool, nil
}
