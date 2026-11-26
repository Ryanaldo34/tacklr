// Command fakevm stands in for Firecracker in the sandbox tests that boot
// no VM. The supervisor starts it as it starts Firecracker: it reads the
// VM's configuration file (--config-file), finds the VM's vsock socket, and
// does what steps.txt, next to its own path, says, one step a line:
//
//	connect   connect to the supervisor's control port, as a guest's init
//	send JSON write a control frame
//	raw HEX   write raw bytes
//	read N    read N bytes
//	close     close the control connection
//	serve     serve the supervisor with the worker (package worker): bash in /tmp
//	hang      wait until the supervisor kills it
//	exit N    exit with code N
//
// It exits 0 after the last step.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/sandbox/worker"
)

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fakevm:", err)
	}
	os.Exit(code)
}

// run does the steps and returns the exit code.
func run() (int, error) {
	var vsock string
	for i, arg := range os.Args {
		if arg == "--config-file" && i+1 < len(os.Args) {
			raw, err := os.ReadFile(os.Args[i+1]) //nolint:gosec // G703: the supervisor's own configuration file
			if err != nil {
				return 1, err
			}
			var config struct {
				Vsock struct {
					UDSPath string `json:"uds_path"`
				} `json:"vsock"`
			}
			if err := json.Unmarshal(raw, &config); err != nil {
				return 1, err
			}
			vsock = config.Vsock.UDSPath
		}
	}
	steps, err := os.ReadFile(filepath.Join(filepath.Dir(os.Args[0]), "steps.txt")) //nolint:gosec // G703: the test's steps, next to this program
	if err != nil {
		return 1, err
	}
	var conn net.Conn
	for line := range strings.Lines(string(steps)) {
		verb, arg, _ := strings.Cut(strings.TrimSuffix(line, "\n"), " ")
		switch verb {
		case "connect":
			if conn, err = net.Dial("unix", vsock+"_1024"); err != nil {
				return 1, err
			}
		case "send":
			var m wire.Message
			if err := json.Unmarshal([]byte(arg), &m); err != nil {
				return 1, err
			}
			if err := wire.Write(conn, m); err != nil {
				return 1, err
			}
		case "raw":
			b, err := hex.DecodeString(arg)
			if err != nil {
				return 1, err
			}
			if _, err := conn.Write(b); err != nil {
				return 1, err
			}
		case "read":
			n, err := strconv.Atoi(arg)
			if err != nil {
				return 1, err
			}
			if _, err := io.ReadFull(conn, make([]byte, n)); err != nil {
				return 1, err
			}
		case "close":
			if err := conn.Close(); err != nil {
				return 1, err
			}
		case "serve":
			if err := worker.Serve(conn, worker.Runner{Bash: "/bin/bash", Dir: os.TempDir(), Env: []string{"PATH=/usr/bin:/bin"}}); err != nil {
				return 1, err
			}
		case "hang":
			select {}
		case "exit":
			code, err := strconv.Atoi(arg)
			return code, err
		default:
			return 1, fmt.Errorf("unknown step %q", verb)
		}
	}
	return 0, nil
}
