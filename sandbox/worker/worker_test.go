package worker_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/sandbox/worker"
)

func runner(t *testing.T) worker.Runner {
	t.Helper()
	return worker.Runner{Bash: "/bin/bash", Dir: t.TempDir(), Env: []string{"PATH=/usr/bin:/bin"}}
}

// serve serves r on one end of a connection and returns the other end, as
// the supervisor has it, after ready, and the result of Serve.
func serve(t *testing.T, r worker.Runner) (net.Conn, <-chan error) {
	t.Helper()
	conn, supervisor := net.Pipe()
	served := make(chan error, 1)
	go func() { served <- worker.Serve(conn, r) }()
	t.Cleanup(func() { _ = supervisor.Close() })
	ready, err := wire.Read(supervisor)
	require.NoError(t, err)
	require.Equal(t, wire.TypeReady, ready.Message.Type)
	return supervisor, served
}

// readRun reads frames until a control frame, and returns the stdout and
// stderr it got and that message.
func readRun(t *testing.T, conn net.Conn) (string, string, wire.Message) {
	t.Helper()
	var stdout, stderr strings.Builder
	for {
		f, err := wire.Read(conn)
		require.NoError(t, err)
		switch f.Kind {
		case wire.KindStdout:
			stdout.Write(f.Data)
		case wire.KindStderr:
			stderr.Write(f.Data)
		default:
			return stdout.String(), stderr.String(), f.Message
		}
	}
}

func run(t *testing.T, conn net.Conn, id uint64, script string) {
	t.Helper()
	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypeRun, ID: id, Script: script}))
}

func TestServeIsReadyAnswersPingsAndStops(t *testing.T) {
	conn, served := serve(t, runner(t))

	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypePing, ID: 7}))
	pong, pongErr := wire.Read(conn)
	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypeStop}))

	require.NoError(t, pongErr)
	require.Equal(t, wire.Message{Type: wire.TypePong, ID: 7}, pong.Message)
	require.NoError(t, <-served)
}

func TestRunStreamsTheOutputAndReturnsTheExitCode(t *testing.T) {
	conn, served := serve(t, runner(t))

	run(t, conn, 1, "printf 'b\\na\\n' | sort; echo warn >&2; head -c 200000 /dev/zero | tr '\\0' x; exit 3")
	stdout, stderr, done := readRun(t, conn)
	run(t, conn, 2, "kill -9 $$")
	_, _, killed := readRun(t, conn)
	require.NoError(t, conn.Close())

	require.Equal(t, "a\nb\n"+strings.Repeat("x", 200000), stdout)
	require.Equal(t, "warn\n", stderr)
	require.Equal(t, 3, *done.Code)
	require.Equal(t, uint64(1), done.ID)
	require.Equal(t, 128+int(syscall.SIGKILL), *killed.Code)
	require.NoError(t, <-served)
}

func TestRunEndsWhenBashExitsAndPingsAreAnsweredWhileItGoesOn(t *testing.T) {
	conn, served := serve(t, runner(t))
	started := time.Now()

	run(t, conn, 1, "(sleep 30; echo late) & sleep 0.5; echo on")
	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypePing, ID: 9}))
	pong, err := wire.Read(conn)
	require.NoError(t, err)
	stdout, _, done := readRun(t, conn)
	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypeStop}))

	require.Equal(t, wire.TypePong, pong.Message.Type)
	require.Equal(t, "on\n", stdout)
	require.Equal(t, 0, *done.Code)
	require.Less(t, time.Since(started), 10*time.Second)
	require.NoError(t, <-served)
}

func TestRunGoesAsTheRunnersUser(t *testing.T) {
	r := runner(t)
	r.Credential = &syscall.Credential{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()), NoSetGroups: true} //nolint:gosec // IDs fit in 32 bits
	conn, served := serve(t, r)

	run(t, conn, 1, "id -u")
	stdout, _, done := readRun(t, conn)
	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypeStop}))

	require.Equal(t, strconv.Itoa(os.Getuid())+"\n", stdout)
	require.Equal(t, 0, *done.Code)
	require.NoError(t, <-served)
}

func TestRunReportsABashThatCannotStart(t *testing.T) {
	r := runner(t)
	r.Bash = "/missing/bash"
	conn, served := serve(t, r)

	run(t, conn, 1, "true")
	_, _, done := readRun(t, conn)
	require.NoError(t, wire.Write(conn, wire.Message{Type: wire.TypeStop}))

	require.Equal(t, -1, *done.Code)
	require.Contains(t, done.Error, "no such file")
	require.NoError(t, <-served)
}

func TestRunFailsWhenTheSupervisorGoesAway(t *testing.T) {
	conn, served := serve(t, runner(t))

	run(t, conn, 1, "yes")
	_, err := wire.Read(conn)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	require.Error(t, <-served)
}

func TestServeRefusesFramesThatAreNotRequests(t *testing.T) {
	frame := func(kind byte, body string) []byte {
		return append(append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), kind), body...) //nolint:gosec // test bodies are short
	}
	cases := map[string][]byte{
		"output":                 frame(wire.KindStdout, "x"),
		"another type":           frame(wire.KindControl, `{"type":"ready"}`),
		"a frame it cannot read": frame(wire.KindControl, "{]"),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			conn, served := serve(t, runner(t))

			_, err := conn.Write(input)

			require.NoError(t, err)
			require.Error(t, <-served)
		})
	}
}

func TestServeFailsWhenItCannotSendReady(t *testing.T) {
	conn, supervisor := net.Pipe()
	require.NoError(t, supervisor.Close())

	err := worker.Serve(conn, runner(t))

	require.Error(t, err)
}

func TestServeEndsTheReadsOfAConnectionAfterItEnds(t *testing.T) {
	conn, served := serve(t, runner(t))

	var frames bytes.Buffer
	require.NoError(t, wire.Write(&frames, wire.Message{Type: wire.TypeStop}))
	require.NoError(t, wire.Write(&frames, wire.Message{Type: wire.TypeStop}))
	_, err := conn.Write(frames.Bytes())

	require.NoError(t, err)
	require.NoError(t, <-served)
}
