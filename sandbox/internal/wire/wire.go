// Package wire is the frame format and the messages of the sandbox. The same
// frames go on both links: from the session runtime to the supervisor (one
// HTTP/2 stream per worker) and from the supervisor to a worker (a Unix
// socket pair). The Rust worker has the same format (sandbox/worker/src/frame.rs
// and message.rs).
//
// A frame is a 5-byte header and a body. The header is the body length as a
// big-endian uint32, then one kind byte: control (a JSON body with a "type"
// field), or the stdout or stderr bytes of a run.
package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// StreamPath is the URL path where a client opens a worker stream.
const StreamPath = "/v1/worker"

// MaxBody is the largest frame body.
const MaxBody = 1 << 20

const headerSize = 5

// The frame kinds.
const (
	KindControl byte = 1
	KindStdout  byte = 2
	KindStderr  byte = 3
)

// The message types. Ready, pong and done come from the worker; ping, stop
// and run from the client. Exit comes from the supervisor.
const (
	TypeReady = "ready"
	TypePing  = "ping"
	TypePong  = "pong"
	TypeStop  = "stop"
	TypeRun   = "run"
	TypeDone  = "done"
	TypeExit  = "exit"
)

// Message is one control message. Only the fields of its Type are set.
type Message struct {
	Type        string       `json:"type"`
	ID          uint64       `json:"id,omitzero"`
	Script      string       `json:"script,omitempty"`
	Code        *int         `json:"code,omitempty"`
	Error       string       `json:"error,omitempty"`
	Enforcement *Enforcement `json:"enforcement,omitempty"`
	Exit        *Exit        `json:"exit,omitempty"`
}

// Frame is one frame: a control message, or output bytes of a run.
type Frame struct {
	// Kind is KindControl, KindStdout or KindStderr.
	Kind byte
	// Message is the control message when Kind is KindControl.
	Message Message
	// Data is the output bytes otherwise.
	Data []byte
}

// Enforcement is what a worker enforces on itself.
type Enforcement struct {
	// LandlockABI is the Landlock ABI version of the worker's ruleset on
	// this kernel: 6 or more.
	LandlockABI int `json:"landlock_abi"`
}

// Exit is how a worker process ended.
type Exit struct {
	// Code is the exit code, or -1 when a signal ended the process.
	Code int `json:"code"`
	// Signal is the name of the signal that ended the process (SIGKILL,
	// SIGSYS), or empty.
	Signal string `json:"signal,omitempty"`
}

// Write writes m as one control frame, in one write.
func Write(w io.Writer, m Message) error {
	body, _ := json.Marshal(m)
	return write(w, KindControl, body)
}

// WriteData writes data as one frame of kind, in one write.
func WriteData(w io.Writer, kind byte, data []byte) error {
	return write(w, kind, data)
}

// write refuses a body larger than MaxBody, as Read does.
func write(w io.Writer, kind byte, body []byte) error {
	length := len(body)
	if length < 0 || length > MaxBody {
		return fmt.Errorf("wire: frame body of %d bytes is larger than %d", length, MaxBody)
	}
	var header [headerSize]byte
	binary.BigEndian.PutUint32(header[:4], uint32(length))
	header[4] = kind
	_, err := w.Write(append(header[:], body...))
	return err
}

// Read reads one frame, and decodes it if it is a control frame. It returns
// io.EOF when the stream ends before a frame starts.
func Read(r io.Reader) (Frame, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	length, kind := binary.BigEndian.Uint32(header[:4]), header[4]
	if length > MaxBody {
		return Frame{}, fmt.Errorf("wire: frame body of %d bytes is larger than %d", length, MaxBody)
	}
	if kind < KindControl || kind > KindStderr {
		return Frame{}, fmt.Errorf("wire: frame kind %d is not known", kind)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Frame{}, fmt.Errorf("wire: frame body ends early: %w", err)
	}
	if kind != KindControl {
		return Frame{Kind: kind, Data: body}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var m Message
	if err := decoder.Decode(&m); err != nil {
		return Frame{}, fmt.Errorf("wire: frame body: %w", err)
	}
	return Frame{Kind: kind, Message: m}, nil
}
