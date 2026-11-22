// Package wire is the frame format and the messages of the sandbox. The same
// frames go on both links: from the session runtime to the supervisor (one
// HTTP/2 stream per worker) and from the supervisor to a worker (a Unix
// socket pair). The Rust worker has the same format (sandbox/worker/src/frame.rs
// and message.rs).
//
// A frame is a 5-byte header and a body. The header is the body length as a
// big-endian uint32, then one kind byte. The only kind today is control: a
// JSON body with a "type" field.
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

const (
	headerSize  = 5
	kindControl = 1
)

// The message types. Ready and pong come from the worker, ping and stop
// from the client. Exit comes from the supervisor.
const (
	TypeReady = "ready"
	TypePing  = "ping"
	TypePong  = "pong"
	TypeStop  = "stop"
	TypeExit  = "exit"
)

// Message is one control message. Only the fields of its Type are set.
type Message struct {
	Type        string       `json:"type"`
	ID          uint64       `json:"id,omitzero"`
	Enforcement *Enforcement `json:"enforcement,omitempty"`
	Exit        *Exit        `json:"exit,omitempty"`
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

// Write writes m as one frame, in one write. It refuses a message whose
// body is larger than MaxBody, as Read does.
func Write(w io.Writer, m Message) error {
	body, _ := json.Marshal(m)
	length := len(body)
	if length < 0 || length > MaxBody {
		return fmt.Errorf("wire: message of %d bytes is larger than %d", length, MaxBody)
	}
	frame := make([]byte, 0, headerSize+length)
	frame = binary.BigEndian.AppendUint32(frame, uint32(length))
	frame = append(frame, kindControl)
	_, err := w.Write(append(frame, body...))
	return err
}

// Read reads one frame and decodes its message. It returns io.EOF when the
// stream ends before a frame starts.
func Read(r io.Reader) (Message, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Message{}, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length > MaxBody {
		return Message{}, fmt.Errorf("wire: frame body of %d bytes is larger than %d", length, MaxBody)
	}
	if header[4] != kindControl {
		return Message{}, fmt.Errorf("wire: frame kind %d is not known", header[4])
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Message{}, fmt.Errorf("wire: frame body ends early: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var m Message
	if err := decoder.Decode(&m); err != nil {
		return Message{}, fmt.Errorf("wire: frame body: %w", err)
	}
	return m, nil
}
