// Package wire is the frame format and the messages of the sandbox. The same
// frames go on both links: from the session runtime to the supervisor (one
// HTTP/2 stream per worker) and from the supervisor to a worker (a Unix
// socket pair). The Rust worker has the same format (sandbox/worker/src/frame.rs
// and message.rs).
//
// A frame is a 5-byte header and a body. The header is the body length as a
// big-endian uint32, then one kind byte: control (a JSON body with a "type"
// field), the stdout or stderr bytes of a run, or a file request or reply
// (the JSON's length as a big-endian uint32, the JSON, then bytes).
package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/ryanaldo34/tacklr/vfs/mount"
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
	KindFS      byte = 4
)

// The message types. Ready, pong and done come from the worker; open, ping,
// stop and run from the client. Exit comes from the supervisor.
const (
	TypeOpen  = "open"
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
	Type        string          `json:"type"`
	ID          uint64          `json:"id,omitzero"`
	Script      string          `json:"script,omitempty"`
	Session     string          `json:"session,omitempty"`
	Bindings    []mount.Binding `json:"bindings,omitempty"`
	Code        *int            `json:"code,omitempty"`
	Error       string          `json:"error,omitempty"`
	Enforcement *Enforcement    `json:"enforcement,omitempty"`
	Exit        *Exit           `json:"exit,omitempty"`
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
	if kind < KindControl || kind > KindFS {
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

// FSRequest is a file request from a worker to its supervisor, during a run. Paths are below
// the workspace root, such as /work/a.txt.
type FSRequest struct {
	Type     string `json:"type"`
	ID       uint64 `json:"id"`
	Op       string `json:"op"`
	Path     string `json:"path,omitempty"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	Handle   uint64 `json:"handle,omitempty"`
	Offset   uint64 `json:"offset,omitempty"`
	Length   uint64 `json:"length,omitempty"`
	Size     uint64 `json:"size,omitempty"`
	Write    bool   `json:"write,omitempty"`
	Create   bool   `json:"create,omitempty"`
	Truncate bool   `json:"truncate,omitempty"`
	Append   bool   `json:"append,omitempty"`
}

// FSReply is the reply to an FSRequest.
type FSReply struct {
	Type     string    `json:"type"`
	ID       uint64    `json:"id"`
	Error    string    `json:"error,omitempty"`
	IsDir    bool      `json:"is_dir,omitempty"`
	Size     uint64    `json:"size,omitempty"`
	Modified uint64    `json:"modified,omitempty"`
	Entries  []FSEntry `json:"entries,omitempty"`
	Handle   uint64    `json:"handle,omitempty"`
}

// FSEntry is one entry of a directory.
type FSEntry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
}

// ParseFS decodes the body of a file frame from a worker: the request and
// the bytes after it.
func ParseFS(body []byte) (FSRequest, []byte, error) {
	if len(body) < 4 || int(binary.BigEndian.Uint32(body)) > len(body)-4 {
		return FSRequest{}, nil, errors.New("wire: the file frame is too short")
	}
	end := 4 + int(binary.BigEndian.Uint32(body))
	decoder := json.NewDecoder(bytes.NewReader(body[4:end]))
	decoder.DisallowUnknownFields()
	var request FSRequest
	if err := decoder.Decode(&request); err != nil {
		return FSRequest{}, nil, fmt.Errorf("wire: file request: %w", err)
	}
	return request, body[end:], nil
}

// WriteFS writes reply and data as one file frame, in one write.
func WriteFS(w io.Writer, reply FSReply, data []byte) error {
	reply.Type = "fs_reply"
	encoded, _ := json.Marshal(reply)
	body := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(encoded)+len(data)), uint32(len(encoded))) //nolint:gosec // a reply is far below 4 GiB
	body = append(append(body, encoded...), data...)
	return write(w, KindFS, body)
}
