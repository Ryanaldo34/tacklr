package wire

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The frame parser reads bytes from a worker, which is not trusted. Each
// case is one refusal of the parser.
func TestReadRefusesFramesItCannotTrust(t *testing.T) {
	frame := func(length uint32, kind byte, body string) []byte {
		header := binary.BigEndian.AppendUint32(nil, length)
		return append(append(header, kind), body...)
	}
	cases := []struct {
		name  string
		input []byte
		err   string
	}{
		{"a body larger than the limit", frame(MaxBody+1, KindControl, ""), "wire: frame body of 1048577 bytes is larger than 1048576"},
		{"a kind it does not know", frame(2, 9, "{}"), "wire: frame kind 9 is not known"},
		{"a body that ends early", frame(10, KindControl, "{}"), "wire: frame body ends early: unexpected EOF"},
		{"a field it does not know", frame(26, KindControl, `{"type":"ready","extra":1}`), `wire: frame body: json: unknown field "extra"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Read(bytes.NewReader(c.input))

			require.EqualError(t, err, c.err)
		})
	}
}

func TestWriteRefusesAMessageLargerThanTheLimit(t *testing.T) {
	err := Write(io.Discard, Message{Type: strings.Repeat("x", MaxBody)})

	require.EqualError(t, err, "wire: frame body of 1048587 bytes is larger than 1048576")
}

func TestParseFSRefusesARequestItCannotDecode(t *testing.T) {
	body := binary.BigEndian.AppendUint32(nil, 2)

	_, _, err := ParseFS(append(body, "{]"...))

	require.ErrorContains(t, err, "wire: file request: invalid character")
}
