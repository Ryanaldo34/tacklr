package sandbox

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/vfs"
)

// The worker is not trusted: the server answers each request it cannot do
// with an errno, and writes back the files a run leaves open.
func TestServerAnswersRequestsItCannotDoAndWritesBackFilesLeftOpen(t *testing.T) {
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(dir)))(t.Context(), t.Name(), vfs.Request{})
	require.NoError(t, err)
	requests := make(chan []byte, 4)
	for _, r := range []wire.FSRequest{
		{ID: 1, Op: "read", Handle: 7},
		{ID: 2, Op: "open", Path: "/work/left.txt", Create: true, Write: true},
		{ID: 3, Op: "chmod", Handle: 1},
		{ID: 4, Op: "write", Handle: 1},
	} {
		encoded, err := json.Marshal(r)
		require.NoError(t, err)
		body := append(binary.BigEndian.AppendUint32(nil, uint32(len(encoded))), encoded...) //nolint:gosec // a request is short
		if r.Op == "write" {
			body = append(body, "kept"...)
		}
		requests <- body
	}
	close(requests)
	var replies []wire.FSReply
	files := &server{files: vfs.NewFiles(ms), open: map[uint64]*vfs.OpenFile{}}

	err = files.serve(t.Context(), requests, func(reply wire.FSReply, _ []byte) { replies = append(replies, reply) })

	require.NoError(t, err)
	require.Equal(t, []wire.FSReply{
		{ID: 1, Error: "EBADF"},
		{ID: 2, Handle: 1},
		{ID: 3, Error: "EIO"},
		{ID: 4, Size: 4},
	}, replies)
	left, err := os.ReadFile(filepath.Join(dir, "left.txt"))
	require.NoError(t, err)
	require.Equal(t, "kept", string(left))
}
