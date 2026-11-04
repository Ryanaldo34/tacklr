// Package httprange is the HTTP Range handling the vfs SDK adapters share:
// n bytes from off, where n < 0 reads to the end and n is never 0.
package httprange

import (
	"fmt"
	"io"
	"net/http"
)

// Header is the HTTP Range value for n bytes from off; "" asks for the
// whole object.
func Header(off, n int64) string {
	switch {
	case n < 0 && off == 0:
		return ""
	case n < 0:
		return fmt.Sprintf("bytes=%d-", off)
	default:
		return fmt.Sprintf("bytes=%d-%d", off, off+n-1)
	}
}

// Body returns resp's body, which must be the range asked for.
func Body(resp *http.Response, off, n int64) (io.ReadCloser, error) {
	if Header(off, n) != "" && resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("vfs: range %s answered with HTTP %d", Header(off, n), resp.StatusCode)
	}
	return resp.Body, nil
}
