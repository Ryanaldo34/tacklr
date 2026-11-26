package vfs

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/rest"
)

// TestMapAPIErrors_turnsRepliesIntoVFSErrors: each Google and Graph error
// reply maps to the vfs error a caller can act on. A status with no vfs
// error keeps its HTTP status and message, and an error that is no reply
// passes through.
func TestMapAPIErrors_turnsRepliesIntoVFSErrors(t *testing.T) {
	t.Parallel()
	errNet := errors.New("connection refused")
	mappers := map[string]func(error) error{"drive": mapDriveError, "docs": mapDocsError, "graph": mapGraphError}
	for _, c := range []struct {
		status  int
		message string
		want    map[string]error // nil: the reply, with its status
	}{
		{http.StatusUnauthorized, "", map[string]error{"drive": ErrAuthExpired, "docs": ErrAuthExpired, "graph": ErrAuthExpired}},
		{http.StatusNotFound, "", map[string]error{"drive": ErrNotExist, "docs": ErrNotExist, "graph": ErrNotExist}},
		{http.StatusForbidden, "", map[string]error{"drive": ErrPermission, "docs": ErrPermission, "graph": ErrPermission}},
		{http.StatusForbidden, "DLP blocked", map[string]error{"drive": ErrPermission, "docs": ErrPermission, "graph": ErrPermission}},
		{http.StatusForbidden, "exportSizeLimitExceeded", map[string]error{"drive": ErrTooLarge, "docs": ErrPermission, "graph": ErrPermission}},
		{http.StatusBadRequest, "export size too large", map[string]error{"drive": ErrTooLarge}},
		{http.StatusConflict, "", map[string]error{"graph": ErrExist}},
		{http.StatusTooManyRequests, "slow down", nil},
	} {
		for name, mapErr := range mappers {
			reply := &rest.Error{Code: c.status, Message: c.message}

			got := mapErr(reply)

			want := c.want[name]
			if want != nil && !errors.Is(got, want) {
				t.Errorf("%s %d %q = %v, want %v", name, c.status, c.message, got, want)
			}
			if want == nil && (!errors.Is(got, reply) || !strings.Contains(got.Error(), reply.Error())) {
				t.Errorf("%s %d %q = %v, want the reply", name, c.status, c.message, got)
			}
		}
	}
	for name, mapErr := range mappers {
		if got := mapErr(errNet); got != errNet { //nolint:errorlint // the error passes through as is
			t.Errorf("%s of a network error = %v, want it as is", name, got)
		}
	}
}
