package vfs

import (
	"errors"
	"strings"
	"testing"
)

func TestFuseProjection_attachFailure(t *testing.T) {
	ms := mustTree(t, At("work", Local(t.TempDir())))

	err := FuseProjection{}.Attach(ms, `sess/with\slash`)
	if err != nil && !errors.Is(err, ErrFuseNotMounted) && !strings.Contains(err.Error(), "fuse") {
		t.Fatalf("attach err = %v", err)
	}
	if err != nil {
		return
	}
	for _, id := range []string{"", ".", ".."} {
		_ = FuseProjection{}.Attach(ms, id)
	}
}
