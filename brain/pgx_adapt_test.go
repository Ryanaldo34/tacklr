package brain_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestAdaptPgx_nil(t *testing.T) {
	if brain.AdaptPgx(nil) != nil {
		t.Fatal("nil querier must yield nil DBTX")
	}
}
