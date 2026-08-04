package brain_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestNewPostgresStore_requiresDB(t *testing.T) {
	if _, err := brain.NewPostgresStore(nil); err == nil {
		t.Fatal("want error")
	}
}
