package vfsindex_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}
