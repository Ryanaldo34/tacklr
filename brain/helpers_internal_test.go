package brain

import "testing"

func mustNS(t testing.TB, nv ...string) Namespace {
	t.Helper()
	ns, err := ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}
