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

func mustFilter(t testing.TB, m map[string]any) Filter {
	t.Helper()
	f, err := DecodeFilter(m)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
