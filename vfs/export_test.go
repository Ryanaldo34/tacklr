package vfs

import "testing"

func mustTree(t *testing.T, members ...Member) *MountSession {
	t.Helper()
	ms, err := Tree(members...)(t.Context(), t.Name(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}
