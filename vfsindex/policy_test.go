package vfsindex

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Unique normalize edges not exercised by mount integrations (whitespace / unknown).
func TestNormalizePolicy_whitespaceAndUnknown(t *testing.T) {
	if got := NormalizePolicy("  SELECTIVE "); got != PolicySelective {
		t.Fatalf("whitespace: %q", got)
	}
	if got := NormalizePolicy("unknown"); got != PolicySelective {
		t.Fatalf("unknown: %q", got)
	}
	if AutoIndex("  SELECTIVE ") {
		t.Fatal("selective is not AutoIndex")
	}
}

// TestBridge_policyAndTrack: Start wires policy; Track enables selective ShouldIndex.
func TestBridge_policyAndTrack(t *testing.T) {
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("br", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	br, err := Start(ms, eng, brain.Scope{Namespace: &ns}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = br.Close() })
	if br.PolicyAt("/work/a.txt") != PolicySelective {
		t.Fatalf("default policy: %s", br.PolicyAt("/work/a.txt"))
	}
	if br.ShouldIndex("/work/a.txt") {
		t.Fatal("selective without track")
	}
	br.Track("/work/a.txt")
	if !br.ShouldIndex("/work/a.txt") {
		t.Fatal("tracked path")
	}
	br.Untrack("/work/a.txt")
	if br.ShouldIndex("/work/a.txt") {
		t.Fatal("after untrack")
	}
}
