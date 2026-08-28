package durable

import (
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMemoryCatalog_lookupDefaultAndIDs(t *testing.T) {
	var none *MemoryCatalog
	if _, ok := none.Lookup("x"); ok || none.DefaultID() != "" || none.IDs() != nil {
		t.Fatal("nil catalog")
	}
	cat := NewCatalog("default")
	if cat.DefaultID() != "default" {
		t.Fatalf("default=%q", cat.DefaultID())
	}
	model := &testkit.ScriptedModel{}
	cat.Register("default", AgentSpec{
		Name:    "Default",
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	cat.Register("other", AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	if spec, ok := cat.Lookup(""); !ok || spec.Name != "Default" {
		t.Fatalf("empty id should use default: %+v ok=%v", spec, ok)
	}
	ids := cat.IDs()
	if len(ids) != 2 || ids[0] != "default" || ids[1] != "other" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestMemoryCatalog_registerPanics(t *testing.T) {
	model := &testkit.ScriptedModel{}
	valid := AgentSpec{Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}}}
	cases := []struct {
		name string
		run  func()
	}{
		{"nil catalog", func() { (*MemoryCatalog)(nil).Register("a", valid) }},
		{"empty id", func() { NewCatalog("").Register("  ", valid) }},
		{"session id set", func() {
			s := valid
			s.Options.SessionID = "s"
			NewCatalog("").Register("a", s)
		}},
		{"mount session set", func() {
			s := valid
			s.Options.MountSession = &vfs.MountSession{}
			NewCatalog("").Register("a", s)
		}},
		{"skills session set", func() {
			s := valid
			s.Options.SkillsSession = &vfs.MountSession{}
			NewCatalog("").Register("a", s)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic")
				}
			}()
			tc.run()
		})
	}
}
