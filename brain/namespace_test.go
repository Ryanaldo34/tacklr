package brain

import (
	"encoding/json"
	"testing"
)

func TestNamespace_stringMapValidate(t *testing.T) {
	ns := MustNamespace("org", "acme", "workspace", "west")
	if ns.String() != "acme.west" {
		t.Fatalf("String = %q", ns.String())
	}
	if err := ns.Validate(); err != nil {
		t.Fatal(err)
	}
	if ns.Empty() {
		t.Fatal("non-empty")
	}
	if !ns.Equal(ns.Clone()) {
		t.Fatal("clone")
	}
}

func TestNamespace_coversHierarchicalRLS(t *testing.T) {
	obj := MustNamespace("org", "acme", "workspace", "west", "project", "alpha")
	org := MustNamespace("org", "acme")
	ws := MustNamespace("org", "acme", "workspace", "west")
	exact := MustNamespace("org", "acme", "workspace", "west", "project", "alpha")
	otherOrg := MustNamespace("org", "other")
	otherWS := MustNamespace("org", "acme", "workspace", "east")
	wsOnly := MustNamespace("workspace", "west")

	var open Namespace
	if !open.Covers(obj) {
		t.Fatal("empty scope covers all")
	}
	if !org.Covers(obj) || !ws.Covers(obj) || !exact.Covers(obj) {
		t.Fatal("prefix attrs must cover")
	}
	if otherOrg.Covers(obj) || otherWS.Covers(obj) {
		t.Fatal("wrong attr value must not cover")
	}
	if !wsOnly.Covers(obj) {
		t.Fatal("named subset (workspace only) must cover")
	}
	if org.Covers(MustNamespace("org", "acme2")) {
		t.Fatal("acme must not cover acme2")
	}
	if exact.Covers(org) {
		t.Fatal("more-specific scope must not cover a coarser object")
	}

	got, err := org.Bind(MustNamespace("workspace", "west"))
	if err != nil || !got.Equal(ws) {
		t.Fatalf("bind extra: %v %v", got, err)
	}
	got, err = org.Bind(nil)
	if err != nil || !got.Equal(org) {
		t.Fatalf("bind empty call: %v %v", got, err)
	}
	if _, err := org.Bind(MustNamespace("org", "other")); err == nil {
		t.Fatal("bind must reject a conflicting ceiling attr")
	}
}

func TestParseNamespace_rejectsInvalid(t *testing.T) {
	cases := [][]string{
		{},
		{"org"},
		{"", "acme"},
		{"org", ""},
		{"org", "ac.me"},
		{"o.rg", "acme"},
		{"org", "acme", "org", "other"},
	}
	for _, kv := range cases {
		if _, err := ParseNamespace(kv...); err == nil {
			t.Fatalf("accepted %v", kv)
		}
	}
	spaced := Namespace{{Name: " org", Value: "acme"}}
	if err := spaced.Validate(); err == nil {
		t.Fatal("leading space")
	}
}

func TestNamespace_jsonSQLRoundTrip(t *testing.T) {
	ns := MustNamespace("org", "acme", "workspace", "west")
	raw, err := json.Marshal(ns)
	if err != nil {
		t.Fatal(err)
	}
	var got Namespace
	if err := json.Unmarshal(raw, &got); err != nil || !got.Equal(ns) {
		t.Fatalf("json %s: %v %v", raw, got, err)
	}
	v, err := ns.Value()
	if err != nil {
		t.Fatal(err)
	}
	var scanned Namespace
	if err := scanned.Scan(v); err != nil || !scanned.Equal(ns) {
		t.Fatalf("scan: %v %v", scanned, err)
	}
	if err := scanned.Scan([]byte("[]")); err != nil || !scanned.Empty() {
		t.Fatalf("empty scan: %v %v", scanned, err)
	}
	if err := scanned.Scan(nil); err != nil {
		t.Fatal(err)
	}
	if err := scanned.Scan(42); err == nil {
		t.Fatal("bad type")
	}
}
