package brain_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestParseNamespace_rejectsInvalid: a namespace with no attrs, a name or
// value that is empty or holds a dot, a repeated name, or a name with a
// space is rejected.
func TestParseNamespace_rejectsInvalid(t *testing.T) {
	t.Parallel()
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
		if _, err := brain.ParseNamespace(kv...); err == nil {
			t.Errorf("parse %v succeeded, want an error", kv)
		}
	}
	if err := (brain.Namespace{{Name: " org", Value: "acme"}}).Validate(); err == nil {
		t.Error("a name with a leading space is valid, want an error")
	}
}

// TestNamespace_roundTripsThroughJSONAndSQL: a namespace comes back equal from
// JSON and from a SQL value; an empty or NULL column scans as empty, and a
// column of another type fails.
func TestNamespace_roundTripsThroughJSONAndSQL(t *testing.T) {
	t.Parallel()
	ns := storetest.Namespace(t, "org", "acme", "workspace", "west")
	raw, err := json.Marshal(ns)
	if err != nil {
		t.Fatal(err)
	}
	v, err := ns.Value()
	if err != nil {
		t.Fatal(err)
	}

	var fromJSON, scanned, empty, null, wrong brain.Namespace
	jsonErr := json.Unmarshal(raw, &fromJSON)
	scanErr := scanned.Scan(v)
	emptyErr := empty.Scan([]byte("[]"))
	nullErr := null.Scan(nil)
	wrongErr := wrong.Scan(42)

	if jsonErr != nil || !fromJSON.Equal(ns) || scanErr != nil || !scanned.Equal(ns) {
		t.Errorf("json %s = %v %v, scan = %v %v, want %v", raw, fromJSON, jsonErr, scanned, scanErr, ns)
	}
	if emptyErr != nil || !empty.Empty() || nullErr != nil || !null.Empty() {
		t.Errorf("scan [] = %v %v, scan NULL = %v %v, want empty", empty, emptyErr, null, nullErr)
	}
	if wrongErr == nil {
		t.Error("scan an int succeeded, want an error")
	}
}

// TestNamespace_bindNarrowsTheHostCeiling: a call namespace adds attrs to the
// host ceiling or repeats them; an empty call keeps the ceiling; a call that
// changes a ceiling attr, an invalid call or an invalid ceiling fails.
func TestNamespace_bindNarrowsTheHostCeiling(t *testing.T) {
	t.Parallel()
	org := storetest.Namespace(t, "org", "acme")

	narrowed, narrowErr := org.Bind(storetest.Namespace(t, "org", "acme", "workspace", "west"))
	kept, keptErr := org.Bind(nil)
	_, otherErr := org.Bind(storetest.Namespace(t, "org", "other"))
	_, badCallErr := org.Bind(brain.Namespace{{Name: "workspace"}})
	_, badCeilingErr := brain.Namespace{}.Bind(org)

	if narrowErr != nil || !narrowed.Equal(storetest.Namespace(t, "org", "acme", "workspace", "west")) {
		t.Errorf("bind a workspace = %v %v, want the org's workspace", narrowed, narrowErr)
	}
	if keptErr != nil || !kept.Equal(org) {
		t.Errorf("bind nothing = %v %v, want the ceiling", kept, keptErr)
	}
	if !errors.Is(otherErr, brain.ErrInvalid) || badCallErr == nil || badCeilingErr == nil {
		t.Errorf("bind another org = %v, an invalid call = %v, under no ceiling = %v, want errors", otherErr, badCallErr, badCeilingErr)
	}
}
