package brain_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

// FuzzDecodeFilter: a filter from the tool boundary is either rejected as
// ErrInvalid or usable by every store (it compiles to SQL).
func FuzzDecodeFilter(f *testing.F) {
	for _, seed := range []string{
		`{}`,
		`{"kind":"Document","stage":"open"}`,
		`{"kind":["Document","Deal"],"amount":[1,2.5]}`,
		`{"updated_after":"2024-01-01","created_before":"2024-06-01T00:00:00Z"}`,
		`{"title":["a",""]}`,
		`{"due-date":"x"}`,
		`{"!!":"x"}`,
		`{"stage":{"nested":1}}`,
		`{"flag":true,"n":null}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if dec.Decode(&m) != nil {
			t.Skip()
		}

		filter, err := brain.DecodeFilter(m)
		if err != nil {
			if !errors.Is(err, brain.ErrInvalid) {
				t.Fatalf("DecodeFilter(%s) = %v, want ErrInvalid", raw, err)
			}
			return
		}
		if _, _, err := brain.FilterSQL(filter, 1); err != nil {
			t.Fatalf("DecodeFilter accepted %s, but FilterSQL = %v", raw, err)
		}
	})
}

// FuzzNamespaceKey: distinct valid namespaces never share a Key.
func FuzzNamespaceKey(f *testing.F) {
	f.Add("a", "x", "b", "y", "a", "x.y", "", "")
	f.Add("org", "acme", "", "", "org", "acme", "team", "x")
	f.Add("a=b", "c", "", "", "a", "b=c", "", "")
	f.Fuzz(func(t *testing.T, n1, v1, n2, v2, n3, v3, n4, v4 string) {
		parse := func(nv ...string) (brain.Namespace, bool) {
			if nv[2] == "" && nv[3] == "" {
				nv = nv[:2]
			}
			ns, err := brain.ParseNamespace(nv...)
			return ns, err == nil
		}
		a, okA := parse(n1, v1, n2, v2)
		b, okB := parse(n3, v3, n4, v4)
		if !okA || !okB {
			t.Skip()
		}

		if a.Key() != a.Clone().Key() {
			t.Fatalf("Key of %v is not stable", a)
		}
		if a.Equal(b) != (a.Key() == b.Key()) {
			t.Fatalf("namespaces %v and %v: equal %v, keys %q and %q", a, b, a.Equal(b), a.Key(), b.Key())
		}
	})
}
