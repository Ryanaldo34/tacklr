package brain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Attr is one named isolation dimension (org, workspace, …).
// Name and Value must be non-empty and must not contain '.'.
type Attr struct {
	Name  string `json:"name" desc:"Attribute name (e.g. org, workspace)."`
	Value string `json:"value" desc:"Attribute value. Must not contain '.'."`
}

// Namespace is ordered named isolation attrs. Empty Scope means no isolation.
// Covers: every scope attr is present on the object with the same value.
type Namespace []Attr

// Empty reports whether n has no attributes (no isolation when used as a Scope).
func (n Namespace) Empty() bool { return len(n) == 0 }

// String joins attribute values with ".".
func (n Namespace) String() string {
	if len(n) == 0 {
		return ""
	}
	parts := make([]string, len(n))
	for i, a := range n {
		parts[i] = a.Value
	}
	return strings.Join(parts, ".")
}

// Clone returns a copy of n.
func (n Namespace) Clone() Namespace { return slices.Clone(n) }

// Equal reports whether n and o have the same attrs in the same order.
func (n Namespace) Equal(o Namespace) bool { return slices.Equal(n, o) }

// Covers reports whether obj is visible under this scope.
// Empty scope covers all.
func (scope Namespace) Covers(obj Namespace) bool {
	if len(scope) == 0 {
		return true
	}
	om := make(map[string]string, len(obj))
	for _, a := range obj {
		om[a.Name] = a.Value
	}
	for _, a := range scope {
		if om[a.Name] != a.Value {
			return false
		}
	}
	return true
}

// Bind merges a per-call namespace onto this host ceiling.
// Call may add attrs. A call value that disagrees with a ceiling attr is invalid.
func (ceiling Namespace) Bind(call Namespace) (Namespace, error) {
	if len(call) > 0 {
		if err := call.Validate(); err != nil {
			return nil, err
		}
	}
	if ceiling.Empty() {
		return call.Clone(), nil
	}
	if call.Empty() {
		return ceiling.Clone(), nil
	}
	have := make(map[string]string, len(ceiling))
	for _, a := range ceiling {
		have[a.Name] = a.Value
	}
	out := ceiling.Clone()
	for _, a := range call {
		if v, ok := have[a.Name]; ok {
			if v != a.Value {
				return nil, fmt.Errorf("%w: namespace attr %q is outside host scope", ErrInvalid, a.Name)
			}
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

// Validate checks names, values, uniqueness, and that n is non-empty.
func (n Namespace) Validate() error {
	if len(n) == 0 {
		return fmt.Errorf("brain: namespace is required")
	}
	seen := make(map[string]struct{}, len(n))
	for i, a := range n {
		name := strings.TrimSpace(a.Name)
		val := strings.TrimSpace(a.Value)
		if name == "" {
			return fmt.Errorf("brain: namespace attr %d: name is required", i)
		}
		if val == "" {
			return fmt.Errorf("brain: namespace attr %q: value is required", name)
		}
		if strings.Contains(name, ".") || strings.Contains(val, ".") {
			return fmt.Errorf("brain: namespace attr %q: name and value must not contain '.'", name)
		}
		if name != a.Name || val != a.Value {
			return fmt.Errorf("brain: namespace attr %q: name and value must not have surrounding space", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("brain: namespace attr %q is duplicated", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// ParseNamespace builds a Namespace from name, value, name, value, ….
func ParseNamespace(nameValues ...string) (Namespace, error) {
	if len(nameValues) == 0 {
		return nil, fmt.Errorf("brain: namespace is required")
	}
	if len(nameValues)%2 != 0 {
		return nil, fmt.Errorf("brain: namespace name/value pairs are uneven")
	}
	ns := make(Namespace, 0, len(nameValues)/2)
	for i := 0; i < len(nameValues); i += 2 {
		ns = append(ns, Attr{
			Name:  strings.TrimSpace(nameValues[i]),
			Value: strings.TrimSpace(nameValues[i+1]),
		})
	}
	if err := ns.Validate(); err != nil {
		return nil, err
	}
	return ns, nil
}

// Value implements driver.Valuer (jsonb array of {name,value}).
func (n Namespace) Value() (driver.Value, error) {
	if n == nil {
		return []byte("[]"), nil
	}
	return json.Marshal(n)
}

// Scan implements sql.Scanner.
func (n *Namespace) Scan(src any) error {
	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	case nil:
		*n = nil
		return nil
	default:
		return fmt.Errorf("brain: cannot scan %T into Namespace", src)
	}
	if len(b) == 0 {
		*n = nil
		return nil
	}
	return json.Unmarshal(b, n)
}
