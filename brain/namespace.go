package brain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Attr is one named isolation dimension (org, workspace, …).
// Name and Value must be non-empty and must not contain '.'.
type Attr struct {
	Name  string `json:"name" desc:"Attribute name (e.g. org, workspace)."`
	Value string `json:"value" desc:"Attribute value. Must not contain '.'."`
}

// Namespace is ordered named isolation attrs. The first attr is the tenant:
// the boundary every store filters on by equality, so no scope ever reaches
// another tenant's rows. Every Scope needs a namespace; an empty one covers
// nothing.
type Namespace []Attr

// Empty reports whether n has no attributes.
func (n Namespace) Empty() bool { return len(n) == 0 }

// Key is a canonical, collision-free encoding of n (names and values), for
// deriving stable ids. Unlike String, {a=x.y} and {a=x, b=y} differ.
func (n Namespace) Key() string {
	var b strings.Builder
	for _, a := range n {
		b.WriteString(strconv.Quote(a.Name))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(a.Value))
		b.WriteByte(';')
	}
	return b.String()
}

// String joins attribute values with "." for display.
func (n Namespace) String() string {
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

// Covers reports whether obj is visible under this scope: both have the same
// tenant (first attr), and every other scope attr is on obj with the same
// value. An empty scope covers nothing.
func (scope Namespace) Covers(obj Namespace) bool {
	if len(scope) == 0 || len(obj) == 0 || scope[0] != obj[0] {
		return false
	}
	for _, a := range scope[1:] {
		if !slices.Contains(obj, a) {
			return false
		}
	}
	return true
}

// Tenant is n's first attr as a namespace of its own (nil for an empty n).
func (n Namespace) Tenant() Namespace {
	return n[:min(len(n), 1)]
}

// Bind merges a per-call namespace onto this host ceiling: the widest
// namespace the host lets a caller reach. The ceiling is required. Call may
// add attrs to narrow it. A call value that disagrees with a ceiling attr is
// invalid.
func (ceiling Namespace) Bind(call Namespace) (Namespace, error) {
	if err := ceiling.Validate(); err != nil {
		return nil, fmt.Errorf("host ceiling: %w", err)
	}
	if len(call) > 0 {
		if err := call.Validate(); err != nil {
			return nil, err
		}
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
// Every failure wraps ErrInvalid.
func (n Namespace) Validate() error {
	if len(n) == 0 {
		return fmt.Errorf("%w: namespace is required", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(n))
	for i, a := range n {
		name := strings.TrimSpace(a.Name)
		val := strings.TrimSpace(a.Value)
		switch {
		case name == "":
			return fmt.Errorf("%w: namespace attr %d: name is required", ErrInvalid, i)
		case val == "":
			return fmt.Errorf("%w: namespace attr %q: value is required", ErrInvalid, name)
		case strings.Contains(name, ".") || strings.Contains(val, "."):
			return fmt.Errorf("%w: namespace attr %q: name and value must not contain '.'", ErrInvalid, name)
		case name != a.Name || val != a.Value:
			return fmt.Errorf("%w: namespace attr %q: name and value must not have surrounding space", ErrInvalid, name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("%w: namespace attr %q is duplicated", ErrInvalid, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// ParseNamespace builds a Namespace from name, value, name, value, ….
func ParseNamespace(nameValues ...string) (Namespace, error) {
	if len(nameValues)%2 != 0 {
		return nil, fmt.Errorf("%w: namespace name/value pairs are uneven", ErrInvalid)
	}
	ns := make(Namespace, 0, len(nameValues)/2)
	for i := 0; i+1 < len(nameValues); i += 2 {
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

// Value implements driver.Valuer (jsonb array of {name,value}, as text).
func (n Namespace) Value() (driver.Value, error) {
	b, err := json.Marshal(n)
	return string(b), err
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
	return json.Unmarshal(b, n)
}
