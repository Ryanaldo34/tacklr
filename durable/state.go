package durable

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/ryanaldo34/tacklr"
)

// EncodeUserState JSON-roundtrips host session state so checkpoint types are
// stable (numbers become float64) and non-serializable values fail now.
func EncodeUserState(state map[string]any) (map[string]any, error) {
	if len(state) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(state))
	for key, value := range state {
		if key == "" {
			return nil, fmt.Errorf("%w: session state key must not be empty", tacklr.ErrInvalid)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("%w: session state %q: %w", tacklr.ErrInvalid, key, err)
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("%w: session state %q: %w", tacklr.ErrInvalid, key, err)
		}
		out[key] = decoded
	}
	return out, nil
}

// MergeUserState copies overlay onto a clone of base. Overlay wins on conflict.
func MergeUserState(base, overlay map[string]any) map[string]any {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := maps.Clone(base)
	if out == nil {
		out = make(map[string]any, len(overlay))
	}
	maps.Copy(out, overlay)
	return out
}
