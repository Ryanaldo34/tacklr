package adapter

import (
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/session"
)

// EncodeState JSON-roundtrips host session state so checkpoint types are
// stable (numbers become float64) and values JSON cannot hold fail now.
func EncodeState(state map[string]any) (map[string]any, error) {
	if len(state) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(state))
	for key, value := range state {
		if key == "" {
			return nil, fmt.Errorf("%w: session state key must not be empty", session.ErrInvalid)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("%w: session state %q: %w", session.ErrInvalid, key, err)
		}
		var decoded any
		_ = json.Unmarshal(raw, &decoded)
		out[key] = decoded
	}
	return out, nil
}
