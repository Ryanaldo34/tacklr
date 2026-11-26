package adapter_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
)

// TestEncodeState_keepsWhatJSONHoldsAndRefusesTheRest: state comes back as
// JSON gives it (numbers as float64); no state is nil; an empty key and a
// value JSON cannot hold are invalid.
func TestEncodeState_keepsWhatJSONHoldsAndRefusesTheRest(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		state map[string]any
		want  map[string]any
		err   error
	}{
		"values":       {map[string]any{"tier": "pro", "n": 3}, map[string]any{"tier": "pro", "n": 3.0}, nil},
		"none":         {nil, nil, nil},
		"an empty key": {map[string]any{"": 1}, nil, session.ErrInvalid},
		"a func value": {map[string]any{"f": func() {}}, nil, session.ErrInvalid},
	} {
		// Act
		got, err := adapter.EncodeState(tc.state)

		// Assert
		if !errors.Is(err, tc.err) || (tc.err == nil) != (err == nil) || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: EncodeState = %v, %v; want %v, %v", name, got, err, tc.want, tc.err)
		}
	}
}
