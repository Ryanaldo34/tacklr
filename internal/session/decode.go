package session

import "encoding/json"

func decodeAs[T any](raw any) (T, bool) {
	var zero T
	if v, ok := raw.(T); ok {
		return v, true
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return zero, false
	}
	var v T
	if json.Unmarshal(b, &v) != nil {
		return zero, false
	}
	return v, true
}
