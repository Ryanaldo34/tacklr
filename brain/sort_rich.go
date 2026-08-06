package brain

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// SortRichObjects sorts objects in place by a well-known or property key.
// Keys: "updated_at", "created_at", "title", "position", or a property name.
func SortRichObjects(objects []RichObject, key string, desc bool) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" || len(objects) < 2 {
		return
	}
	slices.SortFunc(objects, func(a, b RichObject) int {
		c := compareRich(a, b, key)
		if desc {
			return -c
		}
		return c
	})
}

func compareRich(a, b RichObject, key string) int {
	switch key {
	case "updated_at":
		return a.UpdatedAt.Compare(b.UpdatedAt)
	case "created_at":
		return a.CreatedAt.Compare(b.CreatedAt)
	case "title":
		return strings.Compare(a.Title, b.Title)
	case "position":
		return cmp.Compare(posOrNeg(a.Position), posOrNeg(b.Position))
	default:
		return strings.Compare(propSortKey(a.Properties, key), propSortKey(b.Properties, key))
	}
}

func posOrNeg(p *int) int {
	if p == nil {
		return -1
	}
	return *p
}

func propSortKey(props map[string]any, key string) string {
	if props == nil {
		return ""
	}
	v, ok := props[key]
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case bool:
		if x {
			return "1"
		}
		return "0"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return ""
	}
}
