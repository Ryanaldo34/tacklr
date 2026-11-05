package durableobject

import (
	"maps"
	"slices"
	"strings"
	"sync"
)

// MemStorage is a Storage in memory, for tests and local runs.
type MemStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage {
	return &MemStorage{m: map[string][]byte{}}
}

// Get implements Storage.
func (s *MemStorage) Get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	return slices.Clone(v), ok, nil
}

// Scan implements Storage.
func (s *MemStorage) Scan(prefix, after string, limit int) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.m {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]Entry, len(keys))
	for i, k := range keys {
		out[i] = Entry{Key: k, Value: slices.Clone(s.m[k])}
	}
	return out, nil
}

// Write implements Storage.
func (s *MemStorage) Write(ops ...Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range ops {
		if op.Delete {
			delete(s.m, op.Key)
			continue
		}
		s.m[op.Key] = slices.Clone(op.Value)
	}
	return nil
}

// Keys is every key the storage holds, in order.
func (s *MemStorage) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.m))
}
