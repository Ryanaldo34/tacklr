package tacklr

import (
	"slices"
	"sync"
)

// onCallLayer is one completed OnCall middleware layer for a tool call.
type onCallLayer struct {
	Args   string
	Denied bool
}

type onCallStage struct {
	ToolCallID string `json:"toolCallID"`
	TypeName   string `json:"typeName"`
	Denied     bool   `json:"denied"`
	Args       string `json:"args"`
}

// onCallStore holds completed OnCall layers so a parked tool can re-enter
// without re-running constructors. It is a sessionManager module — not
// exposed on HarnessRuntime.
type onCallStore struct {
	mu     sync.RWMutex
	stages []onCallStage
}

// Get returns the completed layer for toolCallID and typeName.
func (s *onCallStore) Get(toolCallID, typeName string) (onCallLayer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := slices.IndexFunc(s.stages, func(st onCallStage) bool {
		return st.ToolCallID == toolCallID && st.TypeName == typeName
	})
	if i < 0 {
		return onCallLayer{}, false
	}
	return onCallLayer{Args: s.stages[i].Args, Denied: s.stages[i].Denied}, true
}

// Record stores a completed OnCall layer for later re-entry.
func (s *onCallStore) Record(toolCallID, typeName string, layer onCallLayer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = append(s.stages, onCallStage{
		ToolCallID: toolCallID,
		TypeName:   typeName,
		Denied:     layer.Denied,
		Args:       layer.Args,
	})
}
