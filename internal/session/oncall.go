package session

import (
	"slices"
	"sync"

	"github.com/ryanaldo34/tacklr/internal/codec"
)

const onCallStagesKey = "_on_call_stages"

func init() {
	reserveStateKeys(onCallStagesKey)
}

// OnCallLayer is one completed OnCall middleware layer for a tool call.
type OnCallLayer struct {
	Args   string
	Denied bool
}

type onCallStage struct {
	ToolCallID string `json:"toolCallID"`
	TypeName   string `json:"typeName"`
	Denied     bool   `json:"denied"`
	Args       string `json:"args"`
}

// OnCallStore holds completed OnCall layers so a parked tool can re-enter
// without re-running constructors. It is a SessionManager module — not
// exposed on HarnessRuntime.
type OnCallStore struct {
	mu     sync.RWMutex
	stages []onCallStage
}

// Get returns the completed layer for toolCallID and typeName.
func (s *OnCallStore) Get(toolCallID, typeName string) (OnCallLayer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := slices.IndexFunc(s.stages, func(st onCallStage) bool {
		return st.ToolCallID == toolCallID && st.TypeName == typeName
	})
	if i < 0 {
		return OnCallLayer{}, false
	}
	return OnCallLayer{Args: s.stages[i].Args, Denied: s.stages[i].Denied}, true
}

// Record stores a completed OnCall layer for later re-entry.
func (s *OnCallStore) Record(toolCallID, typeName string, layer OnCallLayer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = append(s.stages, onCallStage{
		ToolCallID: toolCallID,
		TypeName:   typeName,
		Denied:     layer.Denied,
		Args:       layer.Args,
	})
}

func (s *OnCallStore) exportInto(state map[string]any) {
	s.mu.RLock()
	stages := slices.Clone(s.stages)
	s.mu.RUnlock()
	if len(stages) == 0 {
		delete(state, onCallStagesKey)
		return
	}
	state[onCallStagesKey] = stages
}

func (s *OnCallStore) loadFromState(state map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = nil
	if state == nil {
		return
	}
	raw, ok := state[onCallStagesKey]
	if !ok || raw == nil {
		return
	}
	s.stages = decodeOnCallStages(raw)
}

func decodeOnCallStages(raw any) []onCallStage {
	recs, ok := codec.As[[]onCallStage](raw)
	if !ok || len(recs) == 0 {
		return nil
	}
	return slices.Clone(recs)
}
