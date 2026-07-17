package server

import (
	"context"
	"fmt"

	"github.com/ryanaldo34/tacklr"
)

func (s *Server) loadAgent(ctx context.Context, agentID, threadID string, load bool) (*tacklr.AgentHarness, *AgentSpec, error) {
	spec, err := s.provider.GetAgent(ctx, agentID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve agent: %w", err)
	}

	store := s.store
	if spec.Store != nil {
		store = spec.Store
	}

	var h *tacklr.AgentHarness
	if load {
		if store == nil {
			return nil, nil, clientErrorf(ErrSessionStoreNotConfigured, "session store is not configured")
		}
		h, err = tacklr.NewAgentHarnessFromSession(ctx, threadID, spec.Config, spec.Model, store, spec.WatchDog)
		if err != nil {
			return nil, nil, err
		}
	} else {
		h = tacklr.NewAgent(spec.Config, spec.Model, store, spec.WatchDog)
	}

	h.SessionId = threadID
	h.Tools = spec.Tools
	h.MCPConfigs = spec.MCPConfigs
	if spec.StreamingStrategy != nil {
		h.WithStreamingStrategy(spec.StreamingStrategy)
	}
	return h, &spec, nil
}
