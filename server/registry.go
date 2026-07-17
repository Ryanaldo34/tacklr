package server

import (
	"context"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
)

type AgentSpec struct {
	Config            tacklr.Config
	Model             tacklr.InferenceStrategy
	Tools             []*tacklr.Tool
	MCPConfigs        []mcp.MCPConfig
	WatchDog          tacklr.AgentWatchDog
	StreamingStrategy tacklr.StreamingStrategy
	Store             stores.BaseStore
}

type AgentProvider interface {
	GetAgent(ctx context.Context, agentID string) (AgentSpec, error)
}

type Registry struct {
	agents map[string]AgentSpec
}

func NewRegistry() *Registry {
	return &Registry{agents: make(map[string]AgentSpec)}
}

func (r *Registry) Register(agentID string, spec AgentSpec) {
	r.agents[agentID] = spec
}

func (r *Registry) GetAgent(ctx context.Context, agentID string) (AgentSpec, error) {
	spec, ok := r.agents[agentID]
	if !ok {
		return AgentSpec{}, clientErrorf(ErrAgentNotFound, "agent %q not found", agentID)
	}
	return spec, nil
}
