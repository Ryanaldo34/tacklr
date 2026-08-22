package server

import (
	"context"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func catalogDefault(cat durable.Catalog) string {
	if cat == nil {
		return ""
	}
	return cat.DefaultID()
}

func catalogHasAgent(cat durable.Catalog, id string) bool {
	if cat == nil {
		return false
	}
	_, ok := cat.Lookup(id)
	return ok
}

func catalogAgentModel(cat durable.Catalog, agentID string) tacklr.InferenceStrategy {
	if cat == nil {
		return nil
	}
	spec, ok := cat.Lookup(agentID)
	if !ok {
		return nil
	}
	return spec.Options.Model
}

func catalogConfigOptions(cat durable.Catalog, currentAgent string) []ConfigOption {
	if cat == nil {
		return nil
	}
	if currentAgent == "" {
		currentAgent = cat.DefaultID()
	}
	ids := cat.IDs()
	opts := make([]ConfigOptionValue, 0, len(ids))
	for _, id := range ids {
		spec, _ := cat.Lookup(id)
		name := spec.Name
		if name == "" {
			name = id
		}
		opts = append(opts, ConfigOptionValue{Value: id, Name: name})
	}
	return []ConfigOption{
		{
			ID:           "model",
			Name:         "Agent",
			Description:  "Select which registered agent handles this session",
			Category:     "model",
			Type:         "select",
			CurrentValue: currentAgent,
			Options:      opts,
		},
	}
}

func recordSessionCreated(ctx context.Context) {
	telemetry.MustInstruments(telemetry.Meter()).RecordSessionCreated(ctx)
}
