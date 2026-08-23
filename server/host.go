package server

import (
	"github.com/ryanaldo34/tacklr/durable"
)

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
