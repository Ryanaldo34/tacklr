package temporal

import (
	"log/slog"

	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// PluginOption mutates Temporal OpenTelemetry v2 plugin options.
type PluginOption func(*temporalotel.PluginOptions)

// ObservabilityPlugin returns the Temporal OTEL v2 plugin. Default: context
// propagation only (no SDK auto-spans — SessionWorkflow is the instrumentor)
// plus Temporal SDK metrics on the global MeterProvider.
//
// Requires a ReplaySafe tracer provider (telemetry.Init or EnsureReplaySafeProvider).
func ObservabilityPlugin(opts ...PluginOption) (client.Plugin, error) {
	telemetry.EnsureReplaySafeProvider()
	o := temporalotel.PluginOptions{
		MetricsHandlerOptions: &temporalotel.MetricsHandlerOptions{
			UseMonotonicCounters: true,
			OnError: func(err error) {
				slog.Error("temporal otel metrics", "error", err)
			},
		},
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return temporalotel.NewPlugin(o)
}

// Dial is client.Dial with ObservabilityPlugin prepended so workers inherit
// trace context propagation.
func Dial(opts client.Options) (client.Client, error) {
	plugin, err := ObservabilityPlugin()
	if err != nil {
		return nil, err
	}
	opts.Plugins = append([]client.Plugin{plugin}, opts.Plugins...)
	return client.Dial(opts)
}
