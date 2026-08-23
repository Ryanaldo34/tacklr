package temporal

import (
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// PluginOption mutates Temporal OpenTelemetry v2 plugin options.
type PluginOption func(*temporalotel.PluginOptions)

// ObservabilityPlugin returns the Temporal OTEL v2 plugin. Default: context
// propagation only (no SDK auto-spans — SessionWorkflow is the instrumentor)
// plus Temporal SDK metrics on the process-wide MeterProvider installed by
// telemetry.Init. NewPlugin captures tracers and meters at construction, so
// Init must run first; this does not install a second provider.
func ObservabilityPlugin(opts ...PluginOption) (client.Plugin, error) {
	if !telemetry.IsReplaySafeProvider() {
		return nil, fmt.Errorf("durable/temporal: telemetry.Init must run before Dial so the process shares one ReplaySafe TracerProvider")
	}
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
// trace context from the process-wide OTel providers. Call telemetry.Init first.
func Dial(opts client.Options) (client.Client, error) {
	plugin, err := ObservabilityPlugin()
	if err != nil {
		return nil, err
	}
	opts.Plugins = append([]client.Plugin{plugin}, opts.Plugins...)
	return client.Dial(opts)
}
