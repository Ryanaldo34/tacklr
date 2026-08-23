package temporal

import (
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
)

// Dial is client.Dial with Temporal's OpenTelemetry v2 plugin prepended.
// Call telemetry.Init first so the global TracerProvider is ReplaySafe;
// NewPlugin reads otel.GetTracerProvider() and otel.GetMeterProvider().
// Default: context propagation only (no SDK auto-spans). SessionWorkflow
// starts tacklr.turn via temporalotel.Tracer.
func Dial(opts client.Options) (client.Client, error) {
	plugin, _ := temporalotel.NewPlugin(temporalotel.PluginOptions{})
	opts.Plugins = append([]client.Plugin{plugin}, opts.Plugins...)
	return client.Dial(opts)
}
