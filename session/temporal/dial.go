package temporal

import (
	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"google.golang.org/grpc"
)

// Dial is client.Dial that ignores DNS-provided gRPC service configs and,
// unless opts has a metrics handler of its own, reports the Temporal SDK's
// metrics (workflow task and activity latencies, poll counts, failures) to
// the process-wide MeterProvider: set it up (export.Init) before Dial. A
// worker built from the client reports them too.
//
// A turn's trace needs nothing from Temporal: the wake that starts it and
// each step carry the trace's context themselves, so the plugin's header
// propagation and its own spans are not needed.
func Dial(opts client.Options) (client.Client, error) {
	// gRPC otherwise looks up a DNS TXT record for every host name it
	// dials; a slow lookup can outlast the SDK's connect check. The SDK's
	// own default service config still applies.
	opts.ConnectionOptions.DialOptions = append(opts.ConnectionOptions.DialOptions, grpc.WithDisableServiceConfig())
	if opts.MetricsHandler == nil {
		opts.MetricsHandler = temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
			UseMonotonicCounters: true,
			// A meter that refuses an instrument loses that metric, not the
			// client: OpenTelemetry's error handler reports it.
			OnError: otel.Handle,
		})
	}
	return client.Dial(opts)
}
