package temporal

import (
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
)

// Dial is client.Dial that ignores DNS-provided gRPC service configs.
// Tracing is the host's to set up: add Temporal's OpenTelemetry plugin to
// opts.Plugins (see docs/session.md); a worker built from this client gets
// it too.
func Dial(opts client.Options) (client.Client, error) {
	// gRPC otherwise looks up a DNS TXT record for every host name it
	// dials; a slow lookup can outlast the SDK's connect check. The SDK's
	// own default service config still applies.
	opts.ConnectionOptions.DialOptions = append(opts.ConnectionOptions.DialOptions, grpc.WithDisableServiceConfig())
	return client.Dial(opts)
}
