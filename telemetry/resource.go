package telemetry

import (
	"strings"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// DefaultResource builds a shared Resource for TracerProvider and MeterProvider
// so backends can correlate service.name across traces, metrics, and logs.
// Empty serviceName becomes "tacklr".
func DefaultResource(serviceName, serviceVersion string) *resource.Resource {
	if strings.TrimSpace(serviceName) == "" {
		serviceName = "tacklr"
	}
	res, _ := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	return res
}
