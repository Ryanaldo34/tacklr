package telemetry

import (
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// DefaultResource builds a shared Resource for TracerProvider and MeterProvider
// so Grafana/LGTM backends correlate service.name across Tempo, Mimir, and logs.
func DefaultResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	if strings.TrimSpace(serviceName) == "" {
		if v := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); v != "" {
			serviceName = v
		} else {
			serviceName = "tacklr"
		}
	}
	// NewSchemaless avoids Schema URL conflicts with resource.Default()'s SDK schema.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	return res, nil
}
