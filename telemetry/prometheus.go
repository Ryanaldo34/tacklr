package telemetry

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// MeterProviderFromPrometheusRegisterer builds a MeterProvider that records into
// reg for classic Prometheus scrape (host serves GET /metrics via promhttp).
//
// The host owns the HTTP server and scrape URL, for example:
//
//	reg := prometheus.NewRegistry()
//	mp, err := telemetry.MeterProviderFromPrometheusRegisterer(reg, "my-agent", "")
//	// server.NewRegistry(..., server.WithMeterProvider(mp))
//	// http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
//
// serviceName/serviceVersion set the same resource attributes as OTLP Init.
func MeterProviderFromPrometheusRegisterer(reg prometheus.Registerer, serviceName, serviceVersion string) (*sdkmetric.MeterProvider, error) {
	if reg == nil {
		return nil, fmt.Errorf("telemetry: prometheus registerer is nil")
	}
	res, err := DefaultResource(serviceName, serviceVersion)
	if err != nil {
		return nil, err
	}
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	), nil
}
