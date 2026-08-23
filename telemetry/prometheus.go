package telemetry

import (
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
//	mp := telemetry.MeterProviderFromPrometheusRegisterer(reg, "my-agent", "")
//	// inprocess.New(catalog) / temporal.NewWorker(...)
//	// http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
//
// serviceName/serviceVersion set the same resource attributes as OTLP Init.
func MeterProviderFromPrometheusRegisterer(reg prometheus.Registerer, serviceName, serviceVersion string) *sdkmetric.MeterProvider {
	exporter, _ := otelprom.New(otelprom.WithRegisterer(reg))
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(DefaultResource(serviceName, serviceVersion)),
		sdkmetric.WithReader(exporter),
	)
}
