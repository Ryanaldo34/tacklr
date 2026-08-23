// Package telemetry configures OpenTelemetry for Tacklr hosts and process tools.
//
// Default export is the LGTM stack via one OTLP endpoint (Grafana Alloy or the
// OpenTelemetry Collector → Tempo, Loki, Mimir/Prometheus, Grafana).
//
// Host API:
//   - Config, Init — process-wide OTLP traces/metrics/logs with Temporal's
//     ReplaySafe tracer provider. Call before durable/temporal.Dial.
//   - MeterProviderFromPrometheusRegisterer — Prometheus scrape
//   - DefaultResource — service resource
//
// Span starters, Instruments.Record*, attribute constants, and EmitEvent are
// for the harness and durable packages. Hosts must not start tacklr spans or
// record harness metrics; that can break traces and double-count metrics.
package telemetry
