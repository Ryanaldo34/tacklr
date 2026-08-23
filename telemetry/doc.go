// Package telemetry configures OpenTelemetry for Tacklr hosts and process tools.
//
// Default export is the LGTM stack via one OTLP endpoint (Grafana Alloy or the
// OpenTelemetry Collector → Tempo, Loki, Mimir/Prometheus, Grafana).
//
// Host API:
//   - Config, Init — OTLP traces, metrics, and logs (ReplaySafe tracer provider)
//   - EnsureReplaySafeProvider — required before Temporal ObservabilityPlugin
//   - Instrumentor — durable wait-loop hook (context.Context runtimes)
//   - InstallDefault, InstallDefaultWithOTLP, NewLogger — slog setup
//   - MeterProviderFromPrometheusRegisterer — Prometheus scrape
//   - DefaultResource — service resource
//   - StdioWatchDog — AgentWatchDog that writes to stderr
//
// Span starters, Instruments.Record*, attribute constants, and EmitEvent are
// for the harness and durable packages. Hosts must not start tacklr spans or
// record harness metrics; that can break traces and double-count metrics.
package telemetry
