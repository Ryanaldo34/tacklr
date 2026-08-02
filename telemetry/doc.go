// Package telemetry configures OpenTelemetry for Tacklr hosts and process tools.
//
// Host API:
//   - Config, Init — OTLP traces, metrics, and logs
//   - InstallDefault, InstallDefaultWithOTLP, NewLogger — slog setup
//   - MeterProviderFromPrometheusRegisterer — Prometheus scrape
//   - DefaultResource — service resource
//   - StdioWatchDog — AgentWatchDog that writes to stderr
//   - server.WithTracerProvider, WithMeterProvider — registry providers
//
// Span starters, Instruments.Record*, attribute constants, and EmitEvent are
// for the harness and server packages. Hosts must not start tacklr spans or
// record harness metrics; that can break traces and double-count metrics.
package telemetry
