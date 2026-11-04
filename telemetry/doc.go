// Package telemetry is Tacklr's OpenTelemetry instrumentation: spans,
// metrics and log events on the process-wide OpenTelemetry providers. It
// depends only on the OpenTelemetry API, so it builds anywhere the harness
// does. Package telemetry/export installs the SDK providers and exporters
// (OTLP, Prometheus) a host process sends them to.
//
// Span starters, Instruments.Record*, attribute constants, and EmitEvent are
// for the harness and the session package. Hosts must not start tacklr spans or
// record harness metrics; that can break traces and double-count metrics.
package telemetry
