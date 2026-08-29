// Package postgres is the optional Postgres implementation of brain.Store.
//
// Hosts construct a Store with New(pool) and inject it into brain.NewEngine.
// Call Setup once per database to create extensions, tables, and indexes.
// Query/Exec emit otelpgx spans under the caller context after telemetry.Init.
//
// Package brain does not import this package. MemoryStore stays in brain for
// tests and offline hosts.
package postgres
