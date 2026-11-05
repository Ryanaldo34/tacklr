//go:build js && wasm

// Package cloudflare runs Tacklr sessions on Cloudflare Workers: each
// session is a SQLite-backed Durable Object that runs a
// durableobject.Object, and the Worker serves the host's own routes over a
// session.Client of those objects.
//
// The Go program is compiled with GOOS=js GOARCH=wasm and calls Serve from
// main. shim.js, in this directory, is the JavaScript half: it starts the
// program once per isolate, defines the Durable Object class, and keeps
// each object's storage in one SQLite table. One program serves every
// object of its isolate, so objects are routed by id (durableobject.Registry).
//
// Objects reach each other over the object protocol (durableobject.Handler
// and durableobject.Remote) through Durable Object stubs, inside
// Cloudflare's network. A host's routes are the only way in from outside.
//
// This package and a host's main package are the only Tacklr code with a
// build tag: everything they run is plain Go, tested on Linux.
package cloudflare
