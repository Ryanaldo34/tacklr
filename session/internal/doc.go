// Package adapter holds what the session runtimes share: a turn's auth on
// the session's mount recipes, tool outcomes kept by call key, and host
// state encoding. Hosts use a session.Client, not these functions.
package adapter
