// Package adapter applies a turn's auth to the session's mount recipes:
// the session loop and the Temporal worker share it. Hosts use a
// session.Client, not these functions.
package adapter
