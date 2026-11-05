// Package testkit provides shared test doubles for harness and server integration tests.
package testkit

import "github.com/ryanaldo34/tacklr/message"

// ContentTokenEstimate is a shared length-based token stand-in for pressure tests.
func ContentTokenEstimate(msgs []*message.Message) int {
	n := 0
	for _, m := range msgs {
		if m != nil {
			n += len(m.Content)
		}
	}
	return n
}
