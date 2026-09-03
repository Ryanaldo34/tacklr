//go:build !linux

package command

import "testing"

func skipWithoutJail(*testing.T) {}
