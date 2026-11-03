package storetest_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/storetest"
)

func TestMemoryStores_meetTheContract(t *testing.T) {
	storetest.Store(t, func(*testing.T) session.Store { return session.NewMemoryStore() })
	storetest.Secrets(t, func(*testing.T) session.SecretStorage { return session.NewMemorySecretStorage() })
	storetest.EventLog(t, func(*testing.T) session.EventLog { return session.NewMemoryEventLog() })
}
