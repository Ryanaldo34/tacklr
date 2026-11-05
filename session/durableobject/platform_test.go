package durableobject

import (
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/storetest"
)

// TestStores_meetTheSessionStoreContracts: the snapshot, secret and event
// stores an object keeps over its storage meet the contract every session
// store must meet.
func TestStores_meetTheSessionStoreContracts(t *testing.T) {
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	storetest.Store(t, func(*testing.T) session.Store { return newSnapshots(NewMemStorage()) })
	storetest.Secrets(t, func(*testing.T) session.SecretStorage { return &secretStore{s: NewMemStorage(), sealer: sealer} })
	storetest.EventLog(t, func(*testing.T) session.EventLog { return newEventLog(NewMemStorage()) })
}
