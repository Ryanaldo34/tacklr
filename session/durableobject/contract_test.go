package durableobject_test

import (
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
	"github.com/ryanaldo34/tacklr/session/sessiontest"
)

// TestClient_meetsTheSessionContract: the client runs sessions as every
// runtime does, over objects whose every call crosses the object protocol,
// as on Cloudflare.
func TestClient_meetsTheSessionContract(t *testing.T) {
	t.Parallel()
	sessiontest.Suite{LiveDeltas: true, Deploy: func(t *testing.T, agent tacklr.AgentOptions, jobs map[string]session.JobHandler) session.Client {
		sealer, err := session.NewAESGCM(make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
		return durableobject.NewClient(dotest.NewWire(durableobject.Config{Agent: agent, Jobs: jobs, Sealer: sealer, RetryBackoff: time.Millisecond}))
	}}.Run(t)
}
