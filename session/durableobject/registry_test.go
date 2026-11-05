package durableobject_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
)

// TestRegistry_keepsABoundOfIdleObjectsAndNoneInUse: a request reaches the
// object that served its instance before, and a new instance gets a new
// object. Past its limit the registry drops the least recently used idle
// object, never one with an open request, so a dropped id gets a new
// object and a held one keeps its own.
func TestRegistry_keepsABoundOfIdleObjectsAndNoneInUse(t *testing.T) {
	// Arrange
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	newObject := func() *durableobject.Object {
		// The registry only routes requests: an object it holds serves none here.
		return durableobject.NewObject(nil, durableobject.Config{Sealer: sealer})
	}
	r := durableobject.NewRegistry(1)

	// Act
	a1, releaseA1 := r.Acquire("a", 1, newObject)
	again, releaseAgain := r.Acquire("a", 1, newObject)
	releaseAgain()
	a2, releaseA2 := r.Acquire("a", 2, newObject)
	b, releaseB := r.Acquire("b", 1, newObject)
	releaseA1() // the old instance ends while both live ones serve
	releaseB()  // b is idle past the limit: dropped
	releaseB()  // a second release counts once
	held, releaseHeld := r.Acquire("a", 2, newObject)
	releaseHeld()
	bAgain, releaseBAgain := r.Acquire("b", 1, newObject)
	releaseBAgain()
	releaseA2()

	// Assert
	if again != a1 || a2 == a1 {
		t.Fatal("want one object per instance: the same for its requests, a new one for a new instance")
	}
	if held != a2 || bAgain == b {
		t.Fatal("want the object in use kept and the idle one dropped")
	}
}
