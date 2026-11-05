package durableobject

import "sync"

// Registry holds the live objects of a process that hosts many (on
// Workers, one Go program serves every object of its isolate). It routes
// each request to its object by id, makes a new object when the platform
// starts a new instance of one (after a reset), and keeps at most a bound
// of objects that are idle and serve nothing, dropping the least recently
// used: a dropped object starts again from its storage on its next
// request.
type Registry struct {
	limit int
	mu    sync.Mutex
	live  map[string]*registered
	clock uint64
}

// registered is one live object: the platform instance it serves, its
// open requests, and when it was last used.
type registered struct {
	instance int
	obj      *Object
	open     int
	used     uint64
}

// defaultRegistryLimit is how many idle objects a Registry keeps when its
// limit is zero.
const defaultRegistryLimit = 64

// NewRegistry returns an empty Registry that keeps at most limit idle
// objects (zero or less: 64).
func NewRegistry(limit int) *Registry {
	return &Registry{limit: positive(limit, defaultRegistryLimit), live: map[string]*registered{}}
}

// Acquire is id's object for one request of the platform's instance
// (a number the platform gives each instance of an object). It is made
// with newObject when the registry holds none for id, or holds one of an
// older instance, which it stops. Call release once the request, and any
// stream it answers with, ends.
func (r *Registry) Acquire(id string, instance int, newObject func() *Object) (obj *Object, release func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.live[id]
	if e != nil && e.instance != instance {
		e.obj.Stop()
		e = nil
	}
	if e == nil {
		e = &registered{instance: instance, obj: newObject()}
		r.live[id] = e
	}
	r.clock++
	e.open, e.used = e.open+1, r.clock
	var once sync.Once
	return e.obj, func() { once.Do(func() { r.release(e) }) }
}

// release ends one request of e, then drops the least recently used
// objects that are idle and serve nothing while more than the limit live.
func (r *Registry) release(e *registered) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.open--
	for len(r.live) > r.limit {
		var oldest string
		for id, c := range r.live {
			if c.open == 0 && c.obj.idle() && (oldest == "" || c.used < r.live[oldest].used) {
				oldest = id
			}
		}
		if oldest == "" {
			return
		}
		r.live[oldest].obj.Stop()
		delete(r.live, oldest)
	}
}
