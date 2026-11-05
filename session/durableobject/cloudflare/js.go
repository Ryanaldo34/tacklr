//go:build js && wasm

package cloudflare

import (
	"errors"
	"syscall/js"
)

var (
	jsArray      = js.Global().Get("Array")
	jsObject     = js.Global().Get("Object")
	jsPromise    = js.Global().Get("Promise")
	jsError      = js.Global().Get("Error")
	jsUint8Array = js.Global().Get("Uint8Array")
)

// try runs fn and turns a JavaScript exception it raises into an error.
func try(fn func() js.Value) (v js.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			jsErr, ok := r.(js.Error)
			if !ok {
				panic(r)
			}
			err = jsErr
		}
	}()
	return fn(), nil
}

// promise is a JavaScript Promise of fn's result; fn runs on its own
// goroutine, so it may wait on other promises.
func promise(fn func() (any, error)) js.Value {
	exec := js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve, reject := args[0], args[1]
		go func() {
			v, err := fn()
			if err != nil {
				reject.Invoke(jsError.New(err.Error()))
				return
			}
			resolve.Invoke(v)
		}()
		return nil
	})
	defer exec.Release()
	return jsPromise.New(exec)
}

// await waits for p to settle. Call it on a goroutine of its own, never in
// a function JavaScript calls synchronously.
func await(p js.Value) (js.Value, error) {
	type settled struct {
		v   js.Value
		err error
	}
	ch := make(chan settled, 1)
	ok := js.FuncOf(func(_ js.Value, args []js.Value) any {
		ch <- settled{v: arg(args)}
		return nil
	})
	defer ok.Release()
	fail := js.FuncOf(func(_ js.Value, args []js.Value) any {
		ch <- settled{err: reason(arg(args))}
		return nil
	})
	defer fail.Release()
	p.Call("then", ok, fail)
	s := <-ch
	return s.v, s.err
}

func arg(args []js.Value) js.Value {
	if len(args) == 0 {
		return js.Undefined()
	}
	return args[0]
}

// reason is a rejected promise's reason as an error.
func reason(v js.Value) error {
	if v.Type() == js.TypeObject {
		return js.Error{Value: v}
	}
	return errors.New(v.String())
}

// bytesToJS is b as a new Uint8Array.
func bytesToJS(b []byte) js.Value {
	a := jsUint8Array.New(len(b))
	js.CopyBytesToJS(a, b)
	return a
}

// bytesToGo is a Uint8Array's bytes.
func bytesToGo(a js.Value) []byte {
	b := make([]byte, a.Get("byteLength").Int())
	js.CopyBytesToGo(b, a)
	return b
}
