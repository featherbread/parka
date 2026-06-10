// Package catch confines the effects of panics and [runtime.Goexit] calls.
//
// This is especially useful when testing the behavior of functions that
// intentionally panic or Goexit. Consider the following test:
//
//	func TestPanicking(t *testing.T) {
//		defer func() {
//			r := recover()
//			if rs, ok := r.(string); !ok || rs != "whoops!" {
//				t.Errorf("unexpected panic: got=%v, want=%v", r, "whoops!")
//			}
//		}()
//		panic("whoops!")
//	}
//
// Then, consider how the analogous test using [catch.Do] follows a more
// typical pattern, where the assertions follow the operation itself:
//
//	func TestPanicking(t *testing.T) {
//		result := catch.Do(func() { panic("whoops!") })
//		if rs, ok := result.Recovered().(string); !ok || rs != "whoops!" {
//			t.Errorf("unexpected panic: got=%v, want=%v", result.Recovered(), "whoops!")
//		}
//	}
package catch

import "sync"

// Do runs fn in an independent goroutine and captures its exit behavior,
// isolating the caller from any panic or [runtime.Goexit].
func Do(fn func()) (r Result) {
	r = Goexit()
	var wg sync.WaitGroup
	wg.Go(func() { r = DoOrExit(fn) })
	wg.Wait()
	return
}

// DoOrExit runs fn in the current goroutine and captures a return or panic.
// Unlike [Do], it propagates [runtime.Goexit] without returning.
func DoOrExit(fn func()) (r Result) {
	r.started = true
	func() {
		defer func() { r.panicval = recover() }()
		fn()
		r.returned = true
	}()
	r.recovered = true
	return
}

// Goexit constructs a synthetic result that captures [runtime.Goexit].
func Goexit() Result {
	return Result{
		started:   true,
		returned:  false,
		recovered: false,
	}
}

// Panic constructs a synthetic result that captures "panic(panicval)".
func Panic(panicval any) Result {
	return Result{
		started:   true,
		returned:  false,
		recovered: true,
		panicval:  panicval,
	}
}

// Return constructs a synthetic result that captures a normal return.
func Return() Result {
	return Result{
		started:   true,
		returned:  true,
		recovered: true,
	}
}

// Result captures the exit behavior of an isolated function. The zero Result
// behaves as if capturing a normal return.
type Result struct {
	started   bool
	returned  bool
	recovered bool
	panicval  any
}

// Goexited is true if this result captures [runtime.Goexit].
func (r Result) Goexited() bool {
	return r.started && !r.recovered
}

// Panicked is true if this result captures a panic.
func (r Result) Panicked() bool {
	return r.recovered && !r.returned
}

// Returned is true if this result captures a normal return.
func (r Result) Returned() bool {
	return r.returned || !r.started
}

// Recovered returns any panic value captured by this result.
//
// If the GODEBUG panicnil=1 setting is enabled, a nil Recovered value may
// represent a true "panic(nil)". [Result.Panicked] distinguishes nil panics
// from non-panic results.
func (r Result) Recovered() any {
	return r.panicval
}
