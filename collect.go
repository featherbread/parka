package parka

import (
	"context"
	"math"
	"sync/atomic"
)

// Collect runs a handler function once per unique key in a distinct goroutine,
// and coalesces the results.
//
// If any handler returns an error, Collect cancels the remaining handlers'
// contexts, waits for all handlers to finish, and returns that error.
// Otherwise, it returns a slice of values corresponding to the provided keys.
//
// Collect doesn't recover panics; they crash the program if not recovered
// within the handler. If the handler calls [runtime.Goexit], Collect cancels
// remaining work as described above, then panics with [ErrHandlerGoexit].
func Collect[K comparable, V any](
	ctx context.Context,
	keys []K,
	handle func(context.Context, K) (V, error),
) ([]V, error) {
	return CollectLimited(ctx, math.MaxInt, keys, handle)
}

// CollectLimited behaves like [Collect], but limits the number of goroutines
// that may concurrently handle keys. It panics if the provided limit is less
// than 1.
//
// If any handler returns an error or Goexits, CollectLimited cancels in-flight
// handlers as described in [Collect], and leaves queued keys unhandled.
func CollectLimited[K comparable, V any](
	ctx context.Context,
	limit int,
	keys []K,
	handle func(context.Context, K) (V, error),
) (values []V, err error) {
	var (
		hctx, cancel = context.WithCancel(ctx)
		mapper       *Map[K, V]
		cleaner      Set[struct{}]
		firstErr     atomic.Pointer[error]
	)

	// [Map.Collect] returns the first error with respect to the key order, which
	// may be a useless [context.Canceled] if a subsequent handler canceled first.
	// Returning the error that actually triggered cancellation requires extra
	// work on the handler's part, but is a better match for the venerable
	// golang.org/x/sync/errgroup.
	defer func() {
		if errp := firstErr.Load(); errp != nil {
			err = *errp
		}
	}()

	// The mapper does the actual work. As the [Map.Collect] docs note, it's
	// crucial that we trigger early cancellation before the handler returns,
	// to ensure Collect isn't blocked by a stuck preceding handler.
	mapper = NewMap(func(_ *Handle, key K) (v V, err error) {
		var returned bool
		defer func() {
			if err != nil {
				firstErr.CompareAndSwap(nil, &err)
			}
			if err != nil || !returned {
				cleaner.Inform(struct{}{})
			}
		}()
		v, err = handle(hctx, key)
		returned = true
		return
	})

	// The cleaner is a single-shot task that cancels the context and waits for
	// in-flight handlers to finish. We dequeue pending keys before canceling the
	// context to limit any flood of fast-failing handlers for those keys.
	// Notice how this benefits from being defined as a parka.Set: the cleanup
	// goroutine isn't launched until it's required, and can be triggered with
	// blocking or non-blocking semantics as necessary.
	cleaner = NewSet(func(_ *Handle, _ struct{}) error {
		mapper.DequeueAll()
		cancel()
		mapper.WaitAttached()
		return nil
	})
	defer cleaner.Get(struct{}{})

	mapper.Limit(limit)
	return mapper.Collect(keys...)
}
