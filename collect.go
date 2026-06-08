package parka

import (
	"context"
	"math"
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
) ([]V, error) {
	var (
		hctx, cancel = context.WithCancel(ctx)
		mapper       *Map[K, V]
		cleaner      Set[struct{}]
	)

	mapper = NewMap(func(_ *Handle, key K) (v V, err error) {
		var returned bool
		defer func() {
			if err != nil || !returned {
				cleaner.Inform(struct{}{})
			}
		}()
		v, err = handle(hctx, key)
		returned = true
		return
	})

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
