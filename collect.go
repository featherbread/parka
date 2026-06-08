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
	hctx, cancel := context.WithCancel(ctx)
	m := NewMap(func(_ *Handle, key K) (V, error) { return handle(hctx, key) })

	// See the "Context" example in the Map docs for an explanation of this pattern.
	//
	// TODO: This pattern is currently problematic due to how Collect works.
	// It iterates sequentially to retrieve the results for keys, so a blocked
	// handler for the first key prevents subsequent handlers from triggering
	// cancellation. A big regression over errgroup.
	defer func() {
		m.DequeueAll()
		cancel()
		m.WaitAttached()
	}()

	m.Limit(limit)
	return m.Collect(keys...)
}
