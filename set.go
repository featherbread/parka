package parka

import "context"

// Set wraps a [Map] whose handlers return no meaningful value with simplified
// error-only result APIs.
//
// The use of the term "set" could be considered objectionable when each key
// still maps to a meaningful error value. It may help to think of the handler
// as determining whether each key belongs to the set, and returning a non-nil
// error to explain why a given key isn't a member of the set.
type Set[K comparable] struct {
	*Map[K, struct{}]
}

// NewSet is analogous to [NewMap], but accepts a simplified handler returning
// only an error.
func NewSet[K comparable](handle func(*Handle, K) error) Set[K] {
	return Set[K]{
		Map: NewMap(func(ph *Handle, key K) (_ struct{}, err error) {
			err = handle(ph, key)
			return
		}),
	}
}

// Get is analogous to [Map.Get].
func (s Set[K]) Get(key K) error {
	_, err := s.Map.Get(key)
	return err
}

// Get is analogous to [Map.GetContext].
func (s Set[K]) GetContext(ctx context.Context, key K) error {
	_, err := s.Map.GetContext(ctx, key)
	return err
}

// Collect is analogous to [Map.Collect].
func (s Set[K]) Collect(keys ...K) error {
	_, err := s.Map.Collect(keys...)
	return err
}

// Collect is analogous to [Map.CollectContext].
func (s Set[K]) CollectContext(ctx context.Context, keys ...K) error {
	_, err := s.Map.CollectContext(ctx, keys...)
	return err
}
