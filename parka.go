/*
Package parka provides managed concurrency for tasks identified by keys.

For simple use cases, the top-level [Collect] and [CollectLimited] functions
concurrently map over sets of keys with duplicate suppression and optional
bounded concurrency. For advanced use cases, the underlying [Map] type
additionally exposes persistent caching of results, dynamically adjustable
concurrency limits, and more flexible task prioritization.

This version of Parka is suited for work that doesn't require cache eviction,
like a main package running a single batch-type workflow or short-lived maps
that are disposed of after a single use.
*/
package parka
