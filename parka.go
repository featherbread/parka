/*
Package parka provides managed concurrency for tasks identified by keys.

Parka's [Map] composes duplicate call suppression, caching, and bounded
concurrency into a single mechanism for distributing work across goroutines.
Maps provide dynamic control over concurrency limits and task prioritization,
including at the level of individual running handlers.

This version of Parka is suited for work that doesn't require cache eviction,
like a main package running a single batch-type workflow or short-lived maps
that are disposed of after a single use.
*/
package parka
