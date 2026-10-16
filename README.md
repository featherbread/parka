# Parka

Parka provides managed concurrency for tasks identified by keys.

In many cases, Parka is a convenient alternative to `sync.WaitGroup`
or `golang.org/x/sync/errgroup`. Consider the following:

```go
func HeadURLs(ctx context.Context, urls []string) ([]int, error) {
    const maxInFlight = 5
    return parka.CollectLimited(ctx, maxInFlight, urls,
        func(ctx context.Context, url string) (int, error) {
            req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
            if err != nil {
                return 0, err
            }
            resp, err := http.DefaultClient.Do(req)
            if err != nil {
                return 0, err
            }
            resp.Body.Close()
            return resp.StatusCode, nil
        })
}
```

In this example, Parka will:

* Work to build a `[]int` corresponding one-to-one with `urls []string`.
* Deduplicate `urls` and copy a single result to each corresponding output.
* Cancel the inner `ctx` if any handler fails, like `golang.org/x/sync/errgroup`.
* Limit to 5 goroutines concurrently running the handler function,
  [without spawning or leaking idle workers](https://www.youtube.com/watch?v=5zXAHh5tJqQ&t=1789s)
  for shorter `urls` lists.

These features build on the `parka.Map` type, which additionally exposes
persistent caching of results, dynamically adjustable concurrency limits,
and more flexible task prioritization. Shared maps can be a powerful tool to
orchestrate complex dependencies across tasks in a workflow.

## Resources

* <https://pkg.go.dev/github.com/featherbread/parka>
* <https://github.com/featherbread/magic-mirror>, Parka's original use case,
  which demonstrates the more advanced features of `parka.Map`.

## Status

Parka is tagged at v1 and committed to API stability. As of October 2026:

* I have no plans to add _or maintain_ major new features like cache eviction
  or more granular priority queueing. These features are likely to interact in
  complex and subtle ways with the existing `Map` APIs, but I've yet to find a
  use case for them even with multiple projects using Parka.

* Issues are open. However, as this is a personal project undertaken mainly for
  my own edification, I'd like for all code and documentation to remain the
  product of my own effort and voice, and as such have closed PR submission.
  I actively encourage forks for private experimentation! Thank you for your
  understanding.

* I have no concrete plans for a v2 design, though the introduction of generic
  methods in Go 1.27 opens up an exciting space of possibilities. I would
  expect any v2 of Parka to be flexible enough that the current API could be
  reimplemented with it.

The upshot is that Parka is likely to remain stable in its current form over a
time span on the order of years.
