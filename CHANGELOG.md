## Unreleased

### Added

* **The `{Map,Set}.{Get,Collect}Context` methods.** Individual callers can use
  `context.Context` to cancel their own waiting for results while the map
  continues to handle keys in the background. This simplifies the use of shared
  maps across independent consumers.

## v1.0.0 (2026-10-19)

This is the initial stable release of Parka, a managed concurrency library
extracted from <https://github.com/featherbread/magic-mirror> that occasionally
simplifies my work.

While Parka has only been an independent module for a few months, its history
stretches back several years, and its design and features are based on
practical use cases from Magic Mirror and other projects, so I think it's
mature enough for me to lock in and commit to a stable API. As noted in the
README, I have no concrete plans for _substantial_ changes, but there may be
little quality of life improvements here and there.
