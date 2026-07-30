# Store query timeouts

Store queries are executed through `GuardedStore`, which applies the configured
per-query timeout to each store operation. The timeout is exposed through the
existing `API_QUERY_TIMEOUT` configuration setting and defaults to 25 seconds.

The guarded store derives a child context for each operation and passes that
context to the underlying store implementation. Consequently, the earlier of
the request's existing deadline and `API_QUERY_TIMEOUT` determines how long the
query may run. Cancellation is propagated to the database driver, which stops
the in-flight query and returns the context error.

`API_SLOW_QUERY_THRESHOLD` controls slow-query logging independently; it does
not extend the query timeout. The timeout applies to reads and other guarded
store operations without changing any endpoint, configuration, or database
schema contracts.
