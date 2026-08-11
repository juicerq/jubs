# Versioned envelope instead of BullMQ's native job name

Every job is stored as an envelope — `{ v, name, data }` — and one worker per queue dispatches by `name`, rather than one BullMQ queue or worker per job type. This keeps a single connection pool and one shared concurrency budget per queue, and it lets producers enqueue without importing the handler. The `v` field carries the payload version so a rolling deploy can migrate old envelopes; it is included from day one because adding it later would break every job already in Redis.

## Consequences

Handler dispatch is a runtime lookup, so a definition with no registered handler is only caught at boot, not by the type checker. `jobs.start()` therefore fails loudly when a definition has no handler.
