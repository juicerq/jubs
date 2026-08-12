# Refuse an envelope ahead of the running version

ADR 0001 put `v` in the envelope so a rolling deploy could migrate old envelopes; `migrations` now does that, one step at a time, from the stored version up to the running `version`. The other direction has no such answer: an envelope whose `v` is greater than the running `version` was written by a shape this worker has never seen, and reading the fields it happens to recognise would silently corrupt data. So it is refused whole, never interpreted in part, and it goes to the dead queue with the reason `version_ahead`.

The refusal burns one attempt and is not retried. `UnrecoverableError` is already BullMQ's mechanism for a failure no attempt can fix, and the attempt genuinely happened — pretending otherwise would lie to `attemptsMade`. It does not fire `onStart`: the job never started. `defineJob` does not demand the full chain of steps from `1` to `version - 1`, because a definition raising its version with nothing old left in Redis would have to write identity functions to satisfy it; a missing step fails at run time instead, naming the version it needed.

## Consequences

A scheduled job has a window no deploy order closes. Its producer is `start`, which rewrites the schedule template at boot on the key every pod shares, so during a rolling deploy an occurrence written at the new version can reach a pod still on the old one and die `version_ahead`. The way out today is operational: stop the queue's workers before raising the `version`, or keep the queue in `deadQueues` and replay. Separating schedule reconciliation from `start` would close it properly; that decision is not this one, and is left to future work.
