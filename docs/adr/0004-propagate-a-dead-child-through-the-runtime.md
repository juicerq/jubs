# Propagate a dead child through the runtime, not through BullMQ

Every node of a flow but the root is added with `ignoreDependencyOnFailure`, so a child that exhausts its attempts drops out of its parent's dependencies and leaves its reason in the parent's `:failed` hash. The parent then runs, reads that hash, finds the failure and is buried by juibs with the reason `child_dead` — where the failure hooks fire and a dead entry is kept, carrying what the children that did finish returned.

`failParentOnFailure` was the alternative, and it fails the parent inside BullMQ's own worker, before our processor is called. Nothing would be buried, no hook would fire, and the grandparent would get the same treatment one level up — a whole flow dying with no record of why. Reading the failure ourselves costs one dispatch and buys the burial, the hook and the results.

## Consequences

The parent spends one attempt to discover the failure. It is a real attempt: it was fetched, it ran, and pretending otherwise would lie to `attemptsMade`. The burial is `UnrecoverableError`-shaped, so no further attempt follows.

`origin: "flow"` on the envelope is what makes the runtime read the flow state at all, and is therefore load-bearing rather than descriptive. A job carrying any other origin reads no state and no children, and touches Redis for neither.

A flow job cannot be replayed. `dead.replay` refuses an entry whose origin is `flow`, because the replayed parent would be enqueued alone, run over an empty result set and complete green. The repair path is `jobs.retry` on each failed child, which returns it to its parent's dependencies, then `jobs.retry` on the parent.
