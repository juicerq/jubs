# Validation and versioning

## Payload validation on both sides

The payload is validated twice — once on enqueue, and again when the job runs. The second check matters because the job may have been written by an older deploy.

A job whose stored payload no longer validates fails unrecoverably: it burns one attempt and is not retried. So does a job whose name no handler owns. Retrying either would only fail the same way five more times.

## Payload versioning

A payload shape changes, and Redis still holds jobs written in the old shape. `version` and `migrations` let you change it without losing them.

`version` is the shape number of the payload. It defaults to `1`, and every enqueue writes it into the envelope as `v`. `migrations` raises an older envelope to the running version, one step at a time, **before** the payload is validated. The key is the version the step migrates **from**.

```ts
export const syncContact = defineJob({
	name: "contact.sync",
	queue: "crm",
	payload: type({ email: "string" }),
	version: 2,
	migrations: {
		1: (data) => ({ email: (data as { mail: string }).mail }),
	},
})
```

A job stored at `v: 1` runs step `1`, and the result is what the schema validates. A job stored at `v: 2` runs no step at all. Version 3 would add a step `2`, and a `v: 1` job would run step `1` then step `2`, in that order.

A migration receives the **raw** stored data, not a validated payload — the old shape is exactly what the old schema no longer describes. It may return a promise. Its output is only validated once the last step has run.

Each step is an ordinary function, so it is testable on its own, with no queue and no Redis:

```ts
expect(syncContact.migrations?.[1]?.({ mail: "ada@example.com" })).toEqual({
	email: "ada@example.com",
})
```

### The two directions of a deploy

A deploy is never atomic. Some jobs in Redis were written by the deploy before, and — during a rollout, or after a rollback — some were written by the deploy after.

**Backwards: an old envelope on a new worker.** This is what `migrations` is for. Raise `version` by one and add the step that goes from the old version to the new one. Deploy it. Every job still in Redis runs through the step and works.

**Forwards: a new envelope on an old worker.** The old worker cannot read a shape it has never heard of, and guessing would corrupt data. jubs refuses instead: an envelope whose `v` is greater than the running `version` is **never** interpreted. It fails unrecoverably, burns one attempt, is not retried, and — if the queue is in `deadQueues` — is kept in the dead queue with the reason `version_ahead`. It never fires `onStart` — a job held by a future version never started. Once the new workers are up, replay it.

The safe rollout is two deploys: workers first, producers second. Deploy the new version to the workers while producers still write the old one — the new workers migrate what the old producers write. Then deploy the producers. Nothing is ever `version_ahead` in that order.

**A flow in flight is buried, not migrated.** `migrations` carries a payload across a deploy; it carries no children. A parent whose envelope was written before this build has no record of how many children each slot was given, so the first delivery after the deploy buries it as `children_short` instead of running its handler over slots it cannot read. Drain the flows before the deploy goes up, or accept enqueueing them again from the top with `jobs.enqueue(definition, data, awaits)`.

**A scheduled job has no second deploy.** Its producer is `start` itself: at boot it rewrites the schedule template with the version it runs, on the key every other pod shares. During a rolling deploy an occurrence written at the new version can reach a pod still running the old one, and that occurrence fails `version_ahead`. Stop the queue's workers before you raise the `version` of a scheduled job. The occurrence waits in Redis and runs once the new workers are up — nothing is lost, only delayed, and BullMQ writes the next occurrence only once this one is consumed, so nothing piles up either.

If you roll instead, keep the queue in `deadQueues`: without it the occurrence is only a failed job, and `dead.replay` has nothing to replay. Replay recovers the occurrence exactly — `origin: "schedule"` survives and the delivery matches, only the id is new — which needs both the queue in `deadQueues` and the definition registered in `createJobs({ definitions })`.

The rule when a rollback is likely: keep the step, do not delete it. A migration you delete makes every envelope still stored at that version unrunnable.

### What `defineJob` refuses

`version` must be a whole number of `1` or more, and every migration key must be a whole number between `1` and `version - 1`. A step outside that range never runs, which is always a mistake — a typo, or a `version` you forgot to raise.

The full chain is **not** required. A definition can jump to `version: 3` with only a step `2`, because a job raising its version with nothing old left in Redis should not have to write identity functions. The price is that the missing step is found at run time: a job stored at a version with no step from it fails unrecoverably and names the version it needed.
