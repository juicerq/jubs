# Atomic block takes a transaction handle from v1

Redis cannot join a database transaction, so a rollback after `enqueue` leaves an orphan job. `jobs.atomic(fn, { tx })` buffers enqueues and only delivers them if `fn` resolves, which closes the rollback case without the library knowing anything about databases. The `tx` handle is passed through to a user-supplied `Outbox`, which stores envelopes inside the caller's transaction so state and jobs commit together; a relay then delivers and settles them.

## Considered options

Deferring the outbox interface to a later version was rejected: the `tx` argument has to exist at every call site from the start, and retrofitting it into a production codebase is a migration nobody wants.

## Consequences

The outbox delivers at least once — a relay that crashes between delivery and settle re-delivers. Anyone enabling it should also set `idempotency` on the affected definitions. v1 ships the interface and the relay but no adapter; the README carries a Kysely implementation to copy.
