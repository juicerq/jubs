# juibs

A typed job library over BullMQ. It gives a job a name, a payload schema, a delivery policy and a place to run, so producers and consumers never share code.

## Language

**Job**:
A unit of background work, identified by a stable name.
_Avoid_: Task, message, event

**Definition**:
The producer-side description of a job — its name, payload schema, queue, delivery policy, schedule and result schema.
_Avoid_: Contract, spec, descriptor

**Handler**:
The consumer-side function that runs a job. One handler per definition.
_Avoid_: Processor, worker function, consumer

**Envelope**:
What is stored for a single job instance: payload version, job name and payload.
_Avoid_: Message, record

**Payload version**:
The number that says which shape a stored payload has.
_Avoid_: Schema version, revision

**Migration**:
The step that raises a stored payload from one version to the next.
_Avoid_: Schema migration — the step changes a payload, never a database

**Queue**:
The named channel a job is delivered through. Many jobs share one queue.

**Runtime**:
The set of live workers started for a group of handlers, plus the handle that closes them.
_Avoid_: Engine, processor pool

**Delivery**:
The policy applied when a job is enqueued: attempts, backoff, priority, delay, deduplication, replacement.
_Avoid_: Options, config

**Uniqueness**:
The rule that decides which of several jobs sharing a key survives: the first, the last, or one at a time.
_Avoid_: Deduplication, debounce, throttle

**Schedule**:
The recurrence rule that makes a job run on its own, without a producer.
_Avoid_: Cron, repeat, recurring job

**Flow**:
A job whose execution waits on child jobs, and which reads their results.
_Avoid_: Chain, pipeline, DAG

**Dead queue**:
A queue holding jobs that failed every attempt, kept for inspection and replay. Consumed by nobody.
_Avoid_: DLQ, failed queue, graveyard

**Replay**:
Enqueueing a job from the dead queue again.
_Avoid_: Retry — retry is BullMQ's automatic attempt within one job's life

**Atomic block**:
A scope where enqueued jobs are held back and only delivered if the scope completes.
_Avoid_: Transaction — the block does not own the database transaction

**Outbox**:
Storage that accepts envelopes inside the caller's database transaction, so a state change and its jobs commit together.

**Relay**:
The process that claims envelopes from the outbox, delivers them to the queue, and marks them delivered.
_Avoid_: Publisher, dispatcher

**Origin**:
What caused a job to exist: a direct call, its own schedule, a flow, or the relay.

**Driver**:
The storage a client talks to — Redis in production, memory in tests.

**Idempotency key**:
A value derived from the payload that marks a job as already done, so a repeated delivery skips the handler.
