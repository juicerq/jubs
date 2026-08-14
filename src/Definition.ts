import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { DeliveryPolicy } from "@/Delivery"
import { FLOW_ID_MARKER } from "@/JobId"
import type { PayloadMigration } from "@/Migration"
import type { Schedule } from "@/Schedule"

export const DEFAULT_PAYLOAD_VERSION = 1

export type Origin = "direct" | "schedule" | "flow" | "relay"

/**
 * What a definition waits on, slot by slot. The shape of a slot declares its
 * arity: a bare definition is exactly one child, and an array literal holding
 * one definition is many of them.
 *
 * The slot, not the definition, is what names a child everywhere after this —
 * in the id Redis stores it under, in the results the handler reads and in the
 * failure that buries the parent — so two slots holding the very same
 * definition stay apart.
 */
export type AwaitsMap = Readonly<Record<string, AnyDefinition | readonly [AnyDefinition]>>

/**
 * A definition with every type parameter at its widest, which is what a slot of
 * an `AwaitsMap` holds. Writing it out is what lets `AwaitsMap` name
 * `JobDefinition` while `JobDefinition` names `AwaitsMap` back.
 */
export type AnyDefinition = JobDefinition<
	StandardSchemaV1,
	string,
	StandardSchemaV1 | undefined,
	AwaitsMap | undefined
>

type PayloadOf<Definition> =
	Definition extends JobDefinition<infer Payload> ? StandardSchemaV1.InferInput<Payload> : never

type AwaitsOf<Definition> =
	Definition extends JobDefinition<
		StandardSchemaV1,
		string,
		StandardSchemaV1 | undefined,
		infer Awaits
	>
		? Awaits
		: never

type ResultOf<Definition> =
	Definition extends JobDefinition<StandardSchemaV1, string, infer Result>
		? Result extends StandardSchemaV1
			? StandardSchemaV1.InferOutput<Result>
			: unknown
		: never

/**
 * What one child of a slot takes: its payload alone when it waits on nothing,
 * and its payload beside what its own slots take when it does. That is where
 * the recursion lives — a flow of any depth is this type applied again.
 */
type SlotInput<Definition> =
	AwaitsOf<Definition> extends AwaitsMap
		? { readonly data: PayloadOf<Definition>; readonly awaits: SlotInputs<AwaitsOf<Definition>> }
		: PayloadOf<Definition>

type SlotInputs<Awaits extends AwaitsMap> = {
	readonly [Slot in keyof Awaits]: Awaits[Slot] extends readonly [infer Child]
		? readonly SlotInput<Child>[]
		: SlotInput<Awaits[Slot]>
}

/**
 * What fills every slot of a definition — the third argument `jobs.enqueue`
 * takes from a definition that declares `awaits`.
 *
 * Annotate a pre-built input with it. An input written straight inside the call
 * is checked against the slot it fills, so a typo names that slot; the same
 * input built in a `const` first is checked as a whole afterwards, and the
 * error becomes a wall as deep as the tree. `const input: AwaitsInput<typeof
 * zipNfeExport> = { ... }` puts the leaf error back.
 */
export type AwaitsInput<Definition> =
	AwaitsOf<Definition> extends AwaitsMap ? SlotInputs<AwaitsOf<Definition>> : Record<string, never>

/**
 * What each slot of a definition read back inside its handler: one validated
 * value for a slot declared as a bare definition, and an array for a slot
 * declared as an array.
 */
type Children<Awaits extends AwaitsMap> = {
	readonly [Slot in keyof Awaits]: Awaits[Slot] extends readonly [infer Child]
		? readonly ResultOf<Child>[]
		: ResultOf<Awaits[Slot]>
}

/**
 * The trailing arguments `jobs.enqueue` takes for a definition. A definition
 * that waits on nothing takes none, and `awaits: {}` is a definition that waits
 * on nothing — the slots, not the key, are what make a flow.
 */
export type EnqueueAwaits<Awaits extends AwaitsMap | undefined> = Awaits extends AwaitsMap
	? keyof Awaits extends never
		? []
		: [awaits: SlotInputs<Awaits>]
	: []

export interface HandlerContext<Awaits extends AwaitsMap | undefined = AwaitsMap> {
	/**
	 * The id of this job, in the one form the whole API speaks: the same string
	 * `jobs.enqueue` gave the producer, and the one `jobs.get`, `jobs.retry` and
	 * `jobs.cancel` take.
	 */
	readonly id: string
	readonly attempt: number
	readonly maxAttempts: number
	readonly origin: Origin
	/**
	 * What the children this job waited on returned, under the slot each one
	 * fills. What comes back is the JSON projection of what a child's handler
	 * returned, put through that child definition's `result` schema — so a `Date`
	 * a child returned comes back as the schema reads it.
	 *
	 * Every slot is read and validated before the handler runs, so a value a
	 * schema refuses fails this job for good with no handler code having run. A
	 * definition that declares no `awaits` reads an empty object.
	 */
	readonly children: Awaits extends AwaitsMap ? Children<Awaits> : Record<string, never>
	/**
	 * Aborts when the job's `timeoutMs` expires, when `jobs.cancel(id)` reaches
	 * this job, and when `close({ timeoutMs })` runs out of patience during a
	 * shutdown. `signal.reason` tells the three apart: a shutdown aborts with a
	 * `ShutdownAbortError`, and a cancellation with a `CancelledError`.
	 *
	 * Pass it to whatever waits — `fetch`, a database client, another
	 * `AbortSignal`-aware library — and **throw** once it aborts. A handler that
	 * returns normally after an abort is recorded as a success, and jubs has no
	 * way to know the work stopped half done. A handler that finished just before
	 * the abort should still return: a return is always a success.
	 */
	readonly signal: AbortSignal
}

export interface JobDefinition<
	Payload extends StandardSchemaV1 = StandardSchemaV1,
	Queue extends string = string,
	Result extends StandardSchemaV1 | undefined = StandardSchemaV1 | undefined,
	Awaits extends AwaitsMap | undefined = AwaitsMap | undefined,
> {
	readonly name: string
	readonly queue: Queue
	readonly payload: Payload
	/**
	 * The schema of what the handler resolves. The handler returns this schema's
	 * input, and what the schema gives back is what the handler's return value
	 * becomes. A repeated delivery under an `idempotencyKey` replays that value
	 * as JSON, so a `Date` comes back a string. A definition that declares no
	 * `result` validates nothing, and its handler returns `unknown`.
	 */
	readonly result?: Result
	/**
	 * The jobs this one waits on, under the slot each one fills. Declaring it
	 * makes every enqueue of this job a flow: `jobs.enqueue` then takes what
	 * fills every slot as its third argument, and the handler reads what they
	 * returned under the same slots.
	 */
	readonly awaits?: Awaits
	readonly version?: number
	readonly migrations?: Readonly<Record<number, PayloadMigration>>
	readonly delivery?: DeliveryPolicy
	readonly schedule?: Schedule
	readonly idempotencyKey?: (data: unknown) => string
	readonly timeoutMs?: number
}

export interface JobDefinitionInput<
	Payload extends StandardSchemaV1,
	Queue extends string = string,
	Result extends StandardSchemaV1 | undefined = StandardSchemaV1 | undefined,
	Awaits extends AwaitsMap | undefined = AwaitsMap | undefined,
> extends Omit<
	JobDefinition<Payload, Queue, Result, Awaits>,
	"delivery" | "schedule" | "idempotencyKey"
> {
	readonly delivery?: DeliveryPolicy<StandardSchemaV1.InferOutput<Payload>>
	readonly schedule?: Schedule<StandardSchemaV1.InferInput<Payload>>
	readonly idempotencyKey?: (data: StandardSchemaV1.InferOutput<Payload>) => string
}

export function payloadVersion(definition: JobDefinition): number {
	return definition.version ?? DEFAULT_PAYLOAD_VERSION
}

export interface JobHandler<Queue extends string = string> {
	readonly definition: JobDefinition<StandardSchemaV1, Queue>
	run(data: unknown, context: HandlerContext): Promise<unknown>
}

export type HandlerRun<
	Payload extends StandardSchemaV1,
	Result extends StandardSchemaV1 | undefined = StandardSchemaV1 | undefined,
	Awaits extends AwaitsMap | undefined = AwaitsMap | undefined,
> = (
	data: StandardSchemaV1.InferOutput<Payload>,
	context: HandlerContext<Awaits>,
) => Promise<Result extends StandardSchemaV1 ? StandardSchemaV1.InferInput<Result> : unknown>

function strayMigration(name: string, from: string, version: number): Error {
	if (version <= DEFAULT_PAYLOAD_VERSION) {
		return new Error(
			`jubs: the job "${name}" declares a migration from version ${from}, which never runs — the job runs payload version ${version}, so it takes no migration at all; raise its \`version\` first`,
		)
	}

	return new Error(
		`jubs: the job "${name}" declares a migration from version ${from}, which never runs — the job runs payload version ${version}, so a migration key is a whole number between 1 and ${version - 1}`,
	)
}

function assertVersioning(
	definition: Pick<JobDefinition, "name" | "version" | "migrations">,
): void {
	const version = definition.version ?? DEFAULT_PAYLOAD_VERSION

	if (!Number.isInteger(version) || version < 1) {
		throw new Error(
			`jubs: the job "${definition.name}" declares the payload version ${version} — a payload version is a whole number of 1 or more`,
		)
	}

	const stray = Object.keys(definition.migrations ?? {}).find((key) => {
		const from = Number(key)

		return !Number.isInteger(from) || from < 1 || from >= version
	})

	if (!stray) {
		return
	}

	throw strayMigration(definition.name, stray, version)
}

function assertTimeout(definition: Pick<JobDefinition, "name" | "timeoutMs">): void {
	const { timeoutMs } = definition

	if (timeoutMs === undefined) {
		return
	}

	if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
		return
	}

	throw new Error(
		`jubs: the job "${definition.name}" declares the \`timeoutMs\` ${timeoutMs} — a timeout is a number of milliseconds above 0, so give it one or drop \`timeoutMs\` to let the handler run for as long as it takes`,
	)
}

/**
 * The rules a definition that waits on children has to hold, all of them known
 * from the definition alone.
 *
 * `unique`, `idempotencyKey` and `schedule` are refused here because waiting on
 * children is now a property of the definition, not of one enqueue. Uniqueness
 * does not apply anywhere inside a flow; a key derived from the payload alone
 * cannot name a run whose input is its children as much as its payload; and a
 * recurrence has no producer to fill the slots with.
 *
 * A `delivery` written as a function is not read: it takes the payload of a job
 * that has not been enqueued yet. The composition of a flow resolves it and
 * refuses `unique` again, at every node, which is what covers that case.
 */
function assertAwaits<Payload extends StandardSchemaV1>(
	input: Pick<
		JobDefinitionInput<Payload>,
		"name" | "awaits" | "delivery" | "idempotencyKey" | "schedule"
	>,
): void {
	const slots = Object.keys(input.awaits ?? {})

	if (slots.length === 0) {
		return
	}

	const policy = input.delivery

	if (typeof policy === "object" && policy.unique) {
		throw new Error(
			`jubs: the job "${input.name}" declares \`unique\` and \`awaits\` — uniqueness does not apply inside a flow, at any position, and a job that waits on children is a flow, so drop \`unique\` from its delivery or drop \`awaits\` from that definition`,
		)
	}

	if (input.idempotencyKey) {
		throw new Error(
			`jubs: the job "${input.name}" declares \`idempotencyKey\` and \`awaits\` — a key derived from the payload alone would replay the result of an earlier flow that ran different children, so drop \`idempotencyKey\` from that definition or give the key to the jobs it waits on instead`,
		)
	}

	if (input.schedule) {
		throw new Error(
			`jubs: the job "${input.name}" declares \`schedule\` and \`awaits\` — a recurrence enqueues this job on its own, with nothing to fill its slots, every tick would run over children it never had — drop \`schedule\` from that definition and give it to a job that waits on nothing, which enqueues this flow with jobs.enqueue`,
		)
	}

	for (const slot of slots) {
		const stray = [":", FLOW_ID_MARKER].find((character) => slot.includes(character))

		if (stray) {
			throw new Error(
				`jubs: the job "${input.name}" waits on the slot "${slot}", whose name holds "${stray}" — a child is stored under an id built from the slot it fills, which that character breaks, so rename the slot`,
			)
		}
	}
}

export function defineJob<
	Payload extends StandardSchemaV1,
	const Queue extends string = string,
	Result extends StandardSchemaV1 | undefined = StandardSchemaV1 | undefined,
	Awaits extends AwaitsMap | undefined = undefined,
>(
	input: JobDefinitionInput<Payload, Queue, Result, Awaits>,
): JobDefinition<Payload, Queue, Result, Awaits> {
	assertVersioning(input)
	assertTimeout(input)
	assertAwaits(input)

	const named = { name: input.name, queue: input.queue, payload: input.payload }
	const resulting = input.result ? { ...named, result: input.result } : named
	const awaiting = input.awaits ? { ...resulting, awaits: input.awaits } : resulting
	const versioned = input.version ? { ...awaiting, version: input.version } : awaiting
	const migrating = input.migrations ? { ...versioned, migrations: input.migrations } : versioned
	const scheduled = input.schedule ? { ...migrating, schedule: input.schedule } : migrating
	const timed = input.timeoutMs ? { ...scheduled, timeoutMs: input.timeoutMs } : scheduled

	const definition = input.idempotencyKey
		? { ...timed, idempotencyKey: input.idempotencyKey }
		: timed

	if (!input.delivery) {
		return definition
	}

	return { ...definition, delivery: input.delivery }
}

export function defineHandler<
	Payload extends StandardSchemaV1,
	Queue extends string = string,
	Result extends StandardSchemaV1 | undefined = StandardSchemaV1 | undefined,
	Awaits extends AwaitsMap | undefined = AwaitsMap | undefined,
>(
	definition: JobDefinition<Payload, Queue, Result, Awaits>,
	run: HandlerRun<Payload, Result, Awaits>,
): JobHandler<Queue> {
	return { definition, run }
}
