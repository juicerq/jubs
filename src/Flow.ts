import {
	type AnyDefinition,
	type AwaitsMap,
	type JobDefinition,
	payloadVersion,
} from "@/Definition"
import { resolveDelivery } from "@/Delivery"
import type { ChildResult, FlowChildNode, FlowNode, FlowState } from "@/Driver"
import type { Envelope } from "@/Envelope"
import { validatePayload } from "@/Payload"

/**
 * One slot of a definition's `awaits`, read into the three things every step
 * after this needs: the name the child is stored under, the definition it runs,
 * and whether the slot holds one child or many.
 */
export interface Slot {
	readonly name: string
	readonly definition: AnyDefinition
	readonly many: boolean
}

function holdsMany(
	declared: AnyDefinition | readonly [AnyDefinition],
): declared is readonly [AnyDefinition] {
	return Array.isArray(declared)
}

export function slotsOf(definition: Pick<JobDefinition, "awaits">): Slot[] {
	return Object.entries<AwaitsMap[string]>(definition.awaits ?? {}).map(([name, declared]) => {
		if (holdsMany(declared)) {
			return { name, definition: declared[0], many: true }
		}

		return { name, definition: declared, many: false }
	})
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value)
}

function listed(names: readonly string[]): string {
	return names.map((name) => `"${name}"`).join(", ")
}

/**
 * Why a job that waited on children died: at least one of them failed for good.
 *
 * It is internal, the way `ResultError` is. A handler never catches it — the
 * runtime buries the job before the handler runs — so what it says in `cause`
 * is all a reader needs, and it says both halves of what failed: the slot, and
 * the definition the parent declared for that slot.
 */
export class ChildDeadError extends Error {
	/** What the children that did finish returned, for the dead entry to keep. */
	readonly results: readonly ChildResult[]

	constructor(definition: JobDefinition, state: FlowState) {
		const declared = new Map(slotsOf(definition).map((slot) => [slot.name, slot.definition.name]))

		const reasons = state.failures
			.map((failure) => {
				const runs = declared.get(failure.slot)
				const named = runs ? `"${failure.slot}", which runs "${runs}"` : `"${failure.slot}"`

				return `${named} (${failure.id}): ${failure.reason}`
			})
			.join("; ")

		super(
			`jubs: the job "${definition.name}" waits on children, and one of them failed every attempt — ${reasons}`,
		)
		this.name = "ChildDeadError"
		this.results = state.results
	}
}

/**
 * Why an attempt of a job that waits on children ended before its handler: one
 * of the children is running right now.
 *
 * It is retryable, and deliberately so. A parent reaches its handler with a
 * child in flight after that child was retried and the parent was retried
 * behind it, and the child fills its slot moments later — the next attempt
 * reads a full set and runs. Redis refuses the completion of this attempt
 * anyway, so failing here only moves the refusal to before the handler's side
 * effects instead of after them.
 */
export class ChildrenPendingError extends Error {
	constructor(definition: JobDefinition, pending: number) {
		super(
			`jubs: the job "${definition.name}" waits on children, and ${pending} of them ${pending === 1 ? "has" : "have"} not finished yet — this attempt ended before the handler ran, because a slot they fill would have read empty. The attempt after this one runs once they settle.`,
		)
		this.name = "ChildrenPendingError"
	}
}

/**
 * Why a job that waited on children died: a slot holds fewer children than the
 * definition declared for it, and no failure says why.
 *
 * Redis records a child that is cancelled or removed nowhere — it is dropped
 * from the parent's dependencies and the parent runs on. Only the envelope
 * remembers how many children each slot was enqueued with, so this is the one
 * refusal that reads the envelope against the definition rather than against
 * what Redis holds.
 */
export class ChildrenShortError extends Error {
	constructor(message: string) {
		super(message)
		this.name = "ChildrenShortError"
	}
}

function arrivedIn(state: FlowState, slot: string): number {
	return state.results.filter((result) => result.slot === slot).length
}

/**
 * Whether the children a job holds match the shape it was enqueued with, read
 * before the handler runs.
 *
 * A count is the only honest measure here: a child whose handler returned
 * nothing is stored as the string `"null"` and reads back as `null`, so a slot
 * full of void children is indistinguishable from an empty one by value.
 */
export function childrenShort(
	definition: JobDefinition,
	declared: Envelope["slots"],
	state: FlowState,
): ChildrenShortError | undefined {
	const counted = slotsOf(definition).map((slot) => ({
		name: slot.name,
		expected: declared?.[slot.name],
		arrived: arrivedIn(state, slot.name),
	}))

	const unrecorded = counted.filter((slot) => slot.expected === undefined)

	if (unrecorded.length > 0) {
		return new ChildrenShortError(
			`jubs: the job "${definition.name}" waits on the slots ${listed(unrecorded.map((slot) => slot.name))}, and its envelope carries no record of how many children they were given — it was written by a build that did not record those counts, or it was enqueued with no children at all, and nothing left tells the two apart. Without the counts the runtime cannot know whether those slots are full or empty, and running the handler would risk reading an empty slot and completing green. Enqueue the flow again from the top with jobs.enqueue(definition, data, awaits), which builds the whole flow; one envelope on its own cannot be replayed into a flow.`,
		)
	}

	const short = counted.flatMap((slot) =>
		slot.expected !== undefined && slot.arrived < slot.expected
			? [`"${slot.name}" was given ${slot.expected} and ${slot.arrived} arrived`]
			: [],
	)

	if (short.length === 0) {
		return undefined
	}

	return new ChildrenShortError(
		`jubs: the job "${definition.name}" waits on children, and a slot holds fewer than it was enqueued with — ${short.join("; ")}. Redis writes nothing when a child leaves its parent, so the missing ones were most likely cancelled with jobs.cancel(id) or removed with another job of this flow. The handler would read that slot short and complete green, so the job is buried instead: enqueue the flow again with jobs.enqueue(definition, data, awaits).`,
	)
}

/**
 * What a slot was given, refused before Redis is touched.
 *
 * The types say all of this already, and they stop saying it at every widening
 * boundary — a `JobDefinition` passed as its own default type erases `Awaits`
 * and makes the third argument silently optional. A flow enqueued with a slot
 * missing would run its parent over an input that is short, so it is refused
 * here instead, where the definition is the only source consulted.
 *
 * It gives back the record it accepted, because narrowing and refusing are one
 * act here: every reader after this one holds a value these rules have proved.
 */
function assertAwaitsInput(definition: JobDefinition, awaits: unknown): Record<string, unknown> {
	const slots = slotsOf(definition)

	if (slots.length === 0) {
		return {}
	}

	if (!isRecord(awaits)) {
		throw new Error(
			`jubs: the job "${definition.name}" waits on the slots ${listed(slots.map((slot) => slot.name))}, and this enqueue passed nothing for them — jobs.enqueue(definition, data, awaits) takes what fills every slot as its third argument`,
		)
	}

	const missing = slots.filter((slot) => awaits[slot.name] === undefined)

	if (missing.length > 0) {
		throw new Error(
			`jubs: the job "${definition.name}" waits on the slots ${listed(slots.map((slot) => slot.name))}, and this enqueue passed nothing for ${listed(missing.map((slot) => slot.name))} — every slot a definition declares is filled at enqueue, because a flow enqueued short would run this job over an input it never had`,
		)
	}

	const stray = slots.find((slot) => slot.many !== Array.isArray(awaits[slot.name]))

	if (!stray) {
		return awaits
	}

	if (stray.many) {
		throw new Error(
			`jubs: the job "${definition.name}" waits on many of "${stray.definition.name}" in the slot "${stray.name}", and this enqueue passed one value — fill that slot with an array, or drop the array around the definition the slot declares to wait on exactly one`,
		)
	}

	throw new Error(
		`jubs: the job "${definition.name}" waits on exactly one "${stray.definition.name}" in the slot "${stray.name}", and this enqueue passed an array — fill that slot with a single value, or wrap the definition the slot declares in an array to wait on many`,
	)
}

/**
 * Reads what one child of a slot was given. A child that waits on nothing is
 * given its payload alone; a child that waits on something is given its payload
 * beside what its own slots take.
 *
 * A value in neither shape reads as nothing at all, and the refusals of the
 * node it becomes name it — its payload schema, or its own missing slots.
 *
 * The wrapper is read half by half rather than all or nothing, so a caller who
 * wrote one half and forgot the other is told which half is missing. Reading it
 * whole answered "you passed nothing" to somebody looking at the slots they had
 * just filled correctly.
 */
function childInput(slot: Slot, given: unknown): { data: unknown; awaits: unknown } {
	if (slotsOf(slot.definition).length === 0) {
		return { data: given, awaits: undefined }
	}

	if (isRecord(given)) {
		return { data: given.data, awaits: given.awaits }
	}

	return { data: undefined, awaits: undefined }
}

async function composeChild(slot: Slot, given: unknown): Promise<FlowChildNode> {
	const { data, awaits } = childInput(slot, given)
	const node = await composeFlow(slot.definition, data, awaits)

	return { ...node, slot: slot.name }
}

/**
 * Composes every child one slot was given, in the order they were written.
 *
 * A slot that holds many is read as the array it was proved to be:
 * `assertAwaitsInput` checks the arity of every slot against the definition
 * before anything here runs, so a guard here would only invent a silent empty
 * slot for an input that was already refused.
 */
function composeSlot(slot: Slot, awaits: Record<string, unknown>): Promise<FlowChildNode>[] {
	const given = awaits[slot.name]

	if (!slot.many) {
		return [composeChild(slot, given)]
	}

	return (given as readonly unknown[]).map((one) => composeChild(slot, one))
}

/**
 * Turns a definition and what fills its slots into the tree a driver enqueues,
 * validating every payload and resolving every delivery on the way down.
 *
 * The whole tree is built before it is handed over, so a node a rule refuses
 * stops the flow with nothing enqueued.
 *
 * Every node that waits on something carries how many children each of its
 * slots got, because this is the only moment that number is known: Redis holds
 * the children themselves and forgets them one by one as they are cancelled or
 * removed, with nothing left to compare against.
 *
 * `unique` is refused at **every** position, because BullMQ itself forbids
 * `deduplication` beside a `parent` and on any node with children. It is
 * refused here as well as at `defineJob`, and not only there: a definition that
 * declares no `awaits` is a perfectly ordinary job on its own, and still cannot
 * be deduplicated once it fills a slot of somebody else's flow.
 */
export async function composeFlow(
	definition: JobDefinition,
	data: unknown,
	awaits: unknown,
): Promise<FlowNode> {
	const filled = assertAwaitsInput(definition, awaits)

	const validated = await validatePayload(definition, data)
	const delivery = resolveDelivery(definition, validated)

	if (delivery.unique) {
		throw new Error(
			`jubs: the job "${definition.name}" declares \`unique\` and takes part in a flow — uniqueness does not apply inside a flow, at any position, so drop \`unique\` from its delivery or enqueue that job on its own with jobs.enqueue`,
		)
	}

	const composing = slotsOf(definition).map((slot) => ({
		name: slot.name,
		nodes: composeSlot(slot, filled),
	}))

	const children = await Promise.all(composing.flatMap((slot) => slot.nodes))

	const envelope: Envelope = {
		v: payloadVersion(definition),
		name: definition.name,
		data,
		origin: "flow",
	}

	if (composing.length === 0) {
		return { queue: definition.queue, envelope, delivery, children }
	}

	const slots = Object.fromEntries(composing.map((slot) => [slot.name, slot.nodes.length]))

	return { queue: definition.queue, envelope: { ...envelope, slots }, delivery, children }
}
