import { type JobDefinition, type Origin, payloadVersion } from "@/Definition"

const ORIGINS: readonly Origin[] = ["direct", "schedule", "flow", "relay"]

export interface Envelope {
	readonly v: number
	readonly name: string
	readonly data: unknown
	readonly origin: Origin
	/**
	 * How many children each slot of this job was enqueued with, written for
	 * every node of a flow that waits on something.
	 *
	 * Redis keeps no record of the shape a parent was given: the only write is
	 * one member per child in the parent's `:dependencies`, and a child that is
	 * cancelled or removed is taken out of that set with nothing left behind. A
	 * slot that lost a child is then byte-for-byte a slot enqueued smaller. This
	 * field is the only place the declared shape survives, so the dispatch can
	 * tell the two apart and refuse to run a handler over a slot that went short.
	 *
	 * It is optional because it is a stored format, the way `v` is: an envelope
	 * written by a build where this job waited on nothing carries none, and that
	 * absence is read for what it is — a job that has no children at all.
	 */
	readonly slots?: Readonly<Record<string, number>>
}

export class EnvelopeError extends Error {
	constructor(reason: string) {
		super(`jubs: the stored job is not a jubs envelope — ${reason}`)
		this.name = "EnvelopeError"
	}
}

function isOrigin(value: unknown): value is Origin {
	return ORIGINS.some((origin) => origin === value)
}

/**
 * Reads the child counts back, refusing anything that is not a count.
 *
 * A malformed record is refused rather than dropped, because absence is a
 * decision the dispatch acts on: an envelope whose counts were read as missing
 * would be buried as one written before this job waited on children, which is a
 * different fact and a different fix.
 */
function readSlots(stored: unknown): Readonly<Record<string, number>> | undefined {
	if (stored === undefined) {
		return undefined
	}

	if (typeof stored !== "object" || stored === null || Array.isArray(stored)) {
		throw new EnvelopeError(`its child counts ${JSON.stringify(stored)} are not an object`)
	}

	const counts: [string, unknown][] = Object.entries(stored)
	const whole: Record<string, number> = {}

	for (const [slot, count] of counts) {
		if (typeof count !== "number" || !Number.isInteger(count)) {
			throw new EnvelopeError(
				`its child count for the slot "${slot}" is ${JSON.stringify(count)}, which is not a whole number of children`,
			)
		}

		whole[slot] = count
	}

	return whole
}

export function readEnvelope(stored: unknown): Envelope {
	if (typeof stored !== "object" || stored === null) {
		throw new EnvelopeError(`expected an object, got ${typeof stored}`)
	}

	const v: unknown = Reflect.get(stored, "v")
	const name: unknown = Reflect.get(stored, "name")
	const data: unknown = Reflect.get(stored, "data")
	const origin: unknown = Reflect.get(stored, "origin")
	const rawSlots: unknown = Reflect.get(stored, "slots")

	if (typeof v !== "number") {
		throw new EnvelopeError("its payload version `v` is missing")
	}

	if (typeof name !== "string" || name.length === 0) {
		throw new EnvelopeError("its job name is missing")
	}

	if (!isOrigin(origin)) {
		throw new EnvelopeError(`its origin ${JSON.stringify(origin)} is not a known origin`)
	}

	const read: Envelope = {
		v,
		name,
		data,
		origin,
	}

	const slots = readSlots(rawSlots)

	if (!slots) {
		return read
	}

	return { ...read, slots }
}

export function writeEnvelope(definition: JobDefinition, data: unknown, origin: Origin): Envelope {
	return {
		v: payloadVersion(definition),
		name: definition.name,
		data,
		origin,
	}
}
