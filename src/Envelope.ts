import type { Origin } from "@/Definition"

const ORIGINS: readonly Origin[] = ["direct", "schedule", "flow", "relay"]

export interface TraceContext {
	readonly traceparent: string
	readonly tracestate?: string
}

export interface Envelope {
	readonly v: number
	readonly name: string
	readonly data: unknown
	readonly origin: Origin
	readonly trace?: TraceContext
}

export class EnvelopeError extends Error {
	constructor(reason: string) {
		super(`jubs: the stored job is not a jubs envelope — ${reason}`)
		this.name = "EnvelopeError"
	}
}

function isOrigin(value: unknown): value is Origin {
	return ORIGINS.includes(value as Origin)
}

export function readEnvelope(stored: unknown): Envelope {
	if (typeof stored !== "object" || stored === null) {
		throw new EnvelopeError(`expected an object, got ${typeof stored}`)
	}

	const envelope = stored as Partial<Envelope>

	if (typeof envelope.v !== "number") {
		throw new EnvelopeError("its payload version `v` is missing")
	}

	if (typeof envelope.name !== "string" || envelope.name.length === 0) {
		throw new EnvelopeError("its job name is missing")
	}

	if (!isOrigin(envelope.origin)) {
		throw new EnvelopeError(`its origin ${JSON.stringify(envelope.origin)} is not a known origin`)
	}

	return { v: envelope.v, name: envelope.name, data: envelope.data, origin: envelope.origin }
}
