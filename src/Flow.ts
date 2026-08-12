import type { StandardSchemaV1 } from "@standard-schema/spec"
import { type JobDefinition, payloadVersion } from "@/Definition"
import { resolveDelivery } from "@/Delivery"
import type { ChildResult, FlowNode, FlowState } from "@/Driver"
import { validatePayload } from "@/Payload"

/**
 * What parts a flow child's job id into the definition name and the unique part
 * behind it. It is refused inside a definition name for that reason, and it is
 * neither a character BullMQ refuses in an id nor one that a Redis key or the
 * two-colon cut of a job key would read differently.
 */
export const FLOW_ID_MARKER = "~"

/**
 * One node of a flow, as the producer writes it: a definition, the payload it
 * takes, and the children it waits on.
 *
 * It is type-erased on purpose. Each node is typed where `child` is called,
 * against that definition's payload schema, and the tree it ends up in is not:
 * a heterogeneous tree carries a different type per node, which no caller can
 * spell and no reader can follow.
 */
export interface FlowChild {
	readonly definition: JobDefinition
	readonly data: unknown
	readonly children: readonly FlowChild[]
}

export interface FlowChildren {
	readonly children?: readonly FlowChild[]
}

/**
 * Describes one child of a flow, without touching Redis.
 *
 * It is the same pure data builder `defineJob` is — nothing is enqueued until
 * the tree it belongs to reaches `jobs.flow`.
 */
export function child<Payload extends StandardSchemaV1>(
	definition: JobDefinition<Payload>,
	data: StandardSchemaV1.InferInput<Payload>,
	options?: FlowChildren,
): FlowChild {
	return { definition, data, children: options?.children ?? [] }
}

/**
 * Why a job that waited on children died: at least one of them failed for good.
 *
 * It is internal, the way `ResultError` is. A handler never catches it — the
 * runtime buries the job before the handler runs — so the name it carries in
 * `cause` is all a reader needs.
 */
export class ChildDeadError extends Error {
	/** What the children that did finish returned, for the dead entry to keep. */
	readonly results: readonly ChildResult[]

	constructor(jobName: string, state: FlowState) {
		const reasons = state.failures
			.map((failure) => `"${failure.name}" (${failure.id}): ${failure.reason}`)
			.join("; ")

		super(
			`juibs: the job "${jobName}" waits on children, and one of them failed every attempt — ${reasons}`,
		)
		this.name = "ChildDeadError"
		this.results = state.results
	}
}

/**
 * Turns a flow the producer wrote into the tree a driver enqueues, validating
 * every payload and resolving every delivery on the way down.
 *
 * The whole tree is built before it is handed over, so a node a rule refuses
 * stops the flow with nothing enqueued.
 *
 * The two refusals are not the same width, and are not the same rule.
 * `unique` is refused at **every** position, because BullMQ itself forbids
 * `deduplication` beside a `parent` and on any node with children.
 * `idempotencyKey` is refused only on a node that **has** children, and that
 * one is juibs' own rule about what a job's input is: a parent is fed by its
 * children as much as by its payload, so a key derived from the payload alone
 * names two different runs. A leaf keeps its key, because a leaf really is its
 * payload alone.
 */
export async function composeFlow(node: FlowChild): Promise<FlowNode> {
	const { definition, data } = node
	const validated = await validatePayload(definition, data)
	const delivery = resolveDelivery(definition, validated)

	if (delivery.unique) {
		throw new Error(
			`juibs: the job "${definition.name}" declares \`unique\` and takes part in a flow — uniqueness does not apply inside a flow, at any position, so drop \`unique\` from its delivery or enqueue that job on its own with jobs.enqueue`,
		)
	}

	if (node.children.length > 0 && definition.idempotencyKey) {
		throw new Error(
			`juibs: the job "${definition.name}" declares \`idempotencyKey\` and waits on children — a key derived from the payload alone would replay the result of an earlier flow that ran different children, so drop \`idempotencyKey\` from that definition or give the key to the leaves instead`,
		)
	}

	const children = await Promise.all(node.children.map(composeDependentFlow))

	return {
		queue: definition.queue,
		envelope: { v: payloadVersion(definition), name: definition.name, data, origin: "flow" },
		delivery,
		children,
	}
}

/**
 * A child is stored under an id built from its definition name, so the name
 * survives a sweep that takes its hash away. Two characters cannot go in it:
 * `:` cuts a job key, and BullMQ refuses it in an id outright, while
 * `FLOW_ID_MARKER` is what parts the name from the unique half of the id.
 *
 * A root carries no id of ours, so its name is left alone — and `defineJob`
 * validates no character today, so a name holding either one works everywhere
 * else.
 */
function composeDependentFlow(node: FlowChild): Promise<FlowNode> {
	const stray = [":", FLOW_ID_MARKER].find((character) => node.definition.name.includes(character))

	if (stray) {
		throw new Error(
			`juibs: the job "${node.definition.name}" is a child of a flow and its name holds "${stray}" — a child is stored under an id built from its name, which that character breaks, so rename the job or make it the root of its own flow`,
		)
	}

	return composeFlow(node)
}
