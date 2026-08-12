import type { Origin } from "@/Definition"
import type { SerializedError } from "@/Failure"

export interface JobEvent {
	readonly name: string
	readonly queue: string
	/**
	 * The id of the job the event is about, in the one form the whole API speaks:
	 * the same string `jobs.enqueue` gave the producer, and the one `jobs.get`,
	 * `jobs.retry` and `jobs.cancel` take.
	 */
	readonly id: string
	readonly attempt: number
	readonly origin: Origin
}

export interface JobFailureEvent extends JobEvent {
	readonly error: SerializedError
}

export type JobHook<Event extends JobEvent> = (event: Event) => void | Promise<void>

export interface JobHooks {
	readonly onStart?: JobHook<JobEvent>
	readonly onSuccess?: JobHook<JobEvent>
	readonly onAttemptFailed?: JobHook<JobFailureEvent>
	readonly onDead?: JobHook<JobFailureEvent>
}

export async function notify<Event extends JobEvent>(
	hook: JobHook<Event> | undefined,
	name: keyof JobHooks,
	event: Event,
): Promise<void> {
	if (!hook) {
		return
	}

	try {
		await hook(event)
	} catch (error) {
		console.error(
			`juibs: the ${name} hook threw for job "${event.name}"; the job outcome is unchanged`,
			error,
		)
	}
}
