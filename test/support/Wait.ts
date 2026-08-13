import type { Job, Queue } from "bullmq"

export async function waitFor(reached: () => boolean | Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 10_000

	while (Date.now() < deadline) {
		if (await reached()) {
			return
		}

		await Bun.sleep(25)
	}

	throw new Error("the condition expected by this test did not hold in time")
}

export async function waitForFinished(queue: Queue, id: string): Promise<Job> {
	let finished: Job | undefined

	await waitFor(async () => {
		finished = await queue.getJob(id)

		return !!finished?.finishedOn
	})

	if (!finished) {
		throw new Error(`job ${id} on ${queue.name} did not finish in time`)
	}

	return finished
}
