import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { inAtomicBlock } from "@/Atomic"
import { createJobs, defineHandler, defineJob, type JobDriver } from "@/index"
import { memoryDriver } from "@/testing/index"
import { liveId } from "../support/JobIds"
import { type RecordingDriver, recordingDriver } from "./support/RecordingDriver"

const sendEmail = defineJob({
	name: "email.send",
	queue: "mail",
	payload: type({ to: "string.email", subject: "string" }),
})

const notifyOnce = defineJob({
	name: "email.notify",
	queue: "mail",
	payload: type({ to: "string.email" }),
	delivery: { attempts: 1 },
})

const renderInvoice = defineJob({
	name: "invoice.render",
	queue: "billing",
	payload: type({ order: "string" }),
	result: type({ url: "string.url" }),
})

const mailInvoice = defineJob({
	name: "invoice.mail",
	queue: "mail",
	payload: type({ order: "string" }),
	awaits: { render: renderInvoice },
})

function gate(): { open: Promise<void>; unlock: () => void } {
	let unlock = () => {}

	const open = new Promise<void>((resolve) => {
		unlock = resolve
	})

	return { open, unlock }
}

function refusingTheNth(driver: RecordingDriver, nth: number): JobDriver {
	return {
		...driver,

		async enqueue(request) {
			if (driver.enqueued.length === nth - 1) {
				throw new Error("redis went away")
			}

			return driver.enqueue(request)
		},
	}
}

function reportingItsScope(driver: RecordingDriver, seen: boolean[]): JobDriver {
	return {
		...driver,

		async consume(request) {
			seen.push(inAtomicBlock())

			return driver.consume(request)
		},
	}
}

describe("atomic", () => {
	test("holds every enqueue made inside the block and delivers them when it resolves", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.atomic(async () => {
			await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "first" })
			await jobs.enqueue(sendEmail, { to: "grace@example.com", subject: "second" })

			expect(driver.enqueued).toEqual([])
		})

		expect(driver.enqueued.map((record) => record.envelope.data)).toEqual([
			{ to: "ada@example.com", subject: "first" },
			{ to: "grace@example.com", subject: "second" },
		])
	})

	test("delivers nothing when the block throws", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.atomic(async () => {
				await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "first" })

				throw new Error("the order was refused")
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe("the order was refused")
		expect(driver.enqueued).toEqual([])
	})

	test("holds a whole flow, and delivers it when the block resolves", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.atomic(async () => {
			await jobs.enqueue(mailInvoice, { order: "order-1" }, { render: { order: "order-1" } })

			expect(driver.flows).toEqual([])
		})

		expect(driver.flows).toHaveLength(1)
		expect(driver.flows[0]?.children[0]?.slot).toBe("render")
	})

	test("the enqueue a block holds answers with nothing, because no job exists yet", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.atomic(async () => {
			const held = await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "first" })

			expect(held).toBeNull()
		})

		const enqueued = await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "second" })

		expect(liveId(enqueued)).toBe("mail:2")
	})

	test("a block opened inside another block delivers nothing of its own", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.atomic(async () => {
			await jobs.atomic(async () => {
				await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "inner" })
			})

			expect(driver.enqueued).toEqual([])

			await jobs.enqueue(sendEmail, { to: "grace@example.com", subject: "outer" })
		})

		expect(driver.enqueued.map((record) => record.envelope.data)).toEqual([
			{ to: "ada@example.com", subject: "inner" },
			{ to: "grace@example.com", subject: "outer" },
		])
	})

	test("two blocks running at once never see each other's held jobs", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const second = gate()
		let seenByFirst: unknown[] = []

		const first = jobs.atomic(async () => {
			await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "first" })

			await second.open

			seenByFirst = driver.enqueued.map((record) => record.envelope.data)
		})

		await jobs.atomic(async () => {
			await jobs.enqueue(sendEmail, { to: "grace@example.com", subject: "second" })
		})

		second.unlock()

		await first

		expect(seenByFirst).toEqual([{ to: "grace@example.com", subject: "second" }])
		expect(driver.enqueued.map((record) => record.envelope.data)).toEqual([
			{ to: "grace@example.com", subject: "second" },
			{ to: "ada@example.com", subject: "first" },
		])
	})

	test("refuses a tx while no outbox is configured, before the block runs", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let ran = false

		expect(() =>
			jobs.atomic(
				async () => {
					ran = true
				},
				{ tx: {} },
			),
		).toThrow("no outbox configured")

		expect(ran).toBe(false)
	})
})

describe("an enqueue that outlives its atomic block", () => {
	test("is refused when a callback the block scheduled enqueues after it ended", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const late = Promise.withResolvers<unknown>()

		await jobs.atomic(async () => {
			setImmediate(async () => {
				late.resolve(
					await jobs
						.enqueue(sendEmail, { to: "ada@example.com", subject: "late" })
						.catch((error: unknown) => error),
				)
			})
		})

		expect(((await late.promise) as Error).message).toContain("already ended")
		expect(driver.enqueued).toEqual([])
	})

	test("is refused when the block threw, so nothing lands after a block that delivered nothing", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const late = Promise.withResolvers<unknown>()

		await jobs
			.atomic(async () => {
				setImmediate(async () => {
					late.resolve(
						await jobs
							.enqueue(sendEmail, { to: "ada@example.com", subject: "late" })
							.catch((error: unknown) => error),
					)
				})

				throw new Error("the order was refused")
			})
			.catch(() => {})

		expect(((await late.promise) as Error).message).toContain("already ended")
		expect(driver.enqueued).toEqual([])
	})

	test("is refused when a promise the block never waited for enqueues after it ended", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const resume = gate()
		let floating: Promise<unknown> = Promise.resolve()

		await jobs.atomic(async () => {
			floating = resume.open.then(() =>
				jobs
					.enqueue(sendEmail, { to: "ada@example.com", subject: "floating" })
					.catch((error: unknown) => error),
			)
		})

		resume.unlock()

		expect(((await floating) as Error).message).toContain("already ended")
		expect(driver.enqueued).toEqual([])
	})
})

describe("a flush that fails", () => {
	test("names how many jobs it delivered and how many it abandoned untried", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver: refusingTheNth(driver, 3) })

		const failure = await jobs
			.atomic(async () => {
				await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: "first" })
				await jobs.enqueue(sendEmail, { to: "grace@example.com", subject: "second" })
				await jobs.enqueue(sendEmail, { to: "alan@example.com", subject: "third" })
				await jobs.enqueue(sendEmail, { to: "edsger@example.com", subject: "fourth" })
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("delivered 2 of the 4 jobs")
		expect((failure as Error).message).toContain("1 after it were abandoned")
		expect((failure as Error).cause).toBeInstanceOf(Error)
		expect(driver.enqueued.map((record) => record.envelope.data)).toEqual([
			{ to: "ada@example.com", subject: "first" },
			{ to: "grace@example.com", subject: "second" },
		])
	})
})

describe("a runtime started inside an atomic block", () => {
	test("consumes outside the block, so what its handlers enqueue is never held", async () => {
		const driver = recordingDriver()
		const openAtConsume: boolean[] = []
		const jobs = createJobs({ driver: reportingItsScope(driver, openAtConsume) })

		const runtime = await jobs.atomic(() => jobs.start([defineHandler(sendEmail, async () => {})]))

		await runtime.close()

		expect(openAtConsume).toEqual([false])
	})
})

describe("replaying a dead job inside an atomic block", () => {
	test("is refused, and the dead record stays where it is", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({
			driver,
			definitions: [notifyOnce],
			deadQueues: ["mail"],
		})

		await jobs.start([
			defineHandler(notifyOnce, async () => {
				throw new Error("the mailbox was full")
			}),
		])

		await jobs.enqueue(notifyOnce, { to: "ada@example.com" })
		await driver.drain().catch(() => {})

		const [buried] = await jobs.dead.list("mail")

		const failure = await jobs
			.atomic(async () => {
				await jobs.dead.replay(buried?.id ?? "")
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("inside an atomic block")
		expect(await jobs.dead.list("mail")).toHaveLength(1)
	})

	test("runs from a callback that outlived the block, because no block is open there", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({
			driver,
			definitions: [notifyOnce],
			deadQueues: ["mail"],
		})
		const replayed = Promise.withResolvers<unknown>()

		await jobs.start([
			defineHandler(notifyOnce, async () => {
				throw new Error("the mailbox was full")
			}),
		])

		await jobs.enqueue(notifyOnce, { to: "ada@example.com" })
		await driver.drain().catch(() => {})

		const [buried] = await jobs.dead.list("mail")

		await jobs.atomic(async () => {
			setImmediate(() => {
				replayed.resolve(jobs.dead.replay(buried?.id ?? "").catch((error: unknown) => error))
			})
		})

		expect(await replayed.promise).not.toBeInstanceOf(Error)
		expect(await jobs.dead.list("mail")).toEqual([])
	})
})
