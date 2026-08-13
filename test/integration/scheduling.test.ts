import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import {
	createJobs,
	cron,
	dailyAt,
	defineHandler,
	defineJob,
	every,
	type Origin,
	redisDriver,
} from "@/index"
import { waitFor } from "../support/Wait"
import { scoped } from "./namespace"
import { REDIS_URL } from "./redis"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("scheduling over redis", () => {
	test("start writes the declared scheduler, and a later start without it takes it away", async () => {
		const closeBooks = defineJob({
			name: scoped("billing.close"),
			queue: scoped("jubs.test.schedule.reconcile"),
			payload: type({ ledger: "string" }),
			schedule: dailyAt("09:30", { data: { ledger: "main" }, timezone: "America/Sao_Paulo" }),
		})

		const sendEmail = defineJob({
			name: scoped("email.send"),
			queue: closeBooks.queue,
			payload: type({ to: "string.email" }),
		})

		const queue = inspect(closeBooks.queue)
		await queue.obliterate({ force: true })

		await queue.upsertJobScheduler("legacy.nightly", { pattern: "0 3 * * *" })

		const driver = redisDriver(workerConnection)

		const scheduled = await createJobs({ driver, definitions: [closeBooks, sendEmail] }).start([
			defineHandler(closeBooks, async () => {}),
			defineHandler(sendEmail, async () => {}),
		])

		const schedulerKey = `jubs.${closeBooks.name}`
		const written = await queue.getJobSchedulers()
		const ours = written.find((scheduler) => scheduler.key === schedulerKey)

		expect(written.map((scheduler) => scheduler.key).toSorted()).toEqual(
			[schedulerKey, "legacy.nightly"].toSorted(),
		)

		expect(ours?.name).toBe(closeBooks.name)
		expect(ours?.pattern).toBe("30 9 * * *")
		expect(ours?.tz).toBe("America/Sao_Paulo")
		expect(ours?.template?.data).toEqual({
			v: 1,
			name: closeBooks.name,
			data: { ledger: "main" },
			origin: "schedule",
		})
		expect(ours?.template?.opts?.attempts).toBe(1)
		expect(ours?.template?.opts).not.toHaveProperty("delay")
		expect(ours?.template?.opts).not.toHaveProperty("deduplication")

		await scheduled.close()

		const dropped = await createJobs({ driver, definitions: [sendEmail] }).start([
			defineHandler(sendEmail, async () => {}),
		])

		const left = await queue.getJobSchedulers()

		expect(left.map((scheduler) => scheduler.key)).toEqual(["legacy.nightly"])

		await dropped.close()
	})

	test("a start that registers no definition keeps the scheduler of the handler it runs", async () => {
		const rotateKeys = defineJob({
			name: scoped("security.rotate"),
			queue: scoped("jubs.test.schedule.handlers"),
			payload: type({ scope: "string" }),
			schedule: dailyAt("04:00", { data: { scope: "api" } }),
		})

		const queue = inspect(rotateKeys.queue)
		await queue.obliterate({ force: true })

		const driver = redisDriver(workerConnection)

		const first = await createJobs({ driver }).start([defineHandler(rotateKeys, async () => {})])
		await first.close()

		const second = await createJobs({ driver }).start([defineHandler(rotateKeys, async () => {})])

		const written = await queue.getJobSchedulers()

		expect(written.map((scheduler) => scheduler.key)).toEqual([`jubs.${rotateKeys.name}`])
		expect(written[0]?.tz).toBe("UTC")

		await second.close()
	})

	test("a scheduled job runs with origin schedule, and the same job enqueued by hand with origin direct", async () => {
		const pingHealth = defineJob({
			name: scoped("health.ping"),
			queue: scoped("jubs.test.schedule.origin"),
			payload: type({ target: "string" }),
			schedule: every("1 second", { data: { target: "api" } }),
		})

		const queue = inspect(pingHealth.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection), definitions: [pingHealth] })
		const origins: Origin[] = []

		const runtime = await jobs.start([
			defineHandler(pingHealth, async (_data, context) => {
				origins.push(context.origin)
			}),
		])

		await waitFor(() => origins.includes("schedule"))

		await jobs.enqueue(pingHealth, { target: "api" })

		await waitFor(() => origins.includes("direct"))

		await runtime.close()
	})

	test("a start whose cron redis refuses names the job and the pattern", async () => {
		const sweepLogs = defineJob({
			name: scoped("logs.sweep"),
			queue: scoped("jubs.test.schedule.refused"),
			payload: type({ scope: "string" }),
			schedule: cron("a b c", { data: { scope: "all" } }),
		})

		const queue = inspect(sweepLogs.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection), definitions: [sweepLogs] })

		expect(() => jobs.start([defineHandler(sweepLogs, async () => {})])).toThrow(
			`jubs: redis refused the schedule of the job "${sweepLogs.name}" — correct the pattern "a b c"`,
		)

		expect(await queue.getJobSchedulers()).toEqual([])
	})
})
