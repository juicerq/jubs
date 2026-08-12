import { type } from "arktype"
import { createJobs, defineHandler, defineJob } from "@/index"
import { memoryDriver } from "@/testing/index"

const settlePayment = defineJob({
	name: "payments.settle",
	queue: "payments",
	payload: type({ id: "string" }),
	timeoutMs: 20,
	idempotencyKey: (data) => data.id,
})

const driver = memoryDriver()
const jobs = createJobs({ driver })

const runtime = await jobs.start([defineHandler(settlePayment, () => new Promise<void>(() => {}))])

await jobs.enqueue(settlePayment, { id: "pay-1" })
await driver.runNext().catch(() => {})
await runtime.close()
