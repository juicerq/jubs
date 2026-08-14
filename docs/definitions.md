# Definitions and handlers

## Defining a job

A definition is the producer-side description of a job: its name, its queue and its payload schema. `defineJob` returns a plain data object. It performs no I/O, so it is safe to import from any process.

`payload` is any [Standard Schema](https://standardschema.dev) validator — arktype, Zod and Valibot all qualify. The enqueue argument and the handler's `data` are both inferred from it.

```ts
// jobs/definitions.ts
import { defineJob } from "@juicerq/jubs"
import { type } from "arktype"

export const sendWelcomeEmail = defineJob({
	name: "email.welcome",
	queue: "mail",
	payload: type({ userId: "string", locale: "'en' | 'pt'" }),
})
```

## Handling a job

A handler is the consumer-side function that runs a job. One handler per definition. `defineHandler` binds the two, and `data` arrives already validated.

The second argument is the handler context: `id` is the job id, `attempt` is 1-based, `maxAttempts` is how many attempts this job gets in total, and `origin` says what caused the job to exist.

```ts
// jobs/handlers.ts
import { defineHandler } from "@juicerq/jubs"
import { sendWelcomeEmail } from "./definitions"

export const welcomeEmailHandler = defineHandler(sendWelcomeEmail, async (data, context) => {
	console.log(`attempt ${context.attempt} of ${context.maxAttempts} for job ${context.id}`)

	await mailer.send(data.userId, data.locale)
})
```

## Typing what a handler returns

`result` is the schema of the value the handler resolves. Like `payload`, it is any Standard Schema validator, and it sits on the definition — so the producer's side of the code knows the shape of the answer without importing the handler.

```ts
export const renderInvoice = defineJob({
	name: "invoice.render",
	queue: "reports",
	payload: type({ invoiceId: "string" }),
	result: type({ url: "string.url", bytes: "number" }),
})
```

The handler's return type is inferred from it. A definition that declares no `result` returns `unknown`, and jubs validates nothing at all.

The value is validated the moment the handler resolves, before anything is stored. jubs keeps what the schema gives back, never what the handler returned. The handler therefore returns the schema's **input**: a schema that transforms cannot validate its own output, which is the same rule `payload` follows.

The validated value is what the handler's return value becomes. No client method reads it back — `jobs.get` answers with the job's state, not with its result. Underneath it is BullMQ's own `returnvalue`, which jubs does not surface. The one place it comes back to you is a repeated delivery under an [idempotency key](./uniqueness.md#idempotency): the key replays the validated value, as the JSON projection of it, so a `Date` the schema produced comes back a string. The size limit described there applies to it.

A return value the schema rejects fails the attempt unrecoverably: it burns one attempt and is not retried, exactly as an invalid stored payload does. The failure is yours to fix in code, and five more attempts would only produce it again.
