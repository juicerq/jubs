# Dashboard

`@juicerq/jubs/dashboard` mounts a [Bull Board](https://github.com/felixmosh/bull-board) over the queues your definitions use. It is the read side of the library: what is waiting, what failed, what a job's envelope holds, and what is sitting in a dead queue.

The two Bull Board packages are optional peers. This subpath is the only thing that imports them, so a process that never mounts a board never loads them.

```sh
npm install @bull-board/api @bull-board/express
```

Two packages, not three: your server is not one of them. `@bull-board/express` carries Express as a dependency of its own, and `@bull-board/fastify` carries the `@fastify/*` plugins it renders through — it does not carry Fastify, which you already have, since the board mounts on the app you built. Swap one server package for the other and the install line is the same length. A peer that is not installed comes back as that very command instead of as a module resolution failure. Only a missing module is rewritten that way — a peer that is installed and throws while it loads keeps its own error, because telling you to install a package you already have would send you away from the real failure.

**Express.** The router mounts where you say, and `basePath` has to be that same place.

```ts
// server.ts
import { expressDashboard } from "@juicerq/jubs/dashboard"
import express from "express"
import Redis from "ioredis"
import * as definitions from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379")

const basePath = "/admin/queues"

const app = express()

app.use(
	basePath,
	await expressDashboard({
		connection,
		definitions: Object.values(definitions),
		deadQueues: ["billing"],
		basePath,
	}),
)
```

**Fastify.** The plugin registers under a prefix, and `basePath` has to be that prefix.

```ts
// server.ts
import { fastifyDashboard } from "@juicerq/jubs/dashboard"
import Fastify from "fastify"
import Redis from "ioredis"
import * as definitions from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379")

const basePath = "/admin/queues"

const app = Fastify()

await app.register(
	await fastifyDashboard({
		connection,
		definitions: Object.values(definitions),
		deadQueues: ["billing"],
		basePath,
	}),
	{ prefix: basePath },
)
```

Both functions are async because the peer is imported when they run.

**Pass the connection you built, not a bag of options.** `connection` is BullMQ's `ConnectionOptions`, so an options object typechecks — and it is the wrong shape here. The board opens one `Queue` per queue it shows and closes none of them, exactly as `redisDriver` does, because a connection belongs to whoever made it. A shared client is one client, however many queues open over it. An options object is not shared: every `Queue` builds clients of its own, nothing here ever closes them, and a process that only mounts the board never exits. It needs no `maxRetriesPerRequest: null` either, since a board opens queues and starts no worker.

`basePath` is not a convenience. The adapter builds the board's own links and its API routes out of it before the board exists, so a board mounted at `/admin/queues` and told `/queues` serves a page whose every link points at nothing. Write the path once and pass the same string to both places.

### Mounting on any other framework

`expressDashboard` and `fastifyDashboard` build a server adapter for you. `mountDashboard` takes one you built yourself, and that is how the board reaches a framework jubs does not name.

The Bull Board publishes an adapter per framework, and there are eight of them: `@bull-board/express` and `@bull-board/fastify`, plus `@bull-board/hono`, `@bull-board/koa`, `@bull-board/elysia`, `@bull-board/hapi`, `@bull-board/nestjs` and `@bull-board/h3`. jubs names two and installs none. `mountDashboard` serves all eight, and whatever the ninth turns out to be, because it never imports an adapter — it takes one.

```ts
// server.ts
import { ExpressAdapter } from "@bull-board/express"
import { mountDashboard } from "@juicerq/jubs/dashboard"
import express from "express"
import Redis from "ioredis"
import * as definitions from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379")

const basePath = "/admin/queues"

const serverAdapter = new ExpressAdapter()

await mountDashboard({
	connection,
	definitions: Object.values(definitions),
	deadQueues: ["billing"],
	basePath,
	serverAdapter,
})

const app = express()

app.use(basePath, serverAdapter.getRouter())
```

`mountDashboard` hands nothing back. The adapter is the thing you hold, and you mount it by whatever call that adapter offers. Swap `ExpressAdapter` for `HonoAdapter` and three lines change — the import, the constructor and the mount; the `mountDashboard` call does not. jubs tests no adapter but these two, so any other is yours to verify.

The two named functions earn their place on one point: they convert a missing peer into the command that installs it, because they are the ones doing the import. An adapter you construct was imported by you, so a package you have not installed fails at your own import line, with your own runtime's error — which is where it belongs.

### Which queues the board shows

The queues of the definitions you pass, and the dead queue of every name in `deadQueues`. Nowhere else. The board resolves no queue name from a URL, and offers no box to type one into.

That is a decision about Redis, not about the screen. Constructing a `Queue` writes `bull:<name>:meta`, and nothing ever takes it back. A board that opened the queue somebody typed would turn each typo into a permanent key on a production Redis, and the queue list would slowly fill with names nobody meant to create. So `definitions` is the whole vocabulary, and it is the same list you gave `createJobs({ definitions })`.

`deadQueues` names live queues, exactly as `createJobs({ deadQueues })` does, and the board shows `<queue>.dead` for each. **A dead queue no definition uses is refused for the same reason**, and nothing is opened at all — the error names the queues the definitions do use, so a typo is a failure at boot rather than a key that outlives it. A board given no definitions is refused too, since it would open with nothing to show, and the error names all three mounting functions.

Every name comes out once. Many definitions share one queue, and registering a name twice would open two `Queue` handles over one set of Redis keys — worse, the board keeps its queues in a map, so the second registration would replace the first and take a queue off the screen with nothing said. The narrow case of that is a definition whose queue is literally `x.dead` while `x` is one of your `deadQueues`: those are the same Redis keys twice over, and they come out once, as the dead queue. Read only is the safe half of that pair.

### What the board shows about a job

A job's `data` on the board is the jubs envelope, not your payload. `v` is the payload version, `name` is the job name, `data` is the payload itself, and `origin` says what caused the job to exist.

| `origin`   | What made the job                                                |
| ---------- | ---------------------------------------------------------------- |
| `direct`   | an enqueue somebody made                                         |
| `schedule` | one occurrence of a [schedule](./scheduling.md)                  |
| `flow`     | a node of a [flow](./flows.md)                                   |
| `relay`    | a row the [outbox relay](./outbox.md#outbox-and-relay) delivered |

`origin` is how a scheduled run is told apart from a hand-made one on the screen. The two run the same handler on the same queue and look alike everywhere else — the envelope is the only place that difference was written down, and it is the same field `context.origin` reads inside the handler.

Reading the board over HTTP answers for one queue at a time. `GET <basePath>/api/queues?activeQueue=<queue>&status=latest` lists every queue with its counts, and fills `jobs` for the queue named in `activeQueue` alone; every other queue comes back with an empty `jobs`. Ask once per queue if you script against it.

### Read only until you ask otherwise

Every queue the board shows is read only by default, live and dead alike. The write buttons are gone from the screen, and a write that reaches the API behind them anyway is refused there too — a `PATCH` on a job scheduler comes back `405`, with `QUEUE_READ_ONLY`.

Two of those buttons cannot be made safe from here, and they are the whole reason for the default.

**"Add job" writes a job no worker can read.** What you type becomes the job's `data`, and a jubs worker reads `data` as an envelope. A bare payload carries no `name` and no `v`, so the delivery fails before a handler is even chosen — and nothing reports it, because a hook needs a job name to report it under and there is none. `jobs.enqueue` is what validates a payload and writes an envelope around it.

**A scheduler edited on the screen looks applied and is not.** `start` reconciles the schedules of a queue from the handlers **that process** passed it, and rewrites or removes every `jubs.*` scheduler the code no longer declares. So an edit made on the board lives until the next boot of any process that starts that queue — and reconciliation is scoped to the queue, not to the process, so that boot may be one you did not deploy. Naming the scheduler something else survives reconciliation instead, which is worse: the jobs it produces carry whatever data you typed, and the worker refuses them exactly as above. The recurrence lives on the definition. Change it there and deploy — see [Scheduling](./scheduling.md).

**The board says this on the screen.** jubs writes a description for every queue it registers, and the board shows it, so the operator looking at a queue reads why the buttons are missing and which call to reach for instead — `jobs.retry(id)`, `jobs.cancel(id)`, `jobs.pause(queue)` and `jobs.resume(queue)` on a live queue, `jobs.dead.replay(id)` and `jobs.dead.discard(id)` on a dead one.

`readOnly: false` takes the buttons back, and it is deliberate opt-in.

```ts
await expressDashboard({
	connection,
	definitions: Object.values(definitions),
	deadQueues: ["billing"],
	basePath,
	readOnly: false,
})
```

What comes back with them is the pair above, so a writable queue is described on the screen as writable and names both hazards there. Everything else the board offers — retrying a failed job, promoting a delayed one, pausing the queue — acts on Redis exactly as `jobs.retry` and `jobs.pause` do, and jubs treats the board as it treats any other operator.

**A dead queue ignores `readOnly` entirely.** `<queue>.dead` is read only whatever you pass, because every action the board offers over a burial is wrong: nothing consumes the queue, so a retry never runs, and a clean or an empty destroys the very record the queue exists to keep. The way back is `jobs.dead.replay(id)`, which enqueues the job again from the stored envelope and then drops the entry, or `jobs.dead.discard(id)`, which drops it without enqueueing — see [Dead queue](./operations.md#dead-queue).
