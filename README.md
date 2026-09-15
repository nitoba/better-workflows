# better-workflows

Durable workflows for **NestJS 12**, using decorators, modules, dependency injection and `async/await`. Effect's workflow/cluster engine is private infrastructure; application code does not import Effect.

**Status: `0.1.0-alpha.1`.** This repository contains an executable implementation, not just API declarations. It is an initial release candidate for application-level evaluation, not a claim that every proposed feature or failure mode is covered. No npm publication is required to run the repository.

## Run the example

Development uses Bun **1.4.2**. Compiled ESM consumers require Node **22.16+** or Bun.

```sh
git clone https://github.com/nitoba/better-workflows.git
cd better-workflows
bun install --frozen-lockfile
bun run check

# Generate a real JSON report and leave its workflow waiting for approval.
bun run example report-001

# A separate invocation reopens SQLite and resumes the same execution.
bun run example report-001 --approve
```

The example writes reports and SQLite state under `.demo/`; override `WORKFLOWS_DEMO_DIR` to use another directory. It does not simulate sending emails or generating PDFs. Reusing the same report ID and payload reuses its execution. Use another ID for another execution.

Build a package for local installation with `bun run build && bun pm pack`; install the resulting tarball in the consuming Nest application. The library is ESM-only. Enable `experimentalDecorators` and `emitDecoratorMetadata` in that application, as with standard Nest providers.

## Define activities and a workflow

Zod is used below only as an example. Contracts accept **Standard Schema v1**, including asynchronous validators.

```ts
import { z } from 'zod'
import {
  Activities,
  Activity,
  Workflow,
  defineSignal,
  type ActivityContext,
  type WorkflowContext
} from 'better-workflows'

const Approval = defineSignal('report.approval', z.boolean())
const Input = z.object({ reportId: z.string(), values: z.array(z.number()).min(1) })

@Activities()
export class ReportActivities {
  @Activity({
    name: 'reports.sum',
    version: 1,
    queue: 'reports',
    input: z.array(z.number()),
    output: z.number(),
    timeout: '30s'
  })
  async sum(values: number[], _ctx: ActivityContext): Promise<number> {
    return values.reduce((total, value) => total + value, 0)
  }
}

@Workflow({
  name: 'reports',
  version: 1,
  input: Input,
  output: z.number(),
  signals: [Approval],
  idempotencyKey: (input) => input.reportId
})
export class GenerateReport {
  async run(input: z.infer<typeof Input>, ctx: WorkflowContext): Promise<number> {
    const sum = await ctx.activities(ReportActivities).sum(input.values, { stepId: 'sum' })
    await ctx.sleep('review-delay', '1s')
    const approved = await ctx.waitForSignal('approval', Approval, { timeout: '7d' })
    return approved ? sum : 0
  }
}
```

Both class decorators include Nest's injectable metadata. Activities can inject the application's ordinary services using the Nest constructor. The activity context belongs to an invocation, not to mutable fields on a singleton.

`ctx.activities()` returns a typed **durable client**, not the provider instance. Its calls schedule and await durable work. Calling an activity provider directly is an ordinary method call and does not create a durable step.

## Register with Nest

```ts
import { Module } from '@nestjs/common'
import { WorkflowsModule } from 'better-workflows'
import { sqlite } from 'better-workflows/sqlite'

@Module({
  imports: [WorkflowsModule.forFeature([GenerateReport])],
  providers: [GenerateReport, ReportActivities],
  exports: [WorkflowsModule]
})
export class ReportsModule {}

@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'reports-app',
      storage: sqlite({ filename: './data/workflows.sqlite' }),
      queues: { reports: { concurrency: 2 } }
    }),
    ReportsModule
  ]
})
export class AppModule {}
```

Register **one root** per Nest application. Its infrastructure is global. `forFeature()` creates typed clients only: handlers still belong in the providers of their owning module. Import/export that module rather than registering duplicate handler instances. `forRootAsync({ imports, inject, useFactory })` supports configuration providers.

Handlers must be singleton providers with a static dependency tree. Request-scoped dependencies and non-singleton handlers are rejected. Background execution does not retain an HTTP request. HTTP pipes, guards and interceptors are not silently applied to activities. Apply authorization in the application's controllers/services before accepting, inspecting, signalling or cancelling executions.

## Start, inspect and control

```ts
import { Injectable } from '@nestjs/common'
import { InjectWorkflow, WorkflowClient } from 'better-workflows'

@Injectable()
export class ReportsService {
  constructor(
    @InjectWorkflow(GenerateReport)
    readonly reports: WorkflowClient<typeof GenerateReport>
  ) {}
}

const handle = await reportsService.reports.start({ reportId: 'r-1', values: [10, 20] })
console.log(handle.executionId, handle.created)

await handle.signal(Approval, true, { idempotencyKey: 'approval-event-1' })
const result = await handle.result({ timeout: '30s' })
const status = await handle.describe()
const page = await handle.history({ after: 0, limit: 100 })
// page.nextCursor, when present, is the next request's `after` value.
```

`start()` resolves after a transaction durably accepts the execution and its dispatch outbox record. Acceptance does not mean completion or even that a worker has started. This is suitable for an HTTP `202` response; the library does not install public HTTP endpoints.

Retrieve a handle later with `client.getHandle(executionId)`. `result({ timeout, signal })` only bounds the caller's local wait; it does **not** cancel the workflow.

`pause()` prevents advancement at durable command boundaries. Already running activities may finish. `resume()` releases a pause; it does not reset failed executions. `cancel({ reason })` requests durable cooperative cancellation and aborts running activities through their `AbortSignal`. Cancellation is not rollback. `app.close()` or process restart does not imply cancellation. Enable Nest shutdown hooks for signal-driven graceful shutdown.

## Retries and idempotency

```ts
@Activity({
  name: 'reports.deliver', version: 1, queue: 'delivery',
  input: DeliveryInput, output: DeliveryReceipt,
  retry: { maxAttempts: 3, backoff: 'exponential', initialDelay: '1s', maxDelay: '30s' },
  timeout: '1m'
})
```

`maxAttempts` counts total **business attempts**, including the first one. Throw `new ActivityError({ code, message, retryable: true })` to opt into retries. Unexpected exceptions are not assumed transient. A timeout is retryable, subject to the same policy. Attempts and retry deadlines are persisted; failure and the next deadline are committed together. Infrastructure redeliveries do not reset the business attempt counter.

Use `ctx.idempotencyKey` with external providers or your own database uniqueness constraints. It is stable across retries and infrastructure redeliveries of the same step. `ctx.attempt` identifies the business attempt; `ctx.signal` supports cooperative abort and `await ctx.heartbeat(details)` records progress.

**Delivery is at least once.** A crash after an external side effect but before its durable result is stored can repeat that side effect. Fencing protects journal result commits, not remote providers. Never charge, email or mutate external state assuming exactly-once execution.

Workflow deduplication uses `(namespace, workflow name, idempotency key)`, excluding version. The same key with a different payload fails with `IDEMPOTENCY_CONFLICT`. Without a configured or supplied key, each start creates a new execution. A new version's client can inspect an older execution, but reading its differently typed result requires that execution's versioned client.

## Durability rules

Keep workflow code deterministic. Put I/O, changing database reads, random values and external calls in activities. Always await durable commands. Use unique, stable `stepId` values; do not derive them from time, random IDs or unstable array positions. IDs, command order, payload and retry policy are checked against history; incompatible changes fail instead of silently running a different process.

A suspension abandons the current async round without rejecting its await, so it does not enter user `catch/finally`. A later round replays the workflow. Ordinary business failures still reach `try/catch`. Resources and external side effects do not belong in workflow-body `finally` blocks. Never retain a workflow context or start unawaited background tasks from a workflow.

Transport values must be stable plain JSON, with a 1 MiB encoded limit. A root `undefined` can represent a void result. Dates, BigInts, classes, getters, cyclic objects, sparse arrays and lossy nested undefined values are rejected. Encode these explicitly in the application. General codecs/type-changing schema transforms are not part of this release.

Signals are FIFO and durable, including delivery before a wait is registered. Event keys deduplicate deliveries. Signal acceptance, consumption and timeout decisions are serialized in SQL; replay consumes the same stored outcome, not a second event.

## Storage and distributed execution

SQLite is intended for a **single application process on one host**, with a persistent file. It is selected automatically for Bun or Node using the matching Effect driver. `:memory:` is only for tests. Do not share a SQLite database file across hosts.

```ts
import { postgres } from 'better-workflows/postgres'

WorkflowsModule.forRoot({
  namespace: 'reports-app',
  storage: postgres({ connectionString: databaseUrl, maxConnections: 10 }),
  topology: 'distributed',
  cluster: { address: { host: 'worker-1.internal', port: 34431 } },
  execution: {
    workflows: { enabled: true, concurrency: 20 },
    activities: { enabled: true, queues: ['reports'] }
  },
  queues: { reports: { concurrency: 2 } }
})
```

The address must be reachable by other runners. Optional `cluster.listenAddress` can differ from the advertised address. Keep runner sockets on a trusted private network; the library does not configure a public authenticated gateway.

For API-only or activity-only processes, set `execution.workflows.enabled: false`; no runner address is required. API-only processes also set `execution.activities.enabled: false`. Orchestrators need workflow handlers; activity workers need activity handlers. Contract classes still need to be shared and imported by producers/orchestrators. Use the same namespace and database across roles. A logical queue's concurrency limit is **per process**, shared by its different activity handlers, not a distributed global limit.

## Scope of this alpha

Implemented: native Nest registration/DI, durable acceptance, replay, queued activities, SQL journal/history, signal inbox, timers, persisted retries, worker result fencing, versioned definitions, pause/cancel/resume and SQLite/PostgreSQL infrastructure adapters.

**Not implemented:** `ctx.map`, child workflows, sagas/compensations, scheduling decorators, a virtual-clock testing module, administrative migrations/retention tools, global/per-key concurrency, a queue-transport plugin API, `better-nest-mq` integration, UI or npm publishing automation. `Promise.all` is not a durable parallel primitive here; commands are interpreted sequentially. These are not hidden no-op options.

Schema creation/migration runs automatically at bootstrap for the pinned engine and the journal. There is no `migrations: 'validate'` option yet. There is no automatic history cleanup: retain storage, protect backups and account for growth. Do not manually prune individual journal/queue rows from active executions.

See [architecture and reliability](docs/architecture.md) for storage boundaries and testing limitations.
