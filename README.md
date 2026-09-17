# better-workflows

Durable workflows for **NestJS 12**, using decorators, modules, dependency injection and `async/await`. Effect's workflow/cluster engine is private infrastructure; application code does not import Effect.

**Status: `0.1.0-alpha.8`.** This repository contains an executable implementation, not just API declarations. It is an initial release candidate for application-level evaluation, not a claim that every proposed feature or failure mode is covered. No npm publication is required to run the repository.

## Run the example

Development uses Bun **1.4.2**. Compiled ESM consumers require Node **22.16+** or Bun.

PostgreSQL integration tests use [Testcontainers](https://testcontainers.com/) and require a Docker-compatible runtime. Run them with `bun run test:postgres`; the command starts a disposable PostgreSQL 16 container, runs the multi-process tests, and removes the container afterward. Rootless Podman is supported when its user socket is available.

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
  defineQueue,
  type ActivityContext,
  type WorkflowContext
} from 'better-workflows'

const ReportQueue = defineQueue('reports')
const Approval = defineSignal('report.approval', z.boolean())
const Input = z.object({ reportId: z.string(), values: z.array(z.number()).min(1) })

@Activities({ queue: ReportQueue })
export class ReportActivities {
  @Activity({
    name: 'reports.sum',
    version: 1,
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

`@Activities` and `@Workflow` include Nest's injectable metadata. Activities and
workflow handlers can inject the application's ordinary services using the Nest
constructor. `@WorkflowContract` is intentionally metadata-only. Context objects
belong to an invocation, not to mutable fields on a singleton.

`ctx.activities()` returns a typed **durable client**, not the provider instance. Its calls schedule and await durable work. Calling an activity provider directly is an ordinary method call and does not create a durable step.

## Register with Nest

```ts
import { Module } from '@nestjs/common'
import { WorkflowsModule } from 'better-workflows'
import { sqlite } from 'better-workflows/sqlite'

@Module({
  imports: [
    WorkflowsModule.forFeature({
      name: 'reports',
      workflows: [GenerateReport],
      activities: [ReportActivities],
      queues: [{ queue: ReportQueue, concurrency: 2 }]
    })
  ],
  exports: [WorkflowsModule]
})
export class ReportsModule {}

@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'reports-app',
      storage: sqlite({ filename: './data/workflows.sqlite' })
    }),
    ReportsModule
  ]
})
export class AppModule {}
```

Register **one root** per Nest application. It configures infrastructure and optional application defaults/queue overrides; it does not need to know domain queues. `forFeature()` owns implementations, their typed clients and local configuration. Do not also register those handlers in the outer module's `providers`. Use feature `imports`/`providers` for dependencies or `{ provide, useExisting }` to reuse an exported instance.

`forFeature({ clients: [GenerateReport] })` registers clients only, without constructing workflow services. `activityContracts` provides remote activity metadata without constructing workers. Private feature queues/activities are shared through explicit exports and actual Nest imports. `forRootAsync()` and `forFeatureAsync()` support configuration providers. Infrastructure is global by default; `isGlobal: false` requires explicit infrastructure imports.

## Contract-first workflows (optional)

The simple style above is recommended for monoliths and applications where the
contract and implementation naturally live together. When an API producer must
share a workflow with an orchestrator without importing its Nest dependencies,
declare the durable contract separately:

```ts
import { InjectWorkflow, Workflow, WorkflowContract } from 'better-workflows'
import type { WorkflowClient, WorkflowContext } from 'better-workflows'
import { Injectable } from '@nestjs/common'
import { z } from 'zod'

const GenerateReportInputSchema = z.object({ reportId: z.string() })
const GenerateReportOutputSchema = z.object({ reportId: z.string() })
type GenerateReportInput = z.infer<typeof GenerateReportInputSchema>
type GenerateReportOutput = z.infer<typeof GenerateReportOutputSchema>

@WorkflowContract({
  name: 'reports.generate',
  version: 1,
  input: GenerateReportInputSchema,
  output: GenerateReportOutputSchema,
  idempotencyKey: (input) => input.reportId
})
export abstract class GenerateReportWorkflow {
  abstract run(input: GenerateReportInput, ctx: WorkflowContext): Promise<GenerateReportOutput>
}

@Workflow(GenerateReportWorkflow)
export class GenerateReportHandler implements GenerateReportWorkflow {
  constructor(private readonly reports: ReportsService) {}

  run(input: GenerateReportInput, ctx: WorkflowContext): Promise<GenerateReportOutput> {
    return this.reports.generate(input, ctx)
  }
}
```

The contract decorator writes metadata only; it does not make the abstract class a
Nest provider. The `@Workflow(contract)` decorator makes the concrete handler
injectable and validates its `run` shape. An orchestrator registers the handler:

```ts
WorkflowsModule.forFeature({ name: 'reports', workflows: [GenerateReportHandler] })
```

An API or other client-only process imports only the contract and registers:

```ts
WorkflowsModule.forFeature({ clients: [GenerateReportWorkflow] })

@Injectable()
class ReportsApiService {
  constructor(
    @InjectWorkflow(GenerateReportWorkflow)
    private readonly generateReport: WorkflowClient<typeof GenerateReportWorkflow>
  ) {}
}
```

Clients and child workflows use the contract class as their token. Durable identity
continues to come only from the contract's `name` and `version`; the handler class
name does not affect persistence.

See [modules and configuration](docs/modules.md) for precedence, queue ownership, cross-domain calls, asynchronous factories and separated worker processes. Run `bun run example:modular` for a multi-domain application with **no root queue catalog**.

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

Retrieve a handle later with `client.getHandle(executionId)`. `result({ timeout, signal })` only bounds the caller's local wait; it does **not** cancel the workflow. Result waits are notification-first: SQLite wakes local waiters in memory, PostgreSQL uses one shared `LISTEN` connection per runtime, and a 5-second fallback rechecks the database if a notification is lost.

`pause()` prevents advancement at durable command boundaries. Already running activities may finish. `resume()` releases a pause; it does not reset failed executions. `cancel({ reason })` requests durable cooperative cancellation and aborts running activities through their `AbortSignal`. Cancellation is not rollback. `app.close()` or process restart does not imply cancellation. Enable Nest shutdown hooks for signal-driven graceful shutdown.

Long-lived workflows can bound their history with `await ctx.continueAsNew(nextInput)`. The current execution becomes `continued` and a new generation starts with the same contract and version; the input is validated again before the transition is committed. This control is available only on the root context, and `handle.result()` follows the continuation chain automatically while `describe()` remains specific to the execution being inspected.

## Retries and idempotency

```ts
@Activity({
  name: 'reports.deliver', version: 1, queue: DeliveryQueue,
  input: DeliveryInput, output: DeliveryReceipt,
  retry: { maxAttempts: 3, backoff: 'exponential', initialDelay: '1s', maxDelay: '30s' },
  timeout: '1m'
})
```

`maxAttempts` counts total **business attempts**, including the first one. Throw `new ActivityError({ code, message, retryable: true })` to opt into retries. Unexpected exceptions are not assumed transient. A timeout is retryable, subject to the same policy. Attempts and retry deadlines are persisted; failure and the next deadline are committed together. Infrastructure redeliveries do not reset the business attempt counter.

Operational delivery failures are separate from business failures. Invalid or undecodable envelopes, unavailable activity contracts/versions and exhausted transport deliveries are persisted as dead letters instead of retrying forever. The owner is observable as `blocked` and can be recovered after a deployment fix:

```ts
const page = await admin.listDeadLetters({ state: 'open', limit: 100 })
const deadLetter = page.deadLetters[0]
if (deadLetter) await admin.requeueDeadLetter(deadLetter.id)
// Or explicitly terminate the owner:
// await admin.discardDeadLetter(deadLetter.id, { reason: 'Contract retired' })
```

`requeueDeadLetter()` does not create a business attempt: the original `executionId`, `stepId`, idempotency identity and business attempt remain unchanged. Requeue is transactionally idempotent; `discardDeadLetter()` records `WORKFLOW_DEAD_LETTER_DISCARDED` as an administrative terminal failure, outside user `catch`/saga compensation. `listDeadLetters()` returns metadata only. Use `getDeadLetter(id, { includePayload: true })` deliberately when debugging because persisted payloads may contain sensitive data. The equivalent CLI commands are `better-workflows dead-letters list|show|requeue|discard`.

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
    activities: { enabled: true, queues: [ReportQueue] }
  },
  queues: [{ queue: ReportQueue, concurrency: 2 }]
})
```

The address must be reachable by other runners. Optional `cluster.listenAddress` can differ from the advertised address. Keep runner sockets on a trusted private network; the library does not configure a public authenticated gateway.

For API-only or activity-only processes, set `execution.workflows.enabled: false`; no runner address is required. API-only processes also set `execution.activities.enabled: false`. Orchestrators register workflow handlers and activityContracts with their owning feature configuration; activity workers register activity implementations. API producers can register clients only. Share contract classes and domain configuration across roles. Use the same namespace and database across roles. A logical queue's `concurrency` is **per process**, shared by its activity handlers. `globalConcurrency` and `perKeyConcurrency` add SQL-backed limits shared across processes in the same namespace and queue.

## Structured workflows and operations

The public API also includes durable `ctx.map`, named `ctx.parallel` branches, `ctx.child` / `ctx.startChild`, scoped `ctx.saga` compensation, `WorkflowsTestingModule` with a manual business clock, and `WorkflowsAdmin` / CLI for migrations, dead-letter recovery and retention.

```ts
const results = await ctx.map(
  'analyze-documents',
  input.documentIds,
  { key: (id) => id, concurrency: 3 },
  (documentId, branch) => branch.child('analyze', AnalyzeDocumentWorkflow, { documentId })
)
```

Each branch has its own stable command path and durable admission state. A branch waiting on a signal still counts against this map's limit. Results retain input order. Use `ctx.parallel` for a typed object of named branches. Plain `Promise.all` is not the supported durable parallel primitive.

```ts
queues: [{ queue: DocumentsQueue, concurrency: 8, globalConcurrency: 4, perKeyConcurrency: 1 }]
```

Keyed activities declare `key: (input) => input.tenantId` in `@Activity()`. Limits cover all handlers in that queue, not just one activity. All processes must agree on shared limits; configuration drift fails at bootstrap.

See the **[advanced API and operations guide](docs/advanced.md)** for complete examples of branches, children, compensation, virtual time, migrations, retention and shared limits. The **[architecture guide](docs/architecture.md)** documents recovery boundaries, compatibility and validation evidence.

## Observability, health and diagnostics

Observability is optional and uses the public `better-workflows/observability` entry
point. Traces are enabled by default when OTLP is configured; metrics and logs are
opt-in. Exporter requests are best-effort and bounded during shutdown, so an
unavailable collector does not change workflow or activity outcomes:

```ts
import { WorkflowsModule } from 'better-workflows'
import { sqlite } from 'better-workflows/sqlite'
import { otlp } from 'better-workflows/observability'

WorkflowsModule.forRoot({
  namespace: 'reports-app',
  storage: sqlite({ filename: './data/workflows.sqlite' }),
  observability: otlp({
    serviceName: 'reports-worker',
    endpoint: 'https://otel-collector.internal:4318',
    traces: true,
    metrics: { enabled: true, exportInterval: '10s' },
    logs: { enabled: true, level: 'info' },
    attributes: { 'deployment.environment.name': 'production' }
  })
})
```

`WorkflowsHealth` is provided by the root module, but no controller is installed.
Expose `liveness()` for a cheap process check and `await readiness()` for storage,
schema, dispatcher, scheduler and configured worker checks. A disconnected PostgreSQL notifier
is reported as `degraded`; result-wait fallback keeps it from being a correctness
dependency. Open dead letters are operational work and do not make readiness fail.

`await admin.stats()` reads a namespace-wide snapshot from durable storage. It reports
active execution states, activity queue backlog, dead-letter counts and overdue timers
or retries using aggregate queries. The snapshot never returns workflow inputs,
activity/signal payloads, dead-letter payloads, idempotency keys or authorization
headers. The equivalent CLI view is `better-workflows stats` (or `status`) with
`WORKFLOWS_NAMESPACE` and exactly one of `WORKFLOWS_SQLITE_FILE` or
`WORKFLOWS_DATABASE_URL` set.

Use the same `executionId` from `WorkflowHandle.describe()`, history and dead-letter
administration when investigating an execution. Structured logs and spans use the
canonical `better_workflows.execution.id` attribute; continuation spans also carry
the chain and generation. Trace context crosses the persisted activity envelope, but
span IDs are not written into the workflow journal. Telemetry does not capture payloads
or heartbeat details by default.

## Scheduled workflows

Schedules are durable dispatch definitions owned by a workflow contract. They use the
same database/business clock, acceptance transaction and distributed fencing as other
workflow starts; they do not create one JavaScript timer per schedule.

```ts
import { z } from 'zod'
import { Cron, WorkflowContract } from 'better-workflows'
import type { WorkflowContext } from 'better-workflows'

const ReportInput = z.object({ reportDate: z.string() })
const ReportOutput = z.string()

@Cron({
  name: 'reports.daily',
  expression: '0 8 * * *',
  timezone: 'America/Fortaleza',
  misfire: 'latest',
  overlap: 'skip',
  input: ({ scheduledAt }) => ({ reportDate: scheduledAt })
})
@WorkflowContract({
  name: 'reports.generate',
  version: 1,
  input: ReportInput,
  output: ReportOutput
})
abstract class GenerateReportWorkflow {
  abstract run(
    input: z.infer<typeof ReportInput>,
    ctx: WorkflowContext
  ): Promise<z.infer<typeof ReportOutput>>
}
```

Use `@Interval({ name, every })` for a timeline based on its persisted cursor. Cron
uses UTC when `timezone` is omitted; provide an explicit IANA timezone when needed.
Use `misfire: 'skip'`, `'latest'` (the default) or `'catch-up'` with `maxCatchUp`, and
`overlap: 'allow'` (the default) or `'skip'`. Input resolvers are synchronous and
pure; their values are validated again before acceptance and are never sent to logs,
metrics or traces.

`WorkflowsAdmin` and the CLI expose `listSchedules`, `getSchedule`, paginated
`listScheduleOccurrences`, pause/resume, safe removal and manual trigger operations.
Occurrence history exposes started, skipped and failed outcomes without schedule inputs.
Removal requires explicit confirmation and a paused/orphaned schedule, while occurrence history is retained. Manual triggers have
`trigger: 'manual'` and do not move
the recurring cursor. `WorkflowsHealth.readiness()` reports a stale or failed
scheduler separately from the dispatcher. In tests, `WorkflowsTestHarness.flush()` and
`advanceTime()` process schedule deadlines through the manual business clock without
waiting on real timers.

Definition reconciliation is an offline operation: use `createWorkflowsAdmin`, then
restart the owning runtime so its immutable in-memory registry uses the new definition.

## Release scope

This remains an alpha. External effects are at least once and must be idempotent. Business failures inside a saga scope trigger its registered compensations; suspension, infrastructure loss and forced cancellation do not masquerade as business failures. Completed saga scopes are committed, not an automatic rollback of any later workflow failure.

Schema migrations can run automatically at bootstrap (`migrations: 'run'`, the default), or be applied separately before starting the application with `migrations: 'validate'`. Retention requires an explicit preview and confirmation, preserves active dependencies and keeps compact idempotency tombstones. Open/requeued dead letters protect their owner from retention; resolved, discarded or cancelled-owner records can become eligible. There is no background deletion policy by default.

Calendar/RRULE schedules, automatic definition changes, queue/replace overlap
policies, a visual dashboard and publication automation remain outside this release.
No npm publication is performed by the development scripts.

## Editor documentation

Public decorators, module factories, workflow clients, contexts, configuration types,
adapters, administrative operations and testing utilities include JSDoc summaries,
parameter/return descriptions, defaults, caveats and usage examples. The comments are
retained in the packaged declarations for editor hover/signature help.

`bun run build && bun run docs:check` checks the exported source and declaration
graphs with TypeScript and type-checks every JSDoc example against package exports.
See [JSDoc conventions and verification](docs/jsdoc.md) when changing the public API.
