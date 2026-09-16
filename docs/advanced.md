# Advanced workflows and operations

Examples use the real alpha.4 API. Workflows and activities are normal Nest providers. Keep I/O in activities and retain old handler versions while executions need replay. All persisted results and branch values must be plain JSON (or a root void result).

## Durable map and parallel branches

```ts
const rendered = await ctx.map(
  'render-batch',
  input.reportIds,
  { key: (reportId) => reportId, concurrency: 3 },
  async (reportId, branch) => {
    const file = await branch
      .activities(ReportActivities)
      .render({ reportId }, { stepId: 'render' })
    await branch.sleep('cooldown', '1m')
    return file
  }
)

const results = await ctx.parallel(
  'independent-checks',
  {
    inventory: (branch) =>
      branch.activities(InventoryActivities).check({ orderId: input.orderId }, { stepId: 'check' }),
    shipping: (branch) =>
      branch.activities(ShippingActivities).quote({ orderId: input.orderId }, { stepId: 'quote' })
  },
  { concurrency: 2 }
)
// results.inventory and results.shipping retain their different inferred types.
```

A map's keys must be unique and stable. Membership, order, inputs and concurrency are part of its replay contract. Identical `stepId: 'render'` values in different branches do not collide. Nested maps and parallel groups have nested scopes. Root step IDs beginning with `@bw/` are reserved for scoped identities.

Concurrency counts **durably admitted branches**, including suspended branches. With concurrency 3, three branches waiting for approval prevent admission of a fourth until a branch finishes. This limit is independent of activity-worker slots. A restart does not reset admitted branches or repeat completed branch results.

Groups use **settle-all**, not fail-fast: other branches continue after a business failure. The first failure in input order (map) or sorted branch-name order (parallel) is returned after all branches settle. A branch waiting forever can therefore keep a failed group waiting; give external waits appropriate timeouts. Use workflow cancellation to stop the process rather than assuming a sibling failure cancels it. Empty groups return an empty array/object.

Use the provided branch context for its commands. `Promise.all` over calls on a single context is not a substitute for these primitives.

## Child workflows

```ts
const analysis = await ctx.child(
  'analyze-document',
  AnalyzeDocumentWorkflow,
  { documentId: input.documentId },
  { parentClosePolicy: 'request-cancel' }
)

const background = await ctx.startChild(
  'send-independent-notification',
  NotifyWorkflow,
  { documentId: input.documentId },
  { parentClosePolicy: 'abandon' }
)
// background.executionId can be used with a version-compatible NotifyWorkflow client.
```

`child` waits durably and returns the child's typed output. `startChild` returns after durable creation/linking and does not wait for completion. The child's identity derives from parent execution, scoped step, workflow definition and version. Replay finds that child instead of creating another; two differently named child steps do not deduplicate one another merely because their input matches.

The default parent-close policy is **request-cancel**. It applies on completion, failure or cancellation of the parent. Consequently, use `abandon` for a deliberately independent child that must survive its parent's completion. Cancellation is cooperative, not rollback. The child can fail with a business error that the parent catches; a cancelled child awaiting `child()` produces `CHILD_WORKFLOW_CANCELLED`.

Register child implementations through `forFeature({ workflows: [...] })`. The parent feature declares them or imports their exported clients. Share the same namespace/storage and contract definitions across producers, parents and workers. Parent/child relations are persisted and respected by retention.

## Sagas and compensations

```ts
return ctx.saga('checkout', async (saga) => {
  const payment = await saga.step(
    'charge',
    (forward) =>
      forward
        .activities(PaymentActivities)
        .charge({ orderId: input.orderId }, { stepId: 'charge' }),
    async (receipt, undo) => {
      await undo
        .activities(PaymentActivities)
        .refund({ paymentId: receipt.paymentId }, { stepId: 'refund' })
    }
  )

  const reservation = await saga.step(
    'reserve',
    (forward) =>
      forward
        .activities(StockActivities)
        .reserve({ orderId: input.orderId }, { stepId: 'reserve' }),
    async (receipt, undo) => {
      await undo
        .activities(StockActivities)
        .release({ reservationId: receipt.id }, { stepId: 'release' })
    }
  )

  // A business failure here releases stock, then refunds payment.
  await saga.activities(OrderActivities).confirm({ orderId: input.orderId }, { stepId: 'confirm' })
  return { payment, reservation }
})
```

The saga itself is a `WorkflowContext`: use `saga.activities`, `saga.sleep`, `saga.child`, and other normal commands for work within that scope. Use the `forward`/`undo` context inside step callbacks. Capturing an outer context inside a branch or saga is rejected with `WRONG_WORKFLOW_CONTEXT` instead of deadlocking the interpreter. Each successfully completed step registers its result and compensation durably. A failure inside the saga invokes registered compensations in reverse step order. The failing step is not registered as successful. Keep the forward step small enough that a partial external effect can be retried safely; there is no atomic transaction with an arbitrary remote provider.

Compensation callbacks are reconstructed from the same versioned workflow handler; JavaScript closures are not serialized. Store the information an undo needs in the forward step's JSON result. Persisted undo activities retain their IDs, results and retry deadlines after a crash. A completed undo is not repeated during replay. A terminally failed undo is recorded, the remaining undos still run, and the final error is `COMPENSATION_FAILED` with the original error and failing step IDs. Per-step failures remain in the journal/history.

Use `@Activity({ retry: ... })` on refund/release operations as usual. External compensations must be idempotent too. A saga is a **scope**: once it returns successfully it is committed; a later failure outside it does not undo it. Wrap the whole relevant transaction in one saga.

**Suspension, infrastructure loss and forced workflow cancellation do not trigger business rollback.** To require graceful rollback, deliver an abort signal that your saga interprets as a business failure and wait for the compensation path to complete. Do not use `handle.cancel()` as a refund mechanism. A process being killed during an already-started rollback resumes that rollback.

## Test module and virtual business time

```ts
import { Test } from '@nestjs/testing'
import { WorkflowsModule, getWorkflowToken } from 'better-workflows'
import { WorkflowsTestingModule, WorkflowsTestHarness } from 'better-workflows/testing'

const module = await Test.createTestingModule({
  imports: [
    WorkflowsTestingModule.forRoot({
      clock: 'manual',
      initialTime: Date.UTC(2026, 0, 1)
    }),
    WorkflowsModule.forFeature({
      name: 'reports',
      workflows: [ApprovalWorkflow],
      activities: [ReportActivities],
      queues: [{ queue: ReportQueue, concurrency: 2 }]
    })
  ]
}).compile()
await module.init()
try {
  const client = module.get(getWorkflowToken(ApprovalWorkflow))
  const handle = await client.start({ reportId: 'report-1' })
  const harness = module.get(WorkflowsTestHarness)
  await harness.runUntilIdle()
  await harness.advanceTime('7d')
  const status = await harness.waitFor(
    () => handle.describe(),
    (snapshot) => snapshot.status === 'completed'
  )
} finally {
  await module.close()
}
```

The default is isolated real in-memory SQLite plus the real Effect workflow engine, **not mocked workflow results**. `storage` and `namespace` can be supplied for persistence/restart tests. Time is local to that testing module; `Date.now()` and other applications' clocks are untouched.

`advanceTime` visits each pending business deadline chronologically. It controls new durable timers, signal timeouts, persisted retry deadlines and activity timeouts. For example, a one-day retry followed by a two-day retry fires at day 1 and day 3, not both at the final target date.

Transport/cluster polling and ownership leases use real time. Arbitrary user `setTimeout`, network calls and provider-side clocks are not virtualized. `runUntilIdle` waits for observable state to settle with a bounded real-time safety timeout; use `waitFor` when a specific asynchronous milestone matters. Pending signal waits do not prevent idleness. A continuously progressing workflow can produce `TEST_NOT_IDLE` instead of silently returning.

## Migrations: administrative connection and CLI

```ts
import { createWorkflowsAdmin } from 'better-workflows/admin'
import { postgres } from 'better-workflows/postgres'

const admin = await createWorkflowsAdmin({
  namespace: 'reports-app',
  storage: postgres({ connectionString: process.env.DATABASE_URL! })
})
try {
  const before = await admin.migrations.status() // SELECT-only inspection
  const after = await admin.migrations.run() // applies pending forward migrations
  await admin.migrations.validate() // fails when migration/structure is missing
} finally {
  await admin.close()
}
```

No workflow runner or activity worker starts through this connection. Run schema changes before starting application workers. Migrations serialize using a PostgreSQL transaction advisory lock or SQLite's writer lock. Newer/gapped ledgers and missing already-applied journal structures are rejected, not treated as an empty database. There is no automatic downgrade or rollback migration.

After applying migrations:

```ts
WorkflowsModule.forRoot({
  namespace: 'reports-app',
  storage,
  queues,
  migrations: 'validate'
})
```

Standalone `status` and `validate` issue no DDL. Runtime validation occurs before starting native infrastructure; the pinned Effect constructors subsequently execute their idempotent ensure-existing metadata operations. This is not a claim that all native bootstrap operations work with a SELECT-only database role. Without this option the default is `run` for developer convenience.

`WorkflowsAdmin` is also exported from the root module for injection. Its live-runtime `migrations.run()` rejects with `MIGRATION_RUNTIME_ACTIVE`; use the standalone connection or CLI for migrations. Inspection and retention are available on both.

```bash
export WORKFLOWS_NAMESPACE=reports-app
export WORKFLOWS_SQLITE_FILE=./data/workflows.sqlite
# Or WORKFLOWS_DATABASE_URL for PostgreSQL; never set both storage variables.

better-workflows migrations status
better-workflows migrations run
better-workflows migrations validate
```

When using the repository rather than an installed package, build it and replace `better-workflows` with `node dist/cli.mjs`. `--help` does not open a database. Namespace is mandatory; the CLI does not guess which application's state to delete.

## Safe retention

```ts
const plan = await admin.retention.preview({
  before: '2026-01-01T00:00:00.000Z',
  limit: 100
})
// Inspect plan.candidates and plan.blocked before explicitly authorizing deletion.
const removed = await admin.retention.prune(plan, { confirm: true })
```

```bash
better-workflows retention preview --before 2026-01-01T00:00:00.000Z --limit 100 --plan ./retention-plan.json
better-workflows retention prune --plan ./retention-plan.json --confirm
```

Preview is read-only, bounded to at most 1000 terminal executions and scoped to the namespace. It does not select active workflows. Active parent/child links, live claims/permits, unacknowledged queue deliveries or unprocessed engine messages block deletion. Cutoffs in the future are rejected. A plan file is created exclusively with restrictive permissions, never overwritten silently.

Apply requires an intact plan and rechecks candidate revisions and safety conditions while holding locks. A stale candidate fails the transaction; it is not silently deleted. A plan hash detects accidental edits, **not authorization**: keep admin credentials and application authorization outside untrusted callers.

Successful pruning removes the execution's journal details, queue records and native message/reply records, retaining a compact tombstone with its key and input hash. Restarting a pruned key returns `EXECUTION_PRUNED`; a conflicting payload returns `IDEMPOTENCY_CONFLICT`. Use a new application idempotency key for an intentional new execution. Tombstones are not automatically expired. Retention is not archival/export and does not VACUUM SQLite, shrink PostgreSQL files or delete another application's tables. There is no deletion daemon by default.

## Shared queue limits

Use `DocumentsQueue = defineQueue('documents')`. See [module configuration](modules.md) for ownership, visibility and explicit deployment overrides.

```ts
@Activity({
  name: 'documents.extract', version: 1, queue: DocumentsQueue,
  input: ExtractInput, output: ExtractOutput,
  key: (input) => input.tenantId
})
```

```ts
queues: [
  {
    queue: DocumentsQueue, // defineQueue('documents'), registered in the owning feature or root
    concurrency: 8, // local worker slots in each process
    globalConcurrency: 4, // total live permits across all processes
    perKeyConcurrency: 1 // live permits for each tenant in this queue
  }
]
```

Limits share the `(namespace, queue)` boundary across activity types. `perKeyConcurrency` requires a nonempty stable `Activity.key`; the key is derived from validated input and checked as part of replay. Without a global limit, per-key limiting still applies. Without either shared limit, local queue slots continue to apply.

Admission, claim ownership and permit creation are atomic SQL operations. Heartbeats renew only current ownership; completion releases the permit and stores the result/retry deadline together. Expired/replaced owners cannot write a new result using stale tokens. Non-cooperative remote side effects can outlive a lease: shared limits constrain current owners, not external processes that the library cannot forcibly stop.

Processes must agree on the shared limits. A different deployment configuration fails with `QUEUE_LIMIT_CONFLICT` instead of silently weakening enforcement. Drain active workflows, stop producers/workers, then update persisted policy and deploy matching options:

```ts
await admin.queues.setLimits(DocumentsQueue, {
  globalConcurrency: 6,
  perKeyConcurrency: 2
})
```

The API rejects active namespaces and outstanding live permits. Local `concurrency` can differ by process because it is not a distributed policy. Admission polling does not promise tenant fairness or rate limiting; these are concurrency limits, not requests-per-second quotas.
