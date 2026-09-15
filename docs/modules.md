# Modules, queue ownership and configuration

`forRoot()` starts one infrastructure runtime per Nest application. `forFeature()` composes domain-owned workflow handlers, activity handlers and queue policies into that runtime. A feature does **not** create another engine, database pool or set of queue permits.

The registration API in `0.1.0-alpha.3` intentionally replaces the earlier array-only `forFeature` and string/dictionary queue configuration. There is no compatibility overload. The workflow execution API (`ctx.activities`, `ctx.map`, children, sagas and signals) keeps its existing semantics.

## Identity first, policy in the owning module

```ts
import { defineQueue } from 'better-workflows'

export const RenderQueue = defineQueue('reports.render')
export const EmailQueue = defineQueue('notifications.email')
```

A queue reference is immutable, branded and retains its literal name type. Creating it has no side effects. A reference does not register a queue or start a worker. Its durable identity is the explicit name in the application's namespace, **not** the class/file name, feature name or object reference. Two calls with the same name identify the same logical queue; two independent declarations of its policy are an error.

Schemas belong to activities, not queues. Multiple activity types and versions can share a logical queue and its capacity.

## Root configuration without a queue catalog

```ts
@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'my-app',
      storage: sqlite({ filename: './data/workflows.sqlite' }),
      defaults: {
        queues: { concurrency: 4 },
        activities: { timeout: '2m' }
      },
      execution: { workflows: { concurrency: 20 } }
    }),
    ReportsModule,
    BillingModule
  ]
})
export class AppModule {}
```

Only the infrastructure is global by default. Clients and domain implementations are not made global by this option. A queue can instead be deliberately owned at the root:

```ts
WorkflowsModule.forRoot({
  namespace: 'my-app',
  storage,
  queues: [{ queue: EmailQueue, concurrency: 8, globalConcurrency: 20 }]
})
```

Root-owned queues are visible to all registered features. Declaring a queue alone does not start any worker: an enabled activity implementation is also required.

For explicit infrastructure imports, set `isGlobal: false`, wrap that root dynamic module in an infrastructure module that exports `WorkflowsModule`, and import the wrapper inside each feature's `imports`. Reuse that module; do not call `forRoot()` separately in each feature. In `forRootAsync`, `isGlobal` is a top-level static option, not a value returned by its factory.

## Feature registration

```ts
@Module({
  imports: [
    WorkflowsModule.forFeature({
      name: 'reports',
      imports: [ReportsInfrastructureModule],
      workflows: [GenerateReport, RegenerateReport],
      activities: [ReportActivities],
      queues: [{ queue: RenderQueue, concurrency: 2, globalConcurrency: 6 }],
      defaults: { activities: { timeout: '3m' } }
    })
  ],
  controllers: [ReportsController],
  exports: [WorkflowsModule]
})
export class ReportsModule {}
```

`workflows` registers implementations and their typed clients. `activities` registers implementations. These classes **must not also be registered** in the outer module's `providers`. Their dependencies must be exported by modules in the feature's own `imports`, or declared in the feature's `providers` for local helper services. Nest does not automatically expose the importing parent's providers to its children.

`clients` registers only clients, without constructing workflow implementations:

```ts
WorkflowsModule.forFeature({ clients: [GenerateReport] })
```

Such a client-only import needs no name. A feature owning handlers, activity contracts, queues, defaults or execution settings requires a unique explicit `name`. The name is for ownership and diagnostics, not a durable routing prefix.

An imported domain module can expose clients with normal Nest `exports: [WorkflowsModule]`. The feature exports the typed clients for its `workflows` and `clients`, not the raw implementation services. Decorated but unregistered classes are not silently discovered as workers.

## Provider and method defaults

```ts
@Activities({ queue: RenderQueue, timeout: '2m' })
export class ReportActivities {
  constructor(private readonly reports: ReportsService) {}

  @Activity({
    name: 'reports.render',
    version: 1,
    input: RenderInput,
    output: RenderOutput
  })
  render(input: RenderInputType, ctx: ActivityContext) {
    return this.reports.render(input, ctx.signal)
  }

  @Activity({
    name: 'reports.notify',
    version: 1,
    queue: EmailQueue,
    input: NotifyInput,
    output: NotifyOutput,
    timeout: '30s',
    retry: { maxAttempts: 3 }
  })
  notify(input: NotifyInputType, ctx: ActivityContext) {
    return this.reports.notify(input, ctx.idempotencyKey)
  }
}
```

The effective activity fields resolve in this order (later wins):

1. Root `defaults.activities`.
2. Owning feature `defaults.activities`.
3. `@Activities()`.
4. `@Activity()`.

These defaults cover `queue`, `timeout` and `retry`. A retry object replaces the **entire** earlier policy; it is never recursively merged. Absent retry means one business attempt, and absent timeout means five minutes. An activity must resolve a queue that is owned or imported by its feature. Per-key queues require an explicit `key` callback on every activity method using them.

The **owner**, not the caller, supplies defaults. Workflows, branch contexts, children and saga compensations all dispatch the same resolved activity contract. Method-level queue choices select a destination; they do not redefine its capacity.

## Queue policy precedence

Queue settings resolve from:

1. Library default (`concurrency: 4`, no shared limits).
2. Root `defaults.queues`.
3. Owning feature `defaults.queues`.
4. The queue's explicit registration.
5. An explicit root `queueOverrides` entry.

```ts
WorkflowsModule.forRoot({
  namespace: 'my-app',
  storage,
  defaults: { queues: { globalConcurrency: 10 } },
  queueOverrides: [{ queue: RenderQueue, concurrency: 1, globalConcurrency: null }]
})
```

`null` removes an inherited `globalConcurrency` or `perKeyConcurrency`; omission keeps the inherited value. Local concurrency must remain a positive integer. Overrides never create queues: an unknown override or repeated override fails bootstrap. Root defaults are defaults, **not aggregate budgets across multiple queues**.

A logical queue has one configuration owner. Redeclaring it in another feature—even with identical settings—is rejected. Import the owner's exports to share it. Deployment overrides are the explicit alternative to silently mutating another module's policy. Import order does not decide the winner.

Local `concurrency` is per process, shared across all handlers and features that consume that logical queue. `globalConcurrency` and `perKeyConcurrency` apply across participating processes in the namespace. Local values may differ across processes; shared limits must agree. Distinct queue names have independent limits, even when their defaults are equal.

## Private queues and shared activities

Exporting an activity contract lets another workflow use that activity without knowing or importing its private queue:

```ts
@Module({
  imports: [
    WorkflowsModule.forFeature({
      name: 'notifications',
      imports: [MailInfrastructureModule],
      activities: [MailActivities],
      queues: [{ queue: EmailQueue, concurrency: 8 }],
      defaults: { activities: { queue: EmailQueue } },
      exports: { activities: [MailActivities] }
    })
  ],
  exports: [WorkflowsModule]
})
export class NotificationsModule {}

WorkflowsModule.forFeature({
  name: 'reports',
  imports: [NotificationsModule],
  workflows: [GenerateReport]
})
```

`GenerateReport` can call `ctx.activities(MailActivities)`. Importing only the TypeScript class or having `NotificationsModule` as a sibling import of the application is not sufficient. The owning feature must export the contract and the consuming feature must import the exporting module. These are modularity checks, not authorization or a security sandbox.

To have other modules put **their own** activity handlers on the shared email queue, export its queue capability as well:

```ts
exports: { queues: [EmailQueue], activities: [MailActivities] }
```

The consuming feature imports `NotificationsModule` and references `EmailQueue` in its activity metadata, but **does not redeclare it** in `queues`. Capabilities follow actual Nest module exports/reexports. The library does not use `ModuleRef.get({ strict: false })` to bypass module visibility. Exporting an unowned and unimported queue or activity fails bootstrap.

A child workflow must be declared in the parent's feature (`workflows` or `clients`) or imported through its exported client. Register the child's implementation in an orchestrator application. Merely importing the TypeScript class does not instantiate or register it.

## Asynchronous feature settings

```ts
WorkflowsModule.forFeatureAsync({
  name: 'reports',
  imports: [ReportsSettingsModule, ReportsInfrastructureModule],
  inject: [ReportsSettings],
  workflows: [GenerateReport],
  activities: [ReportActivities],
  useFactory: async (settings: ReportsSettings) => ({
    queues: [
      {
        queue: RenderQueue,
        concurrency: await settings.renderConcurrency(),
        globalConcurrency: 6
      }
    ],
    defaults: { activities: { timeout: '3m' } }
  })
})
```

Only values (`queues`, `defaults`, `execution`) are asynchronous. Imports, providers, client/handler classes and export capabilities belong to the static options; the factory cannot add new Nest providers after dependency graph construction. All factories resolve before catalog validation and before infrastructure/workers start.

## Reusing an existing instance

```ts
WorkflowsModule.forFeature({
  name: 'reports',
  imports: [ExistingReportsModule],
  activities: [{ provide: ReportActivities, useExisting: ReportActivities }],
  queues: [{ queue: RenderQueue }]
})
```

`ExistingReportsModule` must export that service. `provide` identifies the decorated contract; `useExisting` is the Nest injection token supplying its existing implementation. The same form works in `workflows`. The library does not construct another instance. Request-scoped/non-singleton handlers or ambiguous imported implementations fail registration. Reimport the same module to reuse a feature; independently invoking `forFeature` twice with the same owner name is an error.

## Producers, orchestrators and activity workers

An API producer can use `clients` and turn both execution roles off in the root. It does not need to register activity contracts or queues just to start a workflow.

An orchestrator needs workflow handlers and **activity contracts with their owning configuration**, but need not instantiate activity implementation services:

```ts
WorkflowsModule.forFeature({
  name: 'reports',
  workflows: [GenerateReport],
  activityContracts: [ReportActivities],
  queues: [{ queue: RenderQueue, concurrency: 2, globalConcurrency: 6 }],
  defaults: { activities: { timeout: '3m' } }
})
```

An activity worker loads the same queue identities and policies with `activities: [ReportActivities]` and `execution.workflows.enabled: false` at the root. Share a domain configuration factory/constant across roles to prevent default drift; a caller's defaults never replace the remote activity owner's policy. Imports/exports apply equally to contract-only features. Do not register the same provider in both `activities` and `activityContracts` inside one application.

Root and feature execution switches are intersected. A feature cannot reenable a root-disabled role. Root and feature `execution.activities.queues` filters also intersect, and use queue references. Every selected queue must be registered; feature selectors must respect visibility. A feature workflow concurrency limit is shared across its workflow implementations and combined with the process-wide workflow limit. These count active interpreter rounds, not durable suspended executions.

## Administration, testing and diagnostics

`WorkflowsAdmin.queues.setLimits(RenderQueue, { globalConcurrency: 6 })` uses the same stable queue reference. Its database scope remains namespace + queue name. It requires quiescent workers; a module boundary does not permit conflicting online global limits. No new schema migration is required by modular registration.

`WorkflowsTestingModule` accepts the same root defaults/overrides and optional root queues. Import your real features alongside it. It does not use a simplified alternate registry or bypass visibility. The resolved catalog freezes policy objects before workers start; runtime discovery is not a mutable service locator.

Typical bootstrap errors include `FEATURE_NAME_REQUIRED`, `DUPLICATE_FEATURE`, `DUPLICATE_QUEUE_OWNER`, `QUEUE_NOT_VISIBLE`, `INVALID_FEATURE_EXPORT`, `UNKNOWN_QUEUE_OVERRIDE`, `ACTIVITY_KEY_REQUIRED` and `UNSUPPORTED_SCOPE`. Cross-domain durable calls to unimported contracts report `ACTIVITY_NOT_VISIBLE` or `WORKFLOW_NOT_VISIBLE`. These include the relevant queue/provider/feature rather than falling back to arbitrary global handlers.

Run `bun run example:modular` for an executable application with an infrastructure-only root, async report configuration, multiple report workflows, child fan-out, a separate audit domain and a shared activity whose queue remains private. It writes actual JSON reports and audit records, and is exercised in CI.
