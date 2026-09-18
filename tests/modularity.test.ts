import { expect, test } from 'bun:test'
import { forwardRef, Inject, Injectable, Module, Scope } from '@nestjs/common'
import type { DynamicModule, Type } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import type { TestingModule } from '@nestjs/testing'
import { z } from 'zod'
import {
  Activities,
  ActivitiesContract,
  Activity,
  Workflow,
  WorkflowsModule,
  defineQueue,
  getWorkflowToken
} from '../src'
import type {
  WorkflowClient,
  WorkflowClass,
  WorkflowContext,
  WorkflowsOptions,
  ActivityContext
} from '../src'
import { sqlite } from '../src/sqlite'
import { WorkflowsRuntime } from '../src/internal/runtime'
import { eventually } from './helpers'

const Reports = defineQueue('reports.render')
const Mail = defineQueue('notifications.email')
const Other = defineQueue('other')

@Activities({ queue: Reports })
class ReportActivities {
  @Activity({ name: 'reports.render', version: 1, input: z.string(), output: z.string() })
  async render(input: string) {
    return `report:${input}`
  }
}
@Activities()
class MailActivities {
  @Activity({ name: 'notifications.email', version: 1, input: z.string(), output: z.string() })
  async send(input: string) {
    return `email:${input}`
  }
}
@Workflow({ name: 'report', version: 1, input: z.string(), output: z.string() })
class ReportWorkflow {
  async run(input: string, ctx: WorkflowContext) {
    return ctx.activities(ReportActivities).render(input, { stepId: 'render' })
  }
}
@Workflow({ name: 'email', version: 1, input: z.string(), output: z.string() })
class EmailWorkflow {
  async run(input: string, ctx: WorkflowContext) {
    return ctx.activities(MailActivities).send(input, { stepId: 'send' })
  }
}

async function open(
  features: readonly (DynamicModule | Type)[],
  root: Partial<WorkflowsOptions> = {}
): Promise<TestingModule> {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'modularity',
        storage: sqlite({ filename: ':memory:' }),
        pollInterval: '5ms',
        ...root
      }),
      ...features
    ]
  }).compile()
  try {
    await app.init()
    return app
  } catch (error) {
    await app.close().catch(() => {})
    throw error
  }
}
function client<W extends WorkflowClass>(app: TestingModule, workflow: W): WorkflowClient<W> {
  return app.get(getWorkflowToken(workflow))
}

const reportsFeature = () =>
  WorkflowsModule.forFeature({
    name: 'reports',
    workflows: [ReportWorkflow],
    activities: [ReportActivities],
    queues: [{ queue: Reports, concurrency: 1 }]
  })
const mailFeature = () =>
  WorkflowsModule.forFeature({
    name: 'notifications',
    workflows: [EmailWorkflow],
    activities: [MailActivities],
    defaults: { activities: { queue: Mail } },
    queues: [{ queue: Mail, concurrency: 3 }]
  })

test('independent domains contribute queues with no root queue catalog', async () => {
  const app = await open([reportsFeature(), mailFeature()])
  try {
    expect(await (await client(app, ReportWorkflow).start('a')).result({ timeout: '3s' })).toBe(
      'report:a'
    )
    expect(await (await client(app, EmailWorkflow).start('b')).result({ timeout: '3s' })).toBe(
      'email:b'
    )
    const registry = app.get(WorkflowsRuntime).registry
    expect(registry.queues.size).toBe(2)
    expect(registry.queues.get(Reports.name)?.concurrency).toBe(1)
    expect(registry.queues.get(Mail.name)?.concurrency).toBe(3)
  } finally {
    await app.close()
  }
})

test('root, feature, class and method defaults resolve deterministically; retry objects replace as a unit', async () => {
  @Activities({ queue: Reports, timeout: '2s', retry: { maxAttempts: 2, initialDelay: '7s' } })
  class Policies {
    @Activity({ name: 'class-defaults', version: 1, input: z.string(), output: z.string() })
    async inherited(value: string) {
      return value
    }
    @Activity({
      name: 'method-defaults',
      version: 1,
      queue: Mail,
      timeout: '3s',
      retry: { maxAttempts: 3 },
      input: z.string(),
      output: z.string()
    })
    async explicit(value: string) {
      return value
    }
  }
  const settings = { maxAttempts: 9 }
  const app = await open(
    [
      WorkflowsModule.forFeature({
        name: 'policy',
        activities: [Policies],
        defaults: { activities: { timeout: '1s', retry: settings }, queues: { concurrency: 5 } },
        queues: [{ queue: Reports }, { queue: Mail, concurrency: 6 }]
      })
    ],
    {
      defaults: {
        activities: { timeout: '500ms' },
        queues: { concurrency: 4, globalConcurrency: 10 }
      }
    }
  )
  try {
    settings.maxAttempts = 100
    const registry = app.get(WorkflowsRuntime).registry
    const [inherited, explicit] = registry.activityContracts(Policies)
    expect(inherited?.options).toMatchObject({
      queue: Reports.name,
      timeout: '2s',
      retry: { maxAttempts: 2, initialDelay: '7s' }
    })
    expect(explicit?.options).toMatchObject({ queue: Mail.name, timeout: '3s' })
    expect(explicit?.options.retry).toEqual({ maxAttempts: 3 })
    expect(Object.isFrozen(explicit?.options)).toBe(true)
    expect(Object.isFrozen(explicit?.options.retry)).toBe(true)
    expect(registry.queues.get(Reports.name)).toEqual({ concurrency: 5, globalConcurrency: 10 })
    expect(registry.queues.get(Mail.name)).toEqual({ concurrency: 6, globalConcurrency: 10 })
  } finally {
    await app.close()
  }
})

test('application queue overrides are explicit and null removes an inherited shared limit', async () => {
  const app = await open([reportsFeature()], {
    defaults: { queues: { globalConcurrency: 10 } },
    queueOverrides: [{ queue: Reports, concurrency: 2, globalConcurrency: null }]
  })
  try {
    expect(app.get(WorkflowsRuntime).registry.queues.get(Reports.name)).toEqual({ concurrency: 2 })
  } finally {
    await app.close()
  }
})

test('imported activities use their owner defaults; callers need not know or import the private queue', async () => {
  const feature = WorkflowsModule.forFeature({
    name: 'notifications',
    activities: [MailActivities],
    queues: [{ queue: Mail }],
    defaults: { activities: { queue: Mail, timeout: '4s', retry: { maxAttempts: 2 } } },
    exports: { activities: [MailActivities] }
  })
  @Module({ imports: [feature], exports: [WorkflowsModule] })
  class NotificationsModule {}
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'caller',
      imports: [NotificationsModule],
      workflows: [EmailWorkflow],
      queues: [{ queue: Other }],
      defaults: { activities: { queue: Other, timeout: '1ms' } }
    })
  ])
  try {
    expect(await (await client(app, EmailWorkflow).start('owner')).result({ timeout: '3s' })).toBe(
      'email:owner'
    )
    expect(
      app.get(WorkflowsRuntime).registry.activityContracts(MailActivities)[0]?.options
    ).toMatchObject({ queue: Mail.name, timeout: '4s', retry: { maxAttempts: 2 } })
  } finally {
    await app.close()
  }
})

test('queue and contract visibility follows transitive Nest re-exports', async () => {
  const feature = WorkflowsModule.forFeature({
    name: 'queue-owner',
    queues: [{ queue: Reports }],
    exports: { queues: [Reports] }
  })
  @Module({ imports: [feature], exports: [WorkflowsModule] })
  class Owner {}
  @Module({ imports: [Owner], exports: [Owner] })
  class Barrel {}
  @Module({ imports: [Barrel], exports: [Barrel] })
  class PublicModule {}
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'consumer',
      imports: [PublicModule],
      workflows: [ReportWorkflow],
      activities: [ReportActivities]
    })
  ])
  try {
    expect(
      await (await client(app, ReportWorkflow).start('imported')).result({ timeout: '3s' })
    ).toBe('report:imported')
  } finally {
    await app.close()
  }
})

test('importing a queue constant or a sibling module does not make a feature-private queue visible', async () => {
  const owner = WorkflowsModule.forFeature({ name: 'owner', queues: [{ queue: Reports }] })
  @Module({ imports: [owner], exports: [WorkflowsModule] })
  class Owner {}
  await expect(
    open([
      Owner,
      WorkflowsModule.forFeature({
        name: 'consumer',
        imports: [Owner],
        activities: [ReportActivities]
      })
    ])
  ).rejects.toMatchObject({ code: 'QUEUE_NOT_VISIBLE' })
})

test('private activity contracts cannot be used by unrelated workflow features', async () => {
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'private',
      activities: [ReportActivities],
      queues: [{ queue: Reports }]
    }),
    WorkflowsModule.forFeature({ name: 'caller', workflows: [ReportWorkflow] })
  ])
  try {
    await expect(
      (await client(app, ReportWorkflow).start('private')).result({ timeout: '3s' })
    ).rejects.toMatchObject({ code: 'ACTIVITY_NOT_VISIBLE' })
  } finally {
    await app.close()
  }
})

test('unknown and unauthorized re-exports fail before workers start', async () => {
  await expect(
    open([WorkflowsModule.forFeature({ name: 'invalid', exports: { queues: [Reports] } })])
  ).rejects.toMatchObject({ code: 'UNKNOWN_QUEUE' })
  await expect(
    open([
      reportsFeature(),
      WorkflowsModule.forFeature({ name: 'invalid', exports: { activities: [ReportActivities] } })
    ])
  ).rejects.toMatchObject({ code: 'INVALID_FEATURE_EXPORT' })
})

test('reimporting the same Nest feature module does not instantiate its handler twice', async () => {
  let constructions = 0
  @Activities({ queue: Reports })
  class Once {
    constructor() {
      constructions++
    }
    @Activity({ name: 'once', version: 1, input: z.string(), output: z.string() })
    async run(value: string) {
      return value
    }
  }
  const feature = WorkflowsModule.forFeature({
    name: 'once',
    activities: [Once],
    queues: [{ queue: Reports }]
  })
  @Module({ imports: [feature], exports: [WorkflowsModule] })
  class Shared {}
  @Module({ imports: [Shared] })
  class Left {}
  @Module({ imports: [Shared] })
  class Right {}
  const app = await open([Left, Right])
  try {
    expect(constructions).toBe(1)
    expect(app.get(WorkflowsRuntime).registry.activities.size).toBe(1)
  } finally {
    await app.close()
  }
})

test('independent duplicate feature or queue owners are errors, regardless of matching values or import order', async () => {
  await expect(open([reportsFeature(), reportsFeature()])).rejects.toMatchObject({
    code: 'DUPLICATE_FEATURE'
  })
  const a = () =>
    WorkflowsModule.forFeature({ name: 'a', queues: [{ queue: Reports, concurrency: 1 }] })
  const b = () =>
    WorkflowsModule.forFeature({
      name: 'b',
      queues: [{ queue: defineQueue(Reports.name), concurrency: 1 }]
    })
  await expect(open([a(), b()])).rejects.toMatchObject({ code: 'DUPLICATE_QUEUE_OWNER' })
  await expect(open([b(), a()])).rejects.toMatchObject({ code: 'DUPLICATE_QUEUE_OWNER' })
  await expect(open([reportsFeature()], { queues: [{ queue: Reports }] })).rejects.toMatchObject({
    code: 'DUPLICATE_QUEUE_OWNER'
  })
})

test('forFeatureAsync resolves settings and handler dependencies from its own imports', async () => {
  @Injectable()
  class Settings {
    readonly concurrency = 2
    readonly prefix = 'configured:'
  }
  @Module({ providers: [Settings], exports: [Settings] })
  class SettingsModule {}
  @Activities({ queue: Reports })
  class Configured {
    constructor(@Inject(Settings) readonly settings: Settings) {}
    @Activity({ name: 'configured', version: 1, input: z.string(), output: z.string() })
    async run(value: string) {
      return this.settings.prefix + value
    }
  }
  @Workflow({ name: 'configured', version: 1, input: z.string(), output: z.string() })
  class ConfiguredWorkflow {
    async run(value: string, ctx: WorkflowContext) {
      return ctx.activities(Configured).run(value, { stepId: 'configured' })
    }
  }
  let factories = 0
  const app = await open([
    WorkflowsModule.forFeatureAsync({
      name: 'configured',
      imports: [SettingsModule],
      inject: [Settings],
      workflows: [ConfiguredWorkflow],
      activities: [Configured],
      useFactory: async (settings: Settings) => {
        await new Promise((resolve) => setTimeout(resolve, 10))
        factories++
        return {
          queues: [{ queue: Reports, concurrency: settings.concurrency }],
          defaults: { activities: { timeout: '3s' } }
        }
      }
    })
  ])
  try {
    expect(factories).toBe(1)
    expect(
      await (await client(app, ConfiguredWorkflow).start('ok')).result({ timeout: '3s' })
    ).toBe('configured:ok')
    expect(app.get(WorkflowsRuntime).registry.queues.get(Reports.name)?.concurrency).toBe(2)
  } finally {
    await app.close()
  }
})

test('useExisting reuses an exported Nest implementation and its dependency tree', async () => {
  let constructions = 0
  @Activities({ queue: Reports })
  class Existing {
    constructor() {
      constructions++
    }
    @Activity({ name: 'existing', version: 1, input: z.string(), output: z.string() })
    async run(value: string) {
      return value
    }
  }
  @Module({ providers: [Existing], exports: [Existing] })
  class ExistingModule {}
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'reuse',
      imports: [ExistingModule],
      activities: [{ provide: Existing, useExisting: Existing }],
      queues: [{ queue: Reports }]
    })
  ])
  try {
    expect(constructions).toBe(1)
    const registration = [...app.get(WorkflowsRuntime).registry.activities.values()][0]!
    const context: ActivityContext = {
      executionId: 'unit',
      stepId: 'step',
      attempt: 1,
      idempotencyKey: 'key',
      signal: new AbortController().signal,
      heartbeat: async () => {}
    }
    expect(await registration.invoke('same-instance', context)).toBe('same-instance')
    expect(constructions).toBe(1)
  } finally {
    await app.close()
  }
})

test('useExisting binds an advanced activity handler to its shared contract', async () => {
  let constructions = 0
  @ActivitiesContract({ queue: Reports })
  abstract class ExistingContract {
    @Activity({ name: 'existing-advanced', version: 1, input: z.string(), output: z.string() })
    run(_value: string, _context: ActivityContext): Promise<string> {
      throw new Error('contract-only')
    }
  }
  @Activities(ExistingContract)
  class ExistingWorker implements ExistingContract {
    constructor() {
      constructions++
    }
    async run(value: string, _context: ActivityContext): Promise<string> {
      return `advanced:${value}`
    }
  }
  @Module({ providers: [ExistingWorker], exports: [ExistingWorker] })
  class ExistingWorkerModule {}
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'reuse-advanced',
      imports: [ExistingWorkerModule],
      activities: [{ provide: ExistingWorker, useExisting: ExistingWorker }],
      queues: [{ queue: Reports }]
    })
  ])
  try {
    expect(constructions).toBe(1)
    const registration = [...app.get(WorkflowsRuntime).registry.activities.values()][0]!
    const context: ActivityContext = {
      executionId: 'unit',
      stepId: 'step',
      attempt: 1,
      idempotencyKey: 'key',
      signal: new AbortController().signal,
      heartbeat: async () => {}
    }
    expect(await registration.invoke('same-instance', context)).toBe('advanced:same-instance')
    expect(constructions).toBe(1)
  } finally {
    await app.close()
  }
})

test('client-only registration never constructs workflow services or activity workers', async () => {
  @Workflow({ name: 'client-only', version: 1, input: z.string(), output: z.string() })
  class Remote {
    constructor() {
      throw new Error('must not be instantiated')
    }
    async run(value: string) {
      return value
    }
  }
  const app = await open([WorkflowsModule.forFeature({ clients: [Remote] })], {
    execution: { workflows: { enabled: false }, activities: { enabled: false } }
  })
  try {
    expect((await client(app, Remote).start('accepted')).created).toBe(true)
    expect(app.get(WorkflowsRuntime).registry.queues.size).toBe(0)
    expect([...app.get(WorkflowsRuntime).registry.workflows.values()][0]?.handler).toBeUndefined()
  } finally {
    await app.close()
  }
})

test('activityContracts carry owner defaults for remote dispatch without constructing implementations', async () => {
  @Activities()
  class RemoteActivity {
    constructor() {
      throw new Error('worker-only dependency')
    }
    @Activity({ name: 'remote', version: 1, input: z.string(), output: z.string() })
    async run(value: string) {
      return value
    }
  }
  @Workflow({ name: 'remote', version: 1, input: z.string(), output: z.string() })
  class RemoteWorkflow {
    async run(value: string, ctx: WorkflowContext) {
      return ctx.activities(RemoteActivity).run(value, { stepId: 'remote' })
    }
  }
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'remote',
      workflows: [RemoteWorkflow],
      activityContracts: [RemoteActivity],
      queues: [{ queue: Reports }],
      defaults: { activities: { queue: Reports, timeout: '8s' } }
    })
  ])
  try {
    const handle = await client(app, RemoteWorkflow).start('waiting')
    await eventually(
      () => handle.describe(),
      (row) => row.status === 'waiting'
    )
    expect(app.get(WorkflowsRuntime).registry.activities.size).toBe(0)
    expect(
      app.get(WorkflowsRuntime).registry.activityContracts(RemoteActivity)[0]?.options.timeout
    ).toBe('8s')
  } finally {
    await app.close()
  }
})

for (const code of ['UNKNOWN_QUEUE_OVERRIDE', 'DUPLICATE_QUEUE_OVERRIDE'] as const)
  test(code, async () => {
    const queueOverrides =
      code === 'UNKNOWN_QUEUE_OVERRIDE'
        ? [{ queue: Other }]
        : [{ queue: Reports }, { queue: Reports }]
    await expect(open([reportsFeature()], { queueOverrides })).rejects.toMatchObject({ code })
  })

test('root role restrictions dominate feature settings and queue selectors use references', async () => {
  const app = await open(
    [
      WorkflowsModule.forFeature({
        name: 'disabled',
        activities: [ReportActivities],
        workflows: [ReportWorkflow],
        queues: [{ queue: Reports }],
        execution: {
          activities: { enabled: true, queues: [Reports] },
          workflows: { enabled: true }
        }
      })
    ],
    { execution: { activities: { enabled: false }, workflows: { enabled: false } } }
  )
  try {
    const registry = app.get(WorkflowsRuntime).registry
    expect([...registry.activities.values()][0]?.enabled).toBe(false)
    expect([...registry.workflows.values()][0]?.enabled).toBe(false)
  } finally {
    await app.close()
  }
  await expect(
    open([reportsFeature()], { execution: { activities: { queues: [Other] } } })
  ).rejects.toMatchObject({ code: 'UNKNOWN_QUEUE' })
})

test('per-key validation is performed after all defaults and final overrides are resolved', async () => {
  await expect(
    open([reportsFeature()], { queueOverrides: [{ queue: Reports, perKeyConcurrency: 1 }] })
  ).rejects.toMatchObject({ code: 'ACTIVITY_KEY_REQUIRED' })
})

test('non-global infrastructure works when explicitly imported and re-exported', async () => {
  @Module({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'explicit-root',
        storage: sqlite({ filename: ':memory:' }),
        isGlobal: false
      })
    ],
    exports: [WorkflowsModule]
  })
  class Infrastructure {}
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forFeature({
        name: 'explicit',
        imports: [Infrastructure],
        workflows: [ReportWorkflow],
        activities: [ReportActivities],
        queues: [{ queue: Reports }]
      })
    ]
  }).compile()
  try {
    await app.init()
    expect(
      await (await client(app, ReportWorkflow).start('explicit')).result({ timeout: '3s' })
    ).toBe('report:explicit')
  } finally {
    await app.close()
  }
})

test('configuration catalogs remain isolated across independent Nest applications', async () => {
  const a = await open([reportsFeature()], { defaults: { activities: { timeout: '1s' } } })
  const b = await open([reportsFeature()], { defaults: { activities: { timeout: '2s' } } })
  try {
    expect(
      a.get(WorkflowsRuntime).registry.activityContracts(ReportActivities)[0]?.options.timeout
    ).toBe('1s')
    expect(
      b.get(WorkflowsRuntime).registry.activityContracts(ReportActivities)[0]?.options.timeout
    ).toBe('2s')
  } finally {
    await a.close()
    await b.close()
  }
})

test('child workflows obey imported client visibility rather than global handler discovery', async () => {
  @Workflow({ name: 'child-local', version: 1, input: z.string(), output: z.string() })
  class Child {
    async run(value: string) {
      return value
    }
  }
  @Workflow({ name: 'parent-local', version: 1, input: z.string(), output: z.string() })
  class Parent {
    async run(value: string, ctx: WorkflowContext) {
      return ctx.child('child', Child, value)
    }
  }
  const childModule = () => WorkflowsModule.forFeature({ name: 'child', workflows: [Child] })
  const denied = await open([
    childModule(),
    WorkflowsModule.forFeature({ name: 'parent', workflows: [Parent] })
  ])
  try {
    await expect(
      (await client(denied, Parent).start('denied')).result({ timeout: '3s' })
    ).rejects.toMatchObject({ code: 'WORKFLOW_NOT_VISIBLE' })
  } finally {
    await denied.close()
  }
  const allowed = await open([
    WorkflowsModule.forFeature({ name: 'parent', workflows: [Parent], imports: [childModule()] })
  ])
  try {
    expect(await (await client(allowed, Parent).start('allowed')).result({ timeout: '3s' })).toBe(
      'allowed'
    )
  } finally {
    await allowed.close()
  }
})

test('transient implementation registrations and nameless configuration owners are rejected', async () => {
  @Activities({ queue: Reports })
  @Injectable({ scope: Scope.TRANSIENT })
  class Transient {
    @Activity({ name: 'transient', version: 1, input: z.string(), output: z.string() })
    async run(value: string) {
      return value
    }
  }
  // @Activities intentionally composes Injectable; use the Nest override last for this scope test.
  Injectable({ scope: Scope.TRANSIENT })(Transient)
  await expect(
    open([
      WorkflowsModule.forFeature({
        name: 'transient',
        activities: [Transient],
        queues: [{ queue: Reports }]
      })
    ])
  ).rejects.toMatchObject({ code: 'UNSUPPORTED_SCOPE' })
  await expect(
    open([WorkflowsModule.forFeature({ queues: [{ queue: Reports }] })])
  ).rejects.toMatchObject({ code: 'FEATURE_NAME_REQUIRED' })
})

test('one logical queue shares local slots across handlers owned by different features', async () => {
  let active = 0
  let peak = 0
  const called: string[] = []
  const work = async (label: string) => {
    active++
    peak = Math.max(peak, active)
    called.push(label)
    try {
      await new Promise((resolve) => setTimeout(resolve, 30))
      return label
    } finally {
      active--
    }
  }
  @Activities({ queue: Reports })
  class First {
    @Activity({ name: 'shared-first', version: 1, input: z.string(), output: z.string() })
    async execute(value: string) {
      return work(`first:${value}`)
    }
  }
  @Activities({ queue: Reports })
  class Second {
    @Activity({ name: 'shared-second', version: 1, input: z.string(), output: z.string() })
    async execute(value: string) {
      return work(`second:${value}`)
    }
  }
  const queueFeature = WorkflowsModule.forFeature({
    name: 'capacity',
    queues: [{ queue: Reports, concurrency: 1 }],
    exports: { queues: [Reports] }
  })
  @Module({ imports: [queueFeature], exports: [WorkflowsModule] })
  class CapacityModule {}
  const first = WorkflowsModule.forFeature({
    name: 'first',
    imports: [CapacityModule],
    activities: [First],
    exports: { activities: [First] }
  })
  const second = WorkflowsModule.forFeature({
    name: 'second',
    imports: [CapacityModule],
    activities: [Second],
    exports: { activities: [Second] }
  })
  @Workflow({ name: 'shared-batch', version: 1, input: z.string(), output: z.array(z.string()) })
  class SharedBatch {
    async run(input: string, ctx: WorkflowContext) {
      return ctx.map('batch', [0, 1, 2, 3], { key: String, concurrency: 4 }, (index, branch) =>
        branch.activities(index % 2 === 0 ? First : Second).execute(input, { stepId: 'work' })
      )
    }
  }
  const app = await open([
    WorkflowsModule.forFeature({
      name: 'batch',
      imports: [first, second],
      workflows: [SharedBatch]
    })
  ])
  try {
    const result = await (await client(app, SharedBatch).start('x')).result({ timeout: '5s' })
    expect(result).toEqual(['first:x', 'second:x', 'first:x', 'second:x'])
    expect(called.length).toBe(4)
    expect(peak).toBe(1)
    expect(app.get(WorkflowsRuntime).registry.queues.size).toBe(1)
  } finally {
    await app.close()
  }
})

test('feature workflow slots are shared across its workflows and nested under the root budget', async () => {
  let active = 0
  let featureActive = 0
  let peak = 0
  let featurePeak = 0
  const work = async (inside: boolean) => {
    active++
    if (inside) featureActive++
    peak = Math.max(peak, active)
    featurePeak = Math.max(featurePeak, featureActive)
    try {
      await new Promise((resolve) => setTimeout(resolve, 50))
      return inside
    } finally {
      active--
      if (inside) featureActive--
    }
  }
  // Test-only orchestration delays measure interpreter slots, not durable business timers.
  @Workflow({ name: 'slots-first', version: 1, input: z.string(), output: z.boolean() })
  class First {
    async run(_value: string) {
      return work(true)
    }
  }
  @Workflow({ name: 'slots-second', version: 1, input: z.string(), output: z.boolean() })
  class Second {
    async run(_value: string) {
      return work(true)
    }
  }
  @Workflow({ name: 'slots-independent', version: 1, input: z.string(), output: z.boolean() })
  class Independent {
    async run(_value: string) {
      return work(false)
    }
  }
  const app = await open(
    [
      WorkflowsModule.forFeature({
        name: 'bounded',
        workflows: [First, Second],
        execution: { workflows: { concurrency: 1 } }
      }),
      WorkflowsModule.forFeature({ name: 'independent', workflows: [Independent] })
    ],
    { execution: { workflows: { concurrency: 2 } } }
  )
  try {
    const handles = await Promise.all([
      client(app, First).start('a'),
      client(app, Second).start('b'),
      client(app, First).start('c'),
      client(app, Independent).start('d')
    ])
    expect(await Promise.all(handles.map((handle) => handle.result({ timeout: '5s' })))).toEqual([
      true,
      true,
      true,
      false
    ])
    expect(featurePeak).toBe(1)
    expect(peak).toBeLessThanOrEqual(2)
  } finally {
    await app.close()
  }
})

test('feature and process activity queue selectors are intersected, never additive', async () => {
  @Activities()
  class Selected {
    @Activity({
      name: 'selected-a',
      version: 1,
      queue: Reports,
      input: z.string(),
      output: z.string()
    })
    async a(value: string) {
      return value
    }
    @Activity({
      name: 'selected-b',
      version: 1,
      queue: Mail,
      input: z.string(),
      output: z.string()
    })
    async b(value: string) {
      return value
    }
  }
  const app = await open(
    [
      WorkflowsModule.forFeature({
        name: 'selected',
        activities: [Selected],
        queues: [{ queue: Reports }, { queue: Mail }],
        execution: { activities: { queues: [Mail] } }
      })
    ],
    { execution: { activities: { queues: [Reports] } } }
  )
  try {
    expect(
      [...app.get(WorkflowsRuntime).registry.activities.values()].map(
        (activity) => activity.enabled
      )
    ).toEqual([false, false])
  } finally {
    await app.close()
  }
})

test('cyclic reexports cannot grant access to an unrelated private queue', async () => {
  class A {}
  class B {}
  Module({
    imports: [
      WorkflowsModule.forFeature({
        name: 'cycle-a',
        imports: [forwardRef(() => B)],
        exports: { queues: [Reports] }
      })
    ],
    exports: [WorkflowsModule]
  })(A)
  Module({
    imports: [
      WorkflowsModule.forFeature({
        name: 'cycle-b',
        imports: [forwardRef(() => A)],
        exports: { queues: [Reports] }
      })
    ],
    exports: [WorkflowsModule]
  })(B)
  await expect(
    open([WorkflowsModule.forFeature({ name: 'private-owner', queues: [{ queue: Reports }] }), A])
  ).rejects.toMatchObject({ code: 'QUEUE_NOT_VISIBLE' })
})
