import { expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Database } from 'bun:sqlite'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Activities,
  ActivitiesContract,
  Activity,
  Cron,
  defineQueue,
  Interval,
  Workflow,
  WorkflowContract,
  WorkflowsModule
} from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { sqlite } from '../src/sqlite'
import { WorkflowsTestHarness, WorkflowsTestingModule } from '../src/testing'

const Input = z.object({ kind: z.string() })

test('Cron rejects malformed declarations at the decorator boundary', () => {
  const expectCode = (action: () => void, code: string) => {
    let error: unknown
    try {
      action()
    } catch (thrown) {
      error = thrown
    }
    expect(error).toMatchObject({ code })
  }

  expectCode(
    // SAFETY: This deliberately exercises the runtime boundary with malformed configuration.
    () => Cron(undefined as never),
    'INVALID_SCHEDULE'
  )
  expectCode(
    () => Cron({ name: 'invalid.cron', expression: 'not a cron expression' }),
    'INVALID_CRON_EXPRESSION'
  )
  expectCode(
    () => Cron({ name: 'invalid.timezone', expression: '0 * * * *', timezone: 'Mars/Olympus' }),
    'INVALID_SCHEDULE_TIMEZONE'
  )
})

@Cron({
  name: 'reports.daily',
  expression: '0 8 * * *',
  timezone: 'UTC',
  input: ({ scheduledAt }) => ({ kind: scheduledAt })
})
@Workflow({
  name: 'reports.generate',
  version: 1,
  input: Input,
  output: z.string()
})
class DailyReport {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.kind
  }
}

@Interval({ name: 'catalog.sync', every: '15m', input: { kind: 'catalog' } })
@Workflow({
  name: 'catalog.sync',
  version: 1,
  input: Input,
  output: z.string()
})
class CatalogSync {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.kind
  }
}

@Workflow({ name: 'catalog.sync', version: 1, input: Input, output: z.string() })
class CatalogSyncWithoutSchedule {
  async run(input: z.infer<typeof Input>): Promise<string> {
    return input.kind
  }
}

@Interval({ name: 'removed.owner-schedule', every: '1h', input: { kind: 'removed' } })
@Workflow({ name: 'removed.owner-workflow', version: 1, input: Input, output: z.string() })
class RemovedOwnerWorkflow {
  async run(input: z.infer<typeof Input>): Promise<string> {
    return input.kind
  }
}

@Workflow({ name: 'remaining.workflow', version: 1, input: Input, output: z.string() })
class RemainingWorkflow {
  async run(input: z.infer<typeof Input>): Promise<string> {
    return input.kind
  }
}

test('decorated workflows persist cron and interval definitions with initialized cursors', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedules-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'schedule-pr1',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'schedules',
        workflows: [DailyReport, CatalogSync]
      })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await app.init()
    expect(db.query('SELECT version FROM better_workflows_schema ORDER BY version').all()).toEqual(
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map((version) => ({ version }))
    )
    const schedules = db
      .query(
        `SELECT schedule_name, workflow_name, workflow_version, kind, expression, timezone,
                interval_ms, state, last_occurrence_at, next_occurrence_at, revision
         FROM better_workflows_schedules ORDER BY schedule_name`
      )
      .all()
    expect(schedules).toEqual([
      {
        schedule_name: 'catalog.sync',
        workflow_name: 'catalog.sync',
        workflow_version: 1,
        kind: 'interval',
        expression: null,
        timezone: null,
        interval_ms: 900000,
        state: 'active',
        last_occurrence_at: null,
        next_occurrence_at: expect.any(Number),
        revision: 0
      },
      {
        schedule_name: 'reports.daily',
        workflow_name: 'reports.generate',
        workflow_version: 1,
        kind: 'cron',
        expression: '0 8 * * *',
        timezone: 'UTC',
        interval_ms: null,
        state: 'active',
        last_occurrence_at: null,
        next_occurrence_at: expect.any(Number),
        revision: 0
      }
    ])
    expect(
      db
        .query<{ definition_hash: string }, []>(
          'SELECT definition_hash FROM better_workflows_schedules'
        )
        .all()
        .every(
          // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The SQLite assertion checks the migration's returned scalar shape.
          (row) => typeof row.definition_hash === 'string' && row.definition_hash.length === 64
        )
    ).toBe(true)
  } finally {
    db.close()
    await app.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('advanced schedules are discovered from the contract, not from the handler', async () => {
  @WorkflowContract({
    name: 'advanced.schedule-workflow',
    version: 1,
    input: z.string(),
    output: z.string()
  })
  @Cron({
    name: 'advanced.schedule',
    expression: '*/5 * * * *',
    timezone: 'UTC',
    input: 'advanced'
  })
  abstract class AdvancedWorkflow {
    abstract run(input: string, context: WorkflowContext): Promise<string>
  }
  @Workflow(AdvancedWorkflow)
  class AdvancedHandler implements AdvancedWorkflow {
    async run(input: string): Promise<string> {
      return input
    }
  }
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-advanced-schedule-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'advanced-schedule',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'advanced-schedule', workflows: [AdvancedHandler] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await app.init()
    expect(
      db
        .query<{ workflow_name: string; schedule_name: string }, []>(
          'SELECT workflow_name, schedule_name FROM better_workflows_schedules'
        )
        .all()
    ).toEqual([{ workflow_name: 'advanced.schedule-workflow', schedule_name: 'advanced.schedule' }])
  } finally {
    db.close()
    await app.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('contract-first schedules execute the separate handler regardless of decorator order', async () => {
  @Cron({
    name: 'advanced.e2e.schedule',
    expression: '0 8 * * *',
    timezone: 'UTC',
    input: 'from-contract'
  })
  @WorkflowContract({
    name: 'advanced.e2e.workflow',
    version: 1,
    input: z.string(),
    output: z.string()
  })
  abstract class ContractWorkflow {
    abstract run(input: string, context: WorkflowContext): Promise<string>
  }
  @Workflow(ContractWorkflow)
  class ContractHandler implements ContractWorkflow {
    async run(input: string): Promise<string> {
      return input
    }
  }
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-advanced-e2e-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'advanced-e2e',
        initialTime: Date.UTC(2026, 0, 1, 7, 59),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'advanced-e2e', workflows: [ContractHandler] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('1m')
    expect(
      db
        .query<{ workflow_name: string; state: string }, []>(
          `SELECT workflow_name, state FROM better_workflows_runs`
        )
        .all()
    ).toEqual([{ workflow_name: 'advanced.e2e.workflow', state: 'completed' }])
    expect(
      db
        .query<{ trigger_type: string; state: string }, []>(
          `SELECT trigger_type, state FROM better_workflows_schedule_occurrences`
        )
        .all()
    ).toEqual([{ trigger_type: 'scheduled', state: 'started' }])
  } finally {
    db.close()
    await app.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('scheduled contract-first workflows can execute contract-first activities', async () => {
  const queue = defineQueue('scheduled-contract-first-activities')
  const activityInput = z.object({ value: z.string() })

  @ActivitiesContract({ queue })
  abstract class ScheduledActivities {
    @Activity({
      name: 'scheduled.contract-first.activity',
      version: 1,
      input: activityInput,
      output: z.string()
    })
    execute(_input: z.infer<typeof activityInput>, _context: ActivityContext): Promise<string> {
      throw new Error('contract-only')
    }
  }

  @Activities(ScheduledActivities)
  class ScheduledActivitiesHandler implements ScheduledActivities {
    async execute(
      input: z.infer<typeof activityInput>,
      _context: ActivityContext
    ): Promise<string> {
      return `scheduled:${input.value}`
    }
  }

  @Interval({
    name: 'scheduled.contract-first.schedule',
    every: '1m',
    input: { value: 'one' }
  })
  @WorkflowContract({
    name: 'scheduled.contract-first.workflow',
    version: 1,
    input: activityInput,
    output: z.string()
  })
  abstract class ScheduledWorkflow {
    abstract run(input: z.infer<typeof activityInput>, context: WorkflowContext): Promise<string>
  }

  @Workflow(ScheduledWorkflow)
  class ScheduledWorkflowHandler implements ScheduledWorkflow {
    async run(input: z.infer<typeof activityInput>, context: WorkflowContext): Promise<string> {
      return context.activities(ScheduledActivities).execute(input, { stepId: 'execute' })
    }
  }

  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-scheduled-contract-first-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'scheduled-contract-first-activities',
        initialTime: Date.UTC(2026, 0, 1, 0, 0),
        storage: sqlite({ filename })
      }),
      WorkflowsModule.forFeature({
        name: 'scheduled-contract-first-activities',
        workflows: [ScheduledWorkflowHandler],
        activities: [ScheduledActivitiesHandler],
        queues: [{ queue, concurrency: 1 }]
      })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('1m')
    expect(
      db
        .query<{ workflow_name: string; state: string }, []>(
          'SELECT workflow_name, state FROM better_workflows_runs'
        )
        .all()
    ).toEqual([{ workflow_name: 'scheduled.contract-first.workflow', state: 'completed' }])
  } finally {
    db.close()
    await app.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('client-only contracts do not acquire schedule ownership', async () => {
  @Cron({ name: 'client-only.schedule', expression: '0 * * * *' })
  @WorkflowContract({
    name: 'client-only.workflow',
    version: 1,
    input: z.string(),
    output: z.string()
  })
  abstract class ClientOnlyWorkflow {
    abstract run(input: string, context: WorkflowContext): Promise<string>
  }
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-client-schedule-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'client-only-schedule',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ clients: [ClientOnlyWorkflow] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await app.init()
    expect(db.query('SELECT * FROM better_workflows_schedules').all()).toEqual([])
  } finally {
    db.close()
    await app.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('client-only startup does not orphan schedules owned by another deployment', async () => {
  @Cron({ name: 'client-existing.schedule', expression: '0 * * * *' })
  @WorkflowContract({
    name: 'client-existing.workflow',
    version: 1,
    input: z.string(),
    output: z.string()
  })
  abstract class ExistingWorkflow {
    abstract run(input: string, context: WorkflowContext): Promise<string>
  }
  @Workflow(ExistingWorkflow)
  class ExistingHandler implements ExistingWorkflow {
    async run(input: string): Promise<string> {
      return input
    }
  }
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-client-existing-'))
  const filename = join(directory, 'workflows.sqlite')
  const root = WorkflowsModule.forRoot({
    namespace: 'client-existing-schedule',
    storage: sqlite({ filename }),
    execution: { workflows: { enabled: false }, activities: { enabled: false } }
  })
  const owner = await Test.createTestingModule({
    imports: [root, WorkflowsModule.forFeature({ name: 'owner', workflows: [ExistingHandler] })]
  }).compile()
  try {
    await owner.init()
  } finally {
    await owner.close()
  }
  const client = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'client-existing-schedule',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ clients: [ExistingWorkflow] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await client.init()
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='client-existing.schedule'`
        )
        .get()
    ).toEqual({ state: 'active' })
  } finally {
    db.close()
    await client.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('persisted schedule definition changes fail bootstrap instead of moving the cursor', async () => {
  @Cron({ name: 'drift.schedule', expression: '0 8 * * *' })
  @Workflow({ name: 'drift.workflow', version: 1, input: z.string(), output: z.string() })
  class FirstDefinition {
    async run(input: string): Promise<string> {
      return input
    }
  }
  @Cron({ name: 'drift.schedule', expression: '0 9 * * *' })
  @Workflow({ name: 'drift.workflow', version: 1, input: z.string(), output: z.string() })
  class ChangedDefinition {
    async run(input: string): Promise<string> {
      return input
    }
  }
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-drift-'))
  const filename = join(directory, 'workflows.sqlite')
  const root = (namespace: string) =>
    WorkflowsModule.forRoot({
      namespace,
      storage: sqlite({ filename }),
      execution: { workflows: { enabled: false }, activities: { enabled: false } }
    })
  const first = await Test.createTestingModule({
    imports: [
      root('drift'),
      WorkflowsModule.forFeature({ name: 'drift', workflows: [FirstDefinition] })
    ]
  }).compile()
  try {
    await first.init()
  } finally {
    await first.close()
  }
  const changed = await Test.createTestingModule({
    imports: [
      root('drift'),
      WorkflowsModule.forFeature({ name: 'drift', workflows: [ChangedDefinition] })
    ]
  }).compile()
  try {
    await expect(changed.init()).rejects.toMatchObject({ code: 'SCHEDULE_DEFINITION_CHANGED' })
  } finally {
    await changed.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('static schedule input is checked against the workflow schema during bootstrap', async () => {
  @Cron({ name: 'invalid-input.schedule', expression: '0 * * * *', input: {} })
  @Workflow({
    name: 'invalid-input.workflow',
    version: 1,
    input: Input,
    output: z.string()
  })
  class InvalidInput {
    async run(input: z.infer<typeof Input>): Promise<string> {
      return input.kind
    }
  }
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'invalid-input',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'invalid-input', workflows: [InvalidInput] })
    ]
  }).compile()
  await expect(app.init()).rejects.toMatchObject({ code: 'VALIDATION_FAILED' })
  await app.close().catch(() => undefined)
})

test('a partial deployment preserves schedules while another owner lease is live', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-orphan-'))
  const filename = join(directory, 'workflows.sqlite')
  const first = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'orphan',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'orphan', workflows: [DailyReport, CatalogSync] })
    ]
  }).compile()
  try {
    await first.init()
  } finally {
    await first.close()
  }
  const second = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'orphan',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      // The same workflow owner reconciles a definition that disappeared from its
      // catalog; a different partial deployment must not touch this schedule.
      WorkflowsModule.forFeature({ name: 'orphan', workflows: [CatalogSyncWithoutSchedule] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await second.init()
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='catalog.sync'`
        )
        .get()
    ).toEqual({ state: 'active' })
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='reports.daily'`
        )
        .get()
    ).toEqual({ state: 'active' })
  } finally {
    db.close()
    await second.close().catch(() => undefined)
  }
  const restored = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'orphan',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'orphan-restored', workflows: [CatalogSync] })
    ]
  }).compile()
  const restoredDb = new Database(filename)
  try {
    await restored.init()
    expect(
      restoredDb
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='catalog.sync'`
        )
        .get()
    ).toEqual({ state: 'active' })
  } finally {
    restoredDb.close()
    await restored.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('a completely removed workflow is reconciled after its persistent owner lease expires', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-complete-removal-'))
  const filename = join(directory, 'workflows.sqlite')
  const options = {
    namespace: 'complete-removal',
    storage: sqlite({ filename }),
    lease: { duration: '30ms' as const, refreshInterval: '10ms' as const },
    execution: { workflows: { enabled: false }, activities: { enabled: false } }
  }
  const first = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({ name: 'removed-owner', workflows: [RemovedOwnerWorkflow] })
    ]
  }).compile()
  try {
    await first.init()
  } finally {
    await first.close()
  }
  await new Promise((resolve) => setTimeout(resolve, 60))
  const second = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({ name: 'remaining', workflows: [RemainingWorkflow] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await second.init()
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='removed.owner-schedule'`
        )
        .get()
    ).toEqual({ state: 'orphaned' })
  } finally {
    db.close()
    await second.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('a live owner from a partial deployment protects its schedule from reconciliation', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-partial-owner-'))
  const filename = join(directory, 'workflows.sqlite')
  const options = {
    namespace: 'partial-owner',
    storage: sqlite({ filename }),
    lease: { duration: '30ms' as const, refreshInterval: '10ms' as const },
    execution: { workflows: { enabled: false }, activities: { enabled: false } }
  }
  const owner = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({ name: 'owner', workflows: [RemovedOwnerWorkflow] })
    ]
  }).compile()
  const partial = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({ name: 'partial', workflows: [RemainingWorkflow] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await owner.init()
    await new Promise((resolve) => setTimeout(resolve, 15))
    db.exec("DELETE FROM better_workflows_schedule_owners WHERE namespace='partial-owner'")
    await new Promise((resolve) => setTimeout(resolve, 25))
    await partial.init()
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='removed.owner-schedule'`
        )
        .get()
    ).toEqual({ state: 'active' })
  } finally {
    db.close()
    await partial.close().catch(() => undefined)
    await owner.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})

test('a removed schedule is orphaned after its old owner stops during a rolling deployment', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-rolling-removal-'))
  const filename = join(directory, 'workflows.sqlite')
  const options = {
    namespace: 'rolling-removal',
    storage: sqlite({ filename }),
    lease: { duration: '30ms' as const, refreshInterval: '10ms' as const },
    execution: { workflows: { enabled: false }, activities: { enabled: false } }
  }
  const old = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({ name: 'old', workflows: [CatalogSync] })
    ]
  }).compile()
  const replacement = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({ name: 'replacement', workflows: [CatalogSyncWithoutSchedule] })
    ]
  }).compile()
  const db = new Database(filename)
  try {
    await old.init()
    await replacement.init()
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='catalog.sync'`
        )
        .get()
    ).toEqual({ state: 'active' })
    await old.close()
    await new Promise((resolve) => setTimeout(resolve, 60))
    expect(
      db
        .query<{ state: string }, []>(
          `SELECT state FROM better_workflows_schedules WHERE schedule_name='catalog.sync'`
        )
        .get()
    ).toEqual({ state: 'orphaned' })
  } finally {
    db.close()
    await replacement.close().catch(() => undefined)
    await old.close().catch(() => undefined)
    await rm(directory, { recursive: true, force: true })
  }
})
