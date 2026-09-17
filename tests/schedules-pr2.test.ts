import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import { Cron, Interval, Workflow, WorkflowsModule } from '../src'
import type { IntervalOptions, WorkflowContext } from '../src'
import { WorkflowsTestHarness, WorkflowsTestingModule } from '../src/testing'
import { sqlite } from '../src/sqlite'

const Input = z.object({ at: z.string() })

@Interval({
  name: 'pr2.interval',
  every: '15m',
  input: ({ scheduledAt }) => ({ at: scheduledAt })
})
@Workflow({ name: 'pr2.interval-workflow', version: 1, input: Input, output: z.string() })
class IntervalWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.at
  }
}

@Interval({
  name: 'pr2.feature-enabled',
  every: '15m',
  input: ({ scheduledAt }) => ({ at: scheduledAt })
})
@Workflow({ name: 'pr2.feature-enabled-workflow', version: 1, input: Input, output: z.string() })
class FeatureEnabledIntervalWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.at
  }
}

@Cron({
  name: 'pr2.cron',
  expression: '0 8 * * *',
  timezone: 'UTC',
  input: ({ scheduledAt }) => ({ at: scheduledAt })
})
@Workflow({ name: 'pr2.cron-workflow', version: 1, input: Input, output: z.string() })
class CronWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.at
  }
}

function defineMisfireWorkflow(
  name: string,
  policy: 'skip' | 'latest' | 'catch-up',
  maxCatchUp?: number
) {
  class MisfireWorkflow {
    async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
      return input.at
    }
  }
  Workflow({ name: `${name}.workflow`, version: 1, input: Input, output: z.string() })(
    MisfireWorkflow
  )
  const options: IntervalOptions<{ at: string }> =
    maxCatchUp === undefined
      ? {
          name,
          every: '15m',
          misfire: policy,
          input: ({ scheduledAt }) => ({ at: scheduledAt })
        }
      : {
          name,
          every: '15m',
          misfire: policy,
          maxCatchUp,
          input: ({ scheduledAt }) => ({ at: scheduledAt })
        }
  Interval(options)(MisfireWorkflow)
  return MisfireWorkflow
}

async function runMisfire(
  namespace: string,
  scheduleName: string,
  policy: 'skip' | 'latest' | 'catch-up',
  maxCatchUp?: number,
  later = Date.UTC(2026, 0, 1, 9, 1)
) {
  const directory = await mkdtemp(join(tmpdir(), `better-workflows-${namespace}-`))
  const filename = join(directory, 'workflows.sqlite')
  const WorkflowClass = defineMisfireWorkflow(scheduleName, policy, maxCatchUp)
  const initial = Date.UTC(2026, 0, 1, 8)
  const root = (schedules: boolean, time: number) =>
    WorkflowsTestingModule.forRoot({
      namespace,
      initialTime: time,
      storage: sqlite({ filename }),
      execution: { schedules: { enabled: schedules }, activities: { enabled: false } }
    })
  const first = await Test.createTestingModule({
    imports: [
      root(false, initial),
      WorkflowsModule.forFeature({ name: namespace, workflows: [WorkflowClass] })
    ]
  }).compile()
  await first.init()
  await first.close()
  const second = await Test.createTestingModule({
    imports: [
      root(true, later),
      WorkflowsModule.forFeature({ name: namespace, workflows: [WorkflowClass] })
    ]
  }).compile()
  try {
    await second.init()
    await second.get(WorkflowsTestHarness).runUntilIdle()
    const db = new Database(filename)
    try {
      return db
        .query<
          { scheduled_at: number; state: string; trigger_type: string; reason_code: string | null },
          []
        >(
          `SELECT scheduled_at, state, trigger_type, reason_code
           FROM better_workflows_schedule_occurrences ORDER BY scheduled_at`
        )
        .all()
    } finally {
      db.close()
    }
  } finally {
    await second.close()
    await rm(directory, { recursive: true, force: true })
  }
}

test('scheduler materializes interval occurrences from the virtual business clock', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr2-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr2-normal',
        initialTime: Date.UTC(2026, 0, 1, 8),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'pr2', workflows: [IntervalWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    const harness = app.get(WorkflowsTestHarness)
    await harness.advanceTime('1h')
    const db = new Database(filename)
    try {
      const occurrences = db
        .query<{ scheduled_at: number; state: string; trigger_type: string }, []>(
          `SELECT scheduled_at, state, trigger_type
           FROM better_workflows_schedule_occurrences
           ORDER BY scheduled_at`
        )
        .all()
      expect(occurrences).toEqual([
        { scheduled_at: Date.UTC(2026, 0, 1, 8, 15), state: 'started', trigger_type: 'scheduled' },
        { scheduled_at: Date.UTC(2026, 0, 1, 8, 30), state: 'started', trigger_type: 'scheduled' },
        { scheduled_at: Date.UTC(2026, 0, 1, 8, 45), state: 'started', trigger_type: 'scheduled' },
        { scheduled_at: Date.UTC(2026, 0, 1, 9), state: 'started', trigger_type: 'scheduled' }
      ])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('scheduler materializes a cron occurrence at its timezone-aware deadline', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr2-cron-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr2-cron',
        initialTime: Date.UTC(2026, 0, 1, 7, 59),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'pr2-cron', workflows: [CronWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('2m')
    const db = new Database(filename)
    try {
      expect(
        db
          .query<{ scheduled_at: number; state: string }, []>(
            `SELECT scheduled_at, state FROM better_workflows_schedule_occurrences`
          )
          .all()
      ).toEqual([{ scheduled_at: Date.UTC(2026, 0, 1, 8), state: 'started' }])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('scheduler role persists starts without enabling local workflow execution', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr2-role-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr2-role',
        initialTime: Date.UTC(2026, 0, 1, 8),
        storage: sqlite({ filename }),
        execution: {
          workflows: { enabled: false },
          activities: { enabled: false },
          schedules: { enabled: true }
        }
      }),
      WorkflowsModule.forFeature({ name: 'pr2-role', workflows: [IntervalWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('15m')
    const db = new Database(filename)
    try {
      expect(
        db
          .query<{ occurrences: number; runs: number }, []>(
            `SELECT
               (SELECT COUNT(*) FROM better_workflows_schedule_occurrences) AS occurrences,
               (SELECT COUNT(*) FROM better_workflows_runs) AS runs`
          )
          .get()
      ).toEqual({ occurrences: 1, runs: 1 })
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('feature schedule execution can be disabled without dropping its persisted definition', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr2-feature-role-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr2-feature-role',
        initialTime: Date.UTC(2026, 0, 1, 8),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'pr2-feature-role-disabled',
        workflows: [IntervalWorkflow],
        execution: { schedules: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'pr2-feature-role-enabled',
        workflows: [FeatureEnabledIntervalWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('15m')
    const db = new Database(filename)
    try {
      expect(
        db
          .query<{ schedule_name: string; state: string }, []>(
            'SELECT schedule_name, state FROM better_workflows_schedule_occurrences'
          )
          .all()
      ).toEqual([{ schedule_name: 'pr2.feature-enabled', state: 'started' }])
      expect(
        db
          .query<{ state: string }, []>(
            `SELECT state FROM better_workflows_schedules WHERE schedule_name='pr2.interval'`
          )
          .all()
      ).toEqual([{ state: 'active' }])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('a feature can accept schedules without locally dispatching its disabled workflow handler', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr2-feature-workflow-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr2-feature-workflow',
        initialTime: Date.UTC(2026, 0, 1, 8),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'pr2-feature-workflow',
        workflows: [IntervalWorkflow],
        execution: { workflows: { enabled: false } }
      })
    ]
  }).compile()
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('15m')
    const db = new Database(filename)
    try {
      expect(
        db
          .query<{ state: string; dispatched: number }, []>(
            'SELECT state, dispatched FROM better_workflows_runs'
          )
          .all()
      ).toEqual([{ state: 'accepted', dispatched: 0 }])
      expect(db.query('SELECT state FROM better_workflows_schedule_occurrences').all()).toEqual([
        { state: 'started' }
      ])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('scheduler applies skip, latest and catch-up policies after restart', async () => {
  const skipped = await runMisfire('schedule-pr2-skip', 'pr2.skip', 'skip')
  expect(skipped).toHaveLength(4)
  expect(skipped.every((row) => row.state === 'skipped' && row.reason_code === 'misfire')).toBe(
    true
  )

  const latest = await runMisfire('schedule-pr2-latest', 'pr2.latest', 'latest')
  expect(latest).toHaveLength(4)
  expect(latest.slice(0, 3).every((row) => row.state === 'skipped')).toBe(true)
  expect(latest.at(-1)).toEqual({
    scheduled_at: Date.UTC(2026, 0, 1, 9),
    state: 'started',
    trigger_type: 'catch-up',
    reason_code: null
  })

  const catchUp = await runMisfire('schedule-pr2-catch-up', 'pr2.catch-up', 'catch-up')
  expect(catchUp).toHaveLength(4)
  expect(catchUp.every((row) => row.state === 'started' && row.trigger_type === 'catch-up')).toBe(
    true
  )
})

test('misfire skip starts the latest current deadline after an older backlog', async () => {
  const occurrences = await runMisfire(
    'schedule-pr2-skip-current',
    'pr2.skip-current',
    'skip',
    undefined,
    Date.UTC(2026, 0, 1, 8, 30)
  )
  expect(occurrences).toEqual([
    {
      scheduled_at: Date.UTC(2026, 0, 1, 8, 15),
      state: 'skipped',
      trigger_type: 'catch-up',
      reason_code: 'misfire'
    },
    {
      scheduled_at: Date.UTC(2026, 0, 1, 8, 30),
      state: 'started',
      trigger_type: 'scheduled',
      reason_code: null
    }
  ])
})

test('catch-up drains a bounded backlog over successive durable scheduler passes', async () => {
  const occurrences = await runMisfire('schedule-pr2-batch', 'pr2.batch', 'catch-up', 2)
  expect(occurrences).toHaveLength(4)
  expect(occurrences.map((row) => row.scheduled_at)).toEqual([
    Date.UTC(2026, 0, 1, 8, 15),
    Date.UTC(2026, 0, 1, 8, 30),
    Date.UTC(2026, 0, 1, 8, 45),
    Date.UTC(2026, 0, 1, 9)
  ])
})

test('overlap skip treats a continue-as-new chain as active', async () => {
  @Interval({ name: 'pr2.continuation', every: '15m', overlap: 'skip', input: { at: 'chain' } })
  @Workflow({ name: 'pr2.continuation-workflow', version: 1, input: Input, output: z.void() })
  class ContinuationWorkflow {
    async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<void> {
      await context.sleep('before-continue', '1h')
      await context.continueAsNew(input)
    }
  }
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr2-chain-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr2-chain',
        initialTime: Date.UTC(2026, 0, 1, 8),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'pr2-chain', workflows: [ContinuationWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    const harness = app.get(WorkflowsTestHarness)
    await harness.advanceTime('75m')
    await harness.advanceTime('1m')
    const db = new Database(filename)
    try {
      const occurrences = db
        .query<{ scheduled_at: number; state: string; reason_code: string | null }, []>(
          `SELECT scheduled_at, state, reason_code
           FROM better_workflows_schedule_occurrences ORDER BY scheduled_at`
        )
        .all()
      expect(occurrences).toEqual([
        { scheduled_at: Date.UTC(2026, 0, 1, 8, 15), state: 'started', reason_code: null },
        { scheduled_at: Date.UTC(2026, 0, 1, 8, 30), state: 'skipped', reason_code: 'overlap' },
        { scheduled_at: Date.UTC(2026, 0, 1, 8, 45), state: 'skipped', reason_code: 'overlap' },
        { scheduled_at: Date.UTC(2026, 0, 1, 9), state: 'skipped', reason_code: 'overlap' },
        { scheduled_at: Date.UTC(2026, 0, 1, 9, 15), state: 'skipped', reason_code: 'overlap' }
      ])
      expect(
        db
          .query<{ state: string; generation: number }, []>(
            `SELECT state, generation FROM better_workflows_runs ORDER BY generation`
          )
          .all()
      ).toEqual([
        { state: 'continued', generation: 0 },
        { state: 'waiting', generation: 1 }
      ])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})
