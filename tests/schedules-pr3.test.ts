import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import { Interval, Workflow, WorkflowsAdmin, WorkflowsModule } from '../src'
import type { WorkflowContext } from '../src'
import { WorkflowsTestHarness, WorkflowsTestingModule } from '../src/testing'
import { createWorkflowsAdmin } from '../src/admin'
import { sqlite } from '../src/sqlite'
import { eventually } from './helpers'

const Input = z.object({ kind: z.string() })
const invalidScheduleInput = { kind: 123 }

@Interval({
  name: 'pr3.admin',
  every: '1h',
  input: { kind: 'manual' }
})
@Workflow({ name: 'pr3.admin-workflow', version: 1, input: Input, output: z.string() })
class AdminWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.kind
  }
}

@Interval({
  name: 'pr3.failed',
  every: '1h',
  input: () => invalidScheduleInput
})
@Workflow({ name: 'pr3.failed-workflow', version: 1, input: Input, output: z.string() })
class FailedScheduleWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.kind
  }
}

@Interval({ name: 'pr3.one-ms', every: '1ms', input: { kind: 'one-ms' } })
@Workflow({ name: 'pr3.one-ms-workflow', version: 1, input: Input, output: z.string() })
class OneMillisecondWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.kind
  }
}

test('admin lists, pauses, resumes and manually triggers a schedule without moving its cursor', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr3-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr3',
        initialTime: Date.UTC(2026, 0, 1),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'pr3', workflows: [AdminWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    const admin = app.get(WorkflowsAdmin)
    const before = await admin.getSchedule('pr3.admin')
    expect(before).toMatchObject({ name: 'pr3.admin', status: 'active' })
    expect((await admin.stats()).schedules).toEqual({
      active: 1,
      paused: 0,
      overdue: 0,
      oldestLagMs: 0
    })

    expect((await admin.pauseSchedule('pr3.admin')).status).toBe('paused')
    expect((await admin.resumeSchedule('pr3.admin')).status).toBe('active')
    const triggered = await admin.triggerSchedule('pr3.admin', { idempotencyKey: 'operator-1' })
    expect(triggered.occurrence.trigger).toBe('manual')
    expect(
      (await admin.triggerSchedule('pr3.admin', { idempotencyKey: 'operator-1' })).created
    ).toBe(false)
    expect((await admin.getSchedule('pr3.admin')).nextOccurrence).toBe(before.nextOccurrence)

    await app.get(WorkflowsTestHarness).flush()
    const db = new Database(filename)
    try {
      expect(
        db
          .query<{ trigger_type: string; state: string; execution_id: string | null }, []>(
            `SELECT trigger_type, state, execution_id
             FROM better_workflows_schedule_occurrences`
          )
          .all()
      ).toEqual([{ trigger_type: 'manual', state: 'started', execution_id: triggered.executionId }])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('repeated manual triggers cannot consume a future one-millisecond occurrence', async () => {
  const initial = Date.UTC(2026, 0, 1)
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr3-one-ms',
        initialTime: initial,
        storage: sqlite({ filename: ':memory:', runtime: 'auto' }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'schedule-pr3-one-ms',
        workflows: [OneMillisecondWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    const harness = app.get(WorkflowsTestHarness)
    const admin = app.get(WorkflowsAdmin)
    await admin.pauseSchedule('pr3.one-ms')
    harness.clock.advanceTo(initial + 1)
    for (let index = 0; index < 20; index++)
      await admin.triggerSchedule('pr3.one-ms', { idempotencyKey: `manual-${index}` })
    await admin.resumeSchedule('pr3.one-ms')
    await harness.flush()
    await harness.advanceTime('1ms')
    const occurrences = await admin.listScheduleOccurrences('pr3.one-ms', { limit: 100 })
    expect(occurrences.occurrences.filter((item) => item.trigger === 'scheduled')).toHaveLength(2)
    const manual = occurrences.occurrences.filter((item) => item.trigger === 'manual')
    expect(manual).toHaveLength(20)
    expect(new Set(manual.map((item) => item.scheduledAt))).toEqual(
      new Set([new Date(initial + 1).toISOString()])
    )
  } finally {
    await app.close()
  }
})

test('standalone schedule admin can list and trigger persisted static input', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr3-standalone-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr3-standalone',
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'pr3-standalone', workflows: [AdminWorkflow] })
    ]
  }).compile()
  await app.init()
  await app.close()
  try {
    const admin = await createWorkflowsAdmin({
      namespace: 'schedule-pr3-standalone',
      storage: sqlite({ filename })
    })
    try {
      expect(await admin.listSchedules()).toHaveLength(1)
      const result = await admin.triggerSchedule('pr3.admin')
      expect(result.occurrence.trigger).toBe('manual')
      const reconciled = await admin.updateScheduleDefinition('pr3.admin', {
        confirm: true,
        from: 'now',
        definition: {
          workflow: 'pr3.admin-workflow',
          workflowVersion: 1,
          type: 'interval',
          intervalMs: 2 * 60 * 60 * 1_000,
          misfire: 'latest',
          overlap: 'allow',
          maxCatchUp: 100,
          inputMode: 'static',
          input: { kind: 'reconciled' }
        }
      })
      expect(reconciled).toMatchObject({ name: 'pr3.admin', type: 'interval', status: 'active' })
    } finally {
      await admin.close()
    }
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('a running scheduler refuses to materialize a definition reconciled by another process', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-reconcile-live-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'schedule-reconcile-live',
        storage: sqlite({ filename }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } },
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({ name: 'schedule-reconcile-live', workflows: [AdminWorkflow] })
    ]
  }).compile()
  let standalone: Awaited<ReturnType<typeof createWorkflowsAdmin>> | undefined
  try {
    await app.init()
    standalone = await createWorkflowsAdmin({
      namespace: 'schedule-reconcile-live',
      storage: sqlite({ filename })
    })
    await standalone.updateScheduleDefinition('pr3.admin', {
      confirm: true,
      from: 'now',
      definition: {
        workflow: 'pr3.admin-workflow',
        workflowVersion: 1,
        type: 'interval',
        intervalMs: 20,
        misfire: 'latest',
        overlap: 'allow',
        maxCatchUp: 100,
        inputMode: 'static',
        input: { kind: 'reconciled-live' }
      }
    })
    await new Promise((resolve) => setTimeout(resolve, 150))
    const database = new Database(filename)
    try {
      expect(
        database.query('SELECT COUNT(*) AS count FROM better_workflows_schedule_occurrences').get()
      ).toEqual({ count: 0 })
    } finally {
      database.close()
    }
  } finally {
    await standalone?.close().catch(() => undefined)
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('schedule occurrence history is paginated and exposes failed outcomes without inputs', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr3-history-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr3-history',
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'pr3-history',
        workflows: [AdminWorkflow, FailedScheduleWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    const admin = app.get(WorkflowsAdmin)
    const first = await admin.triggerSchedule('pr3.admin')
    const second = await admin.triggerSchedule('pr3.admin')
    const firstPage = await admin.listScheduleOccurrences('pr3.admin', { limit: 1 })
    expect(firstPage.occurrences).toHaveLength(1)
    expect(firstPage.occurrences[0]).toMatchObject({
      occurrence: first.occurrence.occurrence,
      state: 'started',
      executionId: first.executionId,
      trigger: 'manual'
    })
    expect(firstPage.nextCursor).toBe(first.occurrence.occurrence)
    const secondPage = await admin.listScheduleOccurrences('pr3.admin', {
      after: firstPage.nextCursor!,
      limit: 1
    })
    expect(secondPage.occurrences).toHaveLength(1)
    expect(secondPage.occurrences[0]).toMatchObject({
      occurrence: second.occurrence.occurrence,
      state: 'started',
      executionId: second.executionId
    })
    expect(secondPage.nextCursor).toBeUndefined()

    await expect(
      admin.triggerSchedule('pr3.failed', { idempotencyKey: 'failed-manual-1' })
    ).rejects.toMatchObject({
      code: 'SCHEDULE_INPUT_ERROR'
    })
    await expect(
      admin.triggerSchedule('pr3.failed', { idempotencyKey: 'failed-manual-1' })
    ).rejects.toMatchObject({ code: 'SCHEDULE_INPUT_ERROR' })
    const failures = await admin.listScheduleOccurrences('pr3.failed', { state: 'failed' })
    expect(failures.occurrences).toHaveLength(1)
    expect(failures.occurrences[0]).toMatchObject({
      state: 'failed',
      reasonCode: 'SCHEDULE_INPUT_ERROR',
      trigger: 'manual'
    })
    expect(failures.occurrences[0]).not.toHaveProperty('input')
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('schedule removal requires a paused definition and preserves occurrence history', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-pr3-remove-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr3-remove',
        initialTime: Date.UTC(2026, 0, 1),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'pr3-remove', workflows: [AdminWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    const admin = app.get(WorkflowsAdmin)
    await expect(admin.removeSchedule('pr3.admin', { confirm: true })).rejects.toMatchObject({
      code: 'SCHEDULE_MUST_BE_PAUSED'
    })
    await admin.triggerSchedule('pr3.admin')
    await admin.pauseSchedule('pr3.admin')
    await admin.removeSchedule('pr3.admin', { confirm: true })
    expect(await admin.listSchedules()).toEqual([])
    expect(await admin.listScheduleOccurrences('pr3.admin')).toMatchObject({
      occurrences: [{ state: 'started', trigger: 'manual' }]
    })

    const db = new Database(filename)
    try {
      expect(
        db
          .query<{ state: string; trigger_type: string }, []>(
            'SELECT state, trigger_type FROM better_workflows_schedule_occurrences'
          )
          .all()
      ).toEqual([{ state: 'started', trigger_type: 'manual' }])
    } finally {
      db.close()
    }
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('execution retention clears occurrence execution links while preserving schedule history', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-retention-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-retention',
        initialTime: Date.UTC(2026, 0, 1),
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ name: 'schedule-retention', workflows: [AdminWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    const admin = app.get(WorkflowsAdmin)
    const trigger = await admin.triggerSchedule('pr3.admin', { idempotencyKey: 'retention-1' })
    await eventually(
      async () => {
        const db = new Database(filename)
        try {
          return db
            .query<{ state: string }, []>(
              `SELECT state FROM better_workflows_runs WHERE execution_id='${trigger.executionId}'`
            )
            .get()?.state
        } finally {
          db.close()
        }
      },
      (state) => state === 'completed'
    )
    // SQLite's database clock is integer-millisecond precision; stay below it so
    // the cutoff cannot race the read by a sub-millisecond timestamp.
    const plan = await admin.retention.preview({
      before: new Date(Date.now() - 1_000).toISOString()
    })
    expect(plan.candidates.map((candidate) => candidate.executionId)).toContain(trigger.executionId)
    await admin.retention.prune(plan, { confirm: true })
    expect(await admin.listScheduleOccurrences('pr3.admin')).toMatchObject({
      occurrences: [{ state: 'started', trigger: 'manual' }]
    })
    expect((await admin.listScheduleOccurrences('pr3.admin')).occurrences[0]).not.toHaveProperty(
      'executionId'
    )
    expect(await admin.getSchedule('pr3.admin')).not.toHaveProperty('lastExecutionId')
    await expect(
      admin.triggerSchedule('pr3.admin', { idempotencyKey: 'retention-1' })
    ).rejects.toMatchObject({ code: 'EXECUTION_PRUNED' })
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('schedule occurrence retention preserves the highest sequence after manual history', async () => {
  const initial = Date.UTC(2026, 0, 1)
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-occurrence-retention-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-occurrence-retention',
        initialTime: initial,
        storage: sqlite({ filename }),
        execution: { activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'schedule-occurrence-retention',
        workflows: [AdminWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    const harness = app.get(WorkflowsTestHarness)
    await harness.advanceTime('4h')
    const admin = app.get(WorkflowsAdmin)
    const manual = await admin.triggerSchedule('pr3.admin')
    const before = new Date(initial + 5 * 60 * 60 * 1_000).toISOString()
    const plan = await admin.previewScheduleRetention({ before, limit: 100 })
    expect(plan.candidates.map((candidate) => candidate.sequence)).not.toContain(
      manual.occurrence.occurrence
    )
    const result = await admin.pruneScheduleRetention(plan, { confirm: true })
    expect(result).toMatchObject({ deleted: 4 })
    const remaining = (await admin.listScheduleOccurrences('pr3.admin')).occurrences
    expect(remaining).toHaveLength(1)
    expect(remaining[0]).toMatchObject({
      occurrence: manual.occurrence.occurrence,
      trigger: 'manual'
    })
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})
