import { expect, test } from 'bun:test'
import { ManagedRuntime } from 'effect'
import type { Type } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import { Interval, Workflow, WorkflowsAdmin, WorkflowsHealth, WorkflowsModule } from '../src'
import { sqlite } from '../src/sqlite'
import { WorkflowsTestHarness, WorkflowsTestingModule } from '../src/testing'
import { nextScheduleAt, normalizeCron, scheduleDefinitionHash } from '../src/internal/schedule'
import {
  metricAttributes,
  TelemetryService,
  telemetryLayer,
  TelemetryAttributeKey
} from '../src/internal/telemetry'
import { eventually } from './helpers'
import type { WorkflowContext } from '../src'

const Input = z.object({ value: z.string() })

@Interval({ name: 'pr4.health', every: '1h', input: { value: 'private' } })
@Workflow({ name: 'pr4.health-workflow', version: 1, input: Input, output: z.string() })
class SchedulerHealthWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.value
  }
}

@Interval({
  name: 'pr4.skip-normal',
  every: '50ms',
  misfire: 'skip',
  input: { value: 'normal' }
})
@Workflow({ name: 'pr4.skip-normal-workflow', version: 1, input: Input, output: z.string() })
class NormalSkipWorkflow {
  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return input.value
  }
}

const SlowInput = z.object({ value: z.string() }).refine(async () => {
  await new Promise((resolve) => setTimeout(resolve, 100))
  return true
})

@Interval({ name: 'pr4.lease-renewal', every: '1h', input: () => ({ value: 'slow' }) })
@Workflow({ name: 'pr4.lease-renewal-workflow', version: 1, input: SlowInput, output: z.string() })
class LeaseRenewalWorkflow {
  async run(input: z.infer<typeof SlowInput>, _context: WorkflowContext): Promise<string> {
    return input.value
  }
}

function bulkScheduleWorkflows(count: number): Type[] {
  const workflows: Type[] = []
  for (let index = 0; index < count; index++) {
    class BulkWorkflow {
      async run(input: z.infer<typeof Input>): Promise<string> {
        return input.value
      }
    }
    Workflow({
      name: `pr4.bulk-workflow-${index}`,
      version: 1,
      input: Input,
      output: z.string()
    })(BulkWorkflow)
    Interval({
      name: `pr4.bulk-${index}`,
      every: '1d',
      input: { value: 'bulk' }
    })(BulkWorkflow)
    workflows.push(BulkWorkflow)
  }
  return workflows
}

test('readiness reports the durable scheduler separately from the generic dispatcher', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'schedule-pr4-health',
        storage: sqlite({ filename: ':memory:', runtime: 'auto' }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } },
        pollInterval: '20ms'
      }),
      WorkflowsModule.forFeature({
        name: 'schedule-pr4-health',
        workflows: [SchedulerHealthWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    const health = app.get(WorkflowsHealth)
    const readiness = await eventually(
      () => health.readiness(),
      (value) => value.checks.scheduler === 'up'
    )
    expect(readiness).toMatchObject({
      status: 'up',
      ready: true,
      checks: { dispatcher: 'up', scheduler: 'up' },
      scheduler: {
        staleAfterMs: expect.any(Number),
        lastSuccessfulAt: expect.any(String)
      }
    })
  } finally {
    await app.close()
  }
})

test('misfire skip still starts a current deadline observed slightly after its instant', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'schedule-pr4-skip-normal',
        storage: sqlite({ filename: ':memory:', runtime: 'auto' }),
        execution: { activities: { enabled: false } },
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'schedule-pr4-skip-normal',
        workflows: [NormalSkipWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    const runtime = app.get(WorkflowsAdmin)
    const started = await eventually(
      () => runtime.listScheduleOccurrences('pr4.skip-normal'),
      (page) => page.occurrences.some((occurrence) => occurrence.state === 'started'),
      3_000
    )
    expect(started.occurrences.some((occurrence) => occurrence.state === 'started')).toBe(true)
    expect(started.occurrences.find((occurrence) => occurrence.state === 'started')?.trigger).toBe(
      'scheduled'
    )
    await runtime.pauseSchedule('pr4.skip-normal')
  } finally {
    await app.close()
  }
})

test('schedule materialization renews its lease around slow asynchronous input validation', async () => {
  const initial = Date.UTC(2026, 0, 1)
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        namespace: 'schedule-pr4-lease-renewal',
        initialTime: initial,
        storage: sqlite({ filename: ':memory:', runtime: 'auto' }),
        lease: { duration: '60ms', refreshInterval: '20ms' },
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'schedule-pr4-lease-renewal',
        workflows: [LeaseRenewalWorkflow]
      })
    ]
  }).compile()
  try {
    await app.init()
    await app.get(WorkflowsTestHarness).advanceTime('1h')
    const occurrences = await app.get(WorkflowsAdmin).listScheduleOccurrences('pr4.lease-renewal')
    expect(occurrences.occurrences).toMatchObject([{ state: 'started' }])
  } finally {
    await app.close()
  }
})

test('scheduler registration remains bounded for a large schedule catalog', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'schedule-pr4-bulk',
        storage: sqlite({ filename: ':memory:', runtime: 'auto' }),
        execution: {
          workflows: { enabled: false },
          activities: { enabled: false }
        },
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'schedule-pr4-bulk',
        workflows: bulkScheduleWorkflows(1_000)
      })
    ]
  }).compile()
  try {
    await app.init()
    const schedules = await app.get(WorkflowsAdmin).listSchedules({ limit: 1_000 })
    expect(schedules).toHaveLength(1_000)
  } finally {
    await app.close()
  }
})

test('schedule metrics keep schedule metadata but reject execution and payload dimensions', async () => {
  const runtime = ManagedRuntime.make(telemetryLayer)
  try {
    const telemetry = await runtime.runPromise(TelemetryService)
    const secret = 'SCHEDULE_INPUT_SECRET'
    telemetry.count('scheduleStarted', {
      [TelemetryAttributeKey.scheduleName]: 'reports.daily',
      [TelemetryAttributeKey.workflowName]: 'reports.generate',
      [TelemetryAttributeKey.workflowVersion]: 1,
      [TelemetryAttributeKey.scheduleType]: 'cron',
      [TelemetryAttributeKey.scheduleTrigger]: 'scheduled',
      [TelemetryAttributeKey.scheduleMisfirePolicy]: 'latest',
      [TelemetryAttributeKey.scheduleOverlapPolicy]: 'skip',
      [TelemetryAttributeKey.executionId]: secret,
      [TelemetryAttributeKey.scheduleScheduledAt]: secret
    })
    const snapshot = telemetry.snapshot()
    expect(snapshot).toHaveLength(1)
    expect(snapshot[0]).toMatchObject({
      id: 'better_workflows.schedule.started',
      attributes: {
        'better_workflows.schedule.name': 'reports.daily',
        'better_workflows.workflow.name': 'reports.generate',
        'better_workflows.workflow.version': '1',
        'better_workflows.schedule.type': 'cron',
        'better_workflows.schedule.trigger': 'scheduled',
        'better_workflows.schedule.misfire_policy': 'latest',
        'better_workflows.schedule.overlap_policy': 'skip'
      }
    })
    expect(JSON.stringify(snapshot)).not.toContain(secret)
    expect(
      metricAttributes({
        [TelemetryAttributeKey.scheduleSkipReason]: 'not-a-library-reason'
      })
    ).toEqual({})
  } finally {
    await runtime.dispose()
  }
})

test('cron timezone calculation respects a missing DST local time', () => {
  const schedule = normalizeCron({
    name: 'dst.spring-forward',
    expression: '30 2 * * *',
    timezone: 'America/New_York'
  })
  const beforeTransition = Date.UTC(2026, 2, 8, 6)
  expect(new Date(nextScheduleAt(schedule, beforeTransition)).toISOString()).toBe(
    '2026-03-08T07:30:00.000Z'
  )
})

test('cron timezone calculation remains strictly future across a repeated DST hour', () => {
  const schedule = normalizeCron({
    name: 'dst.fall-back',
    expression: '30 1 * * *',
    timezone: 'America/New_York'
  })
  expect(new Date(nextScheduleAt(schedule, Date.UTC(2026, 10, 1, 6))).toISOString()).toBe(
    '2026-11-02T06:30:00.000Z'
  )
})

test('cron timezone calculation converts America/Fortaleza local time to UTC', () => {
  const schedule = normalizeCron({
    name: 'fortaleza.morning',
    expression: '0 8 * * *',
    timezone: 'America/Fortaleza'
  })
  expect(new Date(nextScheduleAt(schedule, Date.UTC(2026, 0, 1, 10))).toISOString()).toBe(
    '2026-01-01T11:00:00.000Z'
  )
})

test('cron without a timezone uses deterministic UTC semantics', () => {
  const implicit = normalizeCron({ name: 'utc.implicit', expression: '0 8 * * *' })
  const explicit = normalizeCron({ name: 'utc.explicit', expression: '0 8 * * *', timezone: 'UTC' })
  expect(implicit.timezone).toBe('UTC')
  expect(new Date(nextScheduleAt(implicit, Date.UTC(2026, 0, 1, 10))).toISOString()).toBe(
    '2026-01-02T08:00:00.000Z'
  )
  const workflow = { options: { name: 'same.workflow', version: 1 } }
  expect(scheduleDefinitionHash(workflow, implicit)).toBe(
    scheduleDefinitionHash(workflow, explicit)
  )
})
