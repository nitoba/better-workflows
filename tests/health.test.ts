import { expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Activities,
  ActivitiesContract,
  Activity,
  WorkflowsHealth,
  WorkflowsModule,
  defineQueue
} from '../src'
import type { ActivityContext } from '../src'
import { postgres } from '../src/postgres'
import { sqlite } from '../src/sqlite'
import { WorkflowsRuntime } from '../src/internal/runtime'
import { eventually } from './helpers'

test('WorkflowsHealth exposes liveness and readiness without an HTTP controller', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-health-'))
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'health',
        storage: sqlite({ filename: join(directory, 'workflows.sqlite') }),
        pollInterval: '20ms',
        lease: { duration: '1500ms', refreshInterval: '400ms' }
      })
    ]
  }).compile()
  const health = app.get(WorkflowsHealth)

  try {
    expect(health.liveness()).toEqual({
      status: 'down',
      runtime: { running: false, stopping: false }
    })

    await app.init()

    expect(health.liveness()).toEqual({
      status: 'up',
      runtime: { running: true, stopping: false }
    })
    await expect(health.readiness()).resolves.toMatchObject({
      status: 'up',
      ready: true,
      checks: {
        runtime: 'up',
        storage: 'up',
        schema: 'up',
        dispatcher: 'up',
        notifier: 'up',
        workflows: 'disabled',
        workers: 'disabled'
      }
    })
  } finally {
    await app.close()
    expect(health.liveness()).toEqual({
      status: 'down',
      runtime: { running: false, stopping: true }
    })
    await rm(directory, { recursive: true, force: true })
  }
})

test('readiness distinguishes storage failure and dispatcher recovery', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-health-failure-'))
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'health-failure',
        storage: sqlite({ filename: join(directory, 'workflows.sqlite') }),
        pollInterval: '20ms',
        lease: { duration: '1500ms', refreshInterval: '400ms' }
      })
    ]
  }).compile()
  const health = app.get(WorkflowsHealth)
  const runtime = app.get(WorkflowsRuntime)

  try {
    await app.init()
    runtime.testingFailNextDispatcherIteration('transient dispatcher failure')

    await eventually(
      () => health.readiness(),
      (readiness) => readiness.checks.dispatcher === 'degraded'
    )
    await eventually(
      () => health.readiness(),
      (readiness) => readiness.checks.dispatcher === 'up'
    )

    await runtime.testingDisposeInfrastructure()
    await expect(health.readiness()).resolves.toMatchObject({
      status: 'down',
      ready: false,
      checks: { storage: 'down', schema: 'down' }
    })
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
})

test('readiness reports an advanced activity worker only when an implementation is registered', async () => {
  const queue = defineQueue('health-advanced-activity')
  @ActivitiesContract({ queue })
  abstract class HealthActivities {
    @Activity({ name: 'health.advanced', version: 1, input: z.string(), output: z.string() })
    execute(_input: string, _context: ActivityContext): Promise<string> {
      throw new Error('contract-only')
    }
  }
  @Activities(HealthActivities)
  class HealthActivitiesHandler implements HealthActivities {
    async execute(input: string, _context: ActivityContext): Promise<string> {
      return input
    }
  }
  const workerApp = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'health-advanced-worker',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'health-advanced-worker',
        activities: [HealthActivitiesHandler],
        queues: [{ queue }]
      })
    ]
  }).compile()
  try {
    const health = workerApp.get(WorkflowsHealth)
    await workerApp.init()
    await expect(health.readiness()).resolves.toMatchObject({
      status: 'up',
      ready: true,
      checks: { workers: 'up' }
    })
  } finally {
    await workerApp.close()
  }

  const contractApp = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'health-advanced-contract-only',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'health-advanced-contract-only',
        activityContracts: [HealthActivities],
        queues: [{ queue }]
      })
    ]
  }).compile()
  try {
    const health = contractApp.get(WorkflowsHealth)
    await contractApp.init()
    await expect(health.readiness()).resolves.toMatchObject({
      status: 'up',
      ready: true,
      checks: { workers: 'disabled' }
    })
  } finally {
    await contractApp.close()
  }
})

const postgresConnectionString = process.env['WORKFLOWS_TEST_POSTGRES_URL']

test.skipIf(!postgresConnectionString)(
  'a disconnected PostgreSQL notifier is degraded and reconnect recovery restores readiness',
  async () => {
    if (!postgresConnectionString) return
    const app = await Test.createTestingModule({
      imports: [
        WorkflowsModule.forRoot({
          namespace: `health-postgres-${process.pid}`,
          storage: postgres({ connectionString: postgresConnectionString }),
          topology: 'distributed',
          execution: { workflows: { enabled: false }, activities: { enabled: false } },
          pollInterval: '20ms',
          lease: { duration: '1500ms', refreshInterval: '400ms' }
        })
      ]
    }).compile()
    const health = app.get(WorkflowsHealth)
    const runtime = app.get(WorkflowsRuntime)
    try {
      await app.init()
      await eventually(
        () => health.readiness(),
        (readiness) => readiness.checks.notifier === 'up'
      )
      runtime.testingSetNotifierConnected(false)
      await expect(health.readiness()).resolves.toMatchObject({
        status: 'degraded',
        ready: true,
        checks: { notifier: 'degraded' }
      })
      runtime.testingSetNotifierConnected(true)
      await expect(health.readiness()).resolves.toMatchObject({
        status: 'up',
        ready: true,
        checks: { notifier: 'up' }
      })
    } finally {
      await app.close()
    }
  }
)
