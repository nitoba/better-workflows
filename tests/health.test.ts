import { expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Test } from '@nestjs/testing'
import { WorkflowsHealth, WorkflowsModule } from '../src'
import { sqlite } from '../src/sqlite'

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
