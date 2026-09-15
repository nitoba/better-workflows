import 'reflect-metadata'
import { appendFile } from 'node:fs/promises'
import { Database } from 'bun:sqlite'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  defineQueue,
  Activities,
  Activity,
  ActivityError,
  Workflow,
  WorkflowError,
  WorkflowsModule,
  getWorkflowToken,
  defineSignal
} from '../../src'
import type { ActivityContext, WorkflowContext, WorkflowClient } from '../../src'
import { sqlite } from '../../src/sqlite'

const filename = process.env['WORKFLOW_DATABASE']!
const log = process.env['WORKFLOW_LOG']!
const scenario = process.env['WORKFLOW_SCENARIO']!
const recovering = process.env['WORKFLOW_MODE'] === 'recover'
const Approval = defineSignal('approval', z.boolean())

@Activities()
class Markers {
  @Activity({
    name: 'structured-marker',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string(),
    retry: { maxAttempts: 2, initialDelay: '2s', maxDelay: '2s' }
  })
  async write(id: string, ctx: ActivityContext) {
    await appendFile(
      log,
      JSON.stringify({ id, attempt: ctx.attempt, key: ctx.idempotencyKey }) + '\n'
    )
    if (id === 'undo-b' && ctx.attempt === 1)
      throw new ActivityError({
        code: 'TEMPORARY',
        message: 'Retry compensation after crash',
        retryable: true
      })
    return id
  }
}

@Workflow({
  name: 'structured-child',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Approval]
})
class Child {
  async run(id: string, ctx: WorkflowContext) {
    const written = await ctx.activities(Markers).write(id, { stepId: 'write' })
    await ctx.waitForSignal('approve', Approval)
    return written
  }
}

@Workflow({
  name: 'structured-crash',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Approval],
  idempotencyKey: (value) => value
})
class Parent {
  async run(id: string, ctx: WorkflowContext): Promise<string> {
    if (scenario === 'map') {
      const values = await ctx.map(
        'batch',
        ['a', 'b', 'c', 'd'],
        { key: (value) => value, concurrency: 2 },
        async (value, branch) => {
          await branch.activities(Markers).write(value, { stepId: 'write' })
          await branch.waitForSignal('approve', Approval)
          return value
        }
      )
      return values.join(',')
    }
    if (scenario === 'child') return ctx.child('child', Child, id)
    return ctx.saga('checkout', async (saga) => {
      for (const item of ['a', 'b']) {
        await saga.step(
          item,
          (step) => step.activities(Markers).write(`forward-${item}`, { stepId: 'write' }),
          async (_result, undo) => {
            await undo.activities(Markers).write(`undo-${item}`, { stepId: 'write' })
          }
        )
      }
      throw new WorkflowError('ORDER_FAILED', 'The business operation failed')
    })
  }
}

@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'structured-crash-tests',
      storage: sqlite({ filename }),
      queues: [{ queue: defineQueue('work'), concurrency: 4, globalConcurrency: 2 }],
      pollInterval: '20ms',
      lease: { duration: '600ms', refreshInterval: '150ms' }
    }),
    WorkflowsModule.forFeature({
      name: 'structured',
      workflows: [Parent, Child],
      activities: [Markers]
    })
  ]
})
class App {}

try {
  const app = await NestFactory.createApplicationContext(App, {
    logger: false,
    abortOnError: false
  })
  const client = app.get<WorkflowClient<typeof Parent>>(getWorkflowToken(Parent))
  const handle = await client.start('structured-id')
  const db = new Database(filename, { readonly: true })
  if (recovering) {
    if (scenario === 'map') {
      for (const item of ['a', 'b', 'c', 'd'])
        await handle.signal(Approval, true, { idempotencyKey: item })
    }
    if (scenario === 'child') {
      const row = db
        .query<{ child_id: string }, [string]>(
          'SELECT child_id FROM better_workflows_children WHERE parent_id=?'
        )
        .get(handle.executionId)!
      const child = app.get<WorkflowClient<typeof Child>>(getWorkflowToken(Child))
      await child.getHandle(row.child_id).signal(Approval, true, { idempotencyKey: 'approval' })
    }
    let result: string
    try {
      result = await handle.result({ timeout: '10s' })
    } catch (error) {
      if (scenario !== 'saga' || !(error instanceof WorkflowError)) throw error
      result = error.code
    }
    process.send?.({
      type: 'complete',
      executionId: handle.executionId,
      created: handle.created,
      result
    })
    db.close()
    await app.close()
    process.disconnect?.()
  } else {
    while (true) {
      const query =
        scenario === 'saga'
          ? 'SELECT COUNT(*) AS n FROM better_workflows_retries WHERE execution_id=?'
          : scenario === 'child'
            ? 'SELECT COUNT(*) AS n FROM better_workflows_waits WHERE execution_id IN (SELECT child_id FROM better_workflows_children WHERE parent_id=?)'
            : 'SELECT COUNT(*) AS n FROM better_workflows_waits WHERE execution_id=?'
      const ready = db.query<{ n: number }, [string]>(query).get(handle.executionId)!.n
      if (ready >= (scenario === 'map' ? 2 : 1)) break
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    db.close()
    process.send?.({ type: 'checkpoint', executionId: handle.executionId })
  }
} catch (error) {
  console.error(error)
  process.exitCode = 1
  process.disconnect?.()
}
