import 'reflect-metadata'
import { appendFile } from 'node:fs/promises'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  defineQueue,
  Activity,
  Activities,
  ActivityError,
  Workflow,
  WorkflowsModule,
  getWorkflowToken,
  defineSignal
} from '../../src'
import type { ActivityContext, WorkflowClient, WorkflowContext } from '../../src'
import { sqlite } from '../../src/sqlite'

const filename = process.env['WORKFLOW_DATABASE']!
const log = process.env['WORKFLOW_LOG']!
const scenario = process.env['WORKFLOW_SCENARIO'] ?? 'signal'
const mode = process.env['WORKFLOW_MODE'] ?? 'wait'
const Approval = defineSignal('approval', z.boolean())

@Activities()
class Files {
  @Activity({
    name: 'write-marker',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string(),
    retry: { maxAttempts: 3, initialDelay: '2s', maxDelay: '2s' }
  })
  async write(id: string, ctx: ActivityContext): Promise<string> {
    await appendFile(log, JSON.stringify({ attempt: ctx.attempt, key: ctx.idempotencyKey }) + '\n')
    if (scenario === 'retry' && ctx.attempt === 1)
      throw new ActivityError({
        code: 'TEMPORARY',
        message: 'Retry after restart',
        retryable: true
      })
    return id
  }
}

@Workflow({
  name: 'crash',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Approval],
  idempotencyKey: (input) => input
})
class CrashWorkflow {
  async run(input: string, ctx: WorkflowContext): Promise<string> {
    if (scenario === 'continue' && input === 'continue-start')
      return ctx.continueAsNew('continue-final')
    const result = await ctx.activities(Files).write(input, { stepId: 'write' })
    if (scenario === 'signal') await ctx.waitForSignal('approval', Approval)
    if (scenario === 'timer') await ctx.sleep('timer', '2s')
    return result
  }
}

@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'crash-tests',
      storage: sqlite({ filename }),
      queues: [{ queue: defineQueue('work'), concurrency: 1 }],
      pollInterval: scenario === 'continue' ? '5s' : '20ms',
      lease: { duration: '600ms', refreshInterval: '150ms' }
    }),
    WorkflowsModule.forFeature({ name: 'crash', workflows: [CrashWorkflow], activities: [Files] })
  ]
})
class AppModule {}

try {
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false })
  const client = app.get<WorkflowClient<typeof CrashWorkflow>>(getWorkflowToken(CrashWorkflow))
  const initialInput = scenario === 'continue' ? 'continue-start' : 'crash-id'
  const handle = await client.start(initialInput)
  if (mode === 'recover') {
    if (scenario === 'signal')
      await handle.signal(Approval, true, { idempotencyKey: 'approval-event' })
    const result = await handle.result({ timeout: '10s' })
    process.send?.({
      type: 'complete',
      result,
      created: handle.created,
      executionId: handle.executionId
    })
    await app.close()
    process.disconnect?.()
  } else {
    while (true) {
      const snapshot = await handle.describe()
      const history = await handle.history()
      const ready =
        scenario === 'continue'
          ? snapshot.status === 'continued'
          : scenario === 'signal'
            ? snapshot.waitingOn?.stepId === 'approval'
            : scenario === 'timer'
              ? snapshot.waitingOn?.stepId === 'timer'
              : history.events.some((event) => event.type === 'activity.failed')
      if (ready) break
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    process.send?.({ type: 'checkpoint', executionId: handle.executionId })
  }
} catch (error) {
  console.error(error)
  process.exitCode = 1
  process.disconnect?.()
}
