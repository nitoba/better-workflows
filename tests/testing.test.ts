import { test, expect } from 'bun:test'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Activities,
  Activity,
  ActivityError,
  Workflow,
  WorkflowsModule,
  getWorkflowToken,
  defineSignal
} from '../src'
import type { WorkflowContext, ActivityContext, WorkflowClient } from '../src'
import { WorkflowsTestingModule, WorkflowsTestHarness } from '../src/testing'

const Confirmation = defineSignal('confirmation', z.boolean())
@Activities()
class Retrying {
  attempts: number[] = []
  @Activity({
    name: 'retry-virtual',
    version: 1,
    queue: 'work',
    input: z.string(),
    output: z.string(),
    retry: { maxAttempts: 3, initialDelay: '1d', maxDelay: '10d' }
  })
  async run(value: string, ctx: ActivityContext) {
    this.attempts.push(ctx.attempt)
    if (ctx.attempt < 3)
      throw new ActivityError({ code: 'TEMPORARY', message: 'retry later', retryable: true })
    return value
  }
}
@Workflow({
  name: 'time-travel',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Confirmation]
})
class Travel {
  async run(value: string, ctx: WorkflowContext) {
    const result = await ctx.activities(Retrying).run(value, { stepId: 'retry' })
    await ctx.sleep('week', '7d')
    try {
      await ctx.waitForSignal('confirmation', Confirmation, { timeout: '2d' })
    } catch {
      return `${result}-expired`
    }
    return result
  }
}

test('virtual clock visits persisted retries, timers and signal timeouts chronologically without waiting days', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsTestingModule.forRoot({
        initialTime: Date.UTC(2026, 0, 1),
        queues: { work: { concurrency: 2 } }
      }),
      WorkflowsModule.forFeature([Travel])
    ],
    providers: [Travel, Retrying]
  }).compile()
  try {
    await app.init()
    const harness = app.get(WorkflowsTestHarness)
    const client = app.get<WorkflowClient<typeof Travel>>(getWorkflowToken(Travel))
    const handle = await client.start('done')
    await harness.advanceTime('12d')
    expect(await handle.result({ timeout: '1s' })).toBe('done-expired')
    expect(app.get(Retrying).attempts).toEqual([1, 2, 3])
    expect(harness.clock.now()).toBe(Date.UTC(2026, 0, 13))
  } finally {
    await app.close()
  }
}, 15000)
