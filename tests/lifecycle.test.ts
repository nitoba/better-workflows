import { expect, test } from 'bun:test'
import { z } from 'zod'
import { Activity, Activities, ActivityError, Workflow, defineSignal } from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { testApp, eventually } from './helpers'

const Input = z.object({ id: z.string() })
const Approval = defineSignal('approval', z.boolean())

@Activities()
class RetryActivities {
  attempts: number[] = []
  keys: string[] = []

  @Activity({
    name: 'retry',
    version: 1,
    queue: 'work',
    input: z.string(),
    output: z.number(),
    retry: { maxAttempts: 3, initialDelay: '30ms', maxDelay: '60ms' }
  })
  async execute(_input: string, context: ActivityContext): Promise<number> {
    this.attempts.push(context.attempt)
    this.keys.push(context.idempotencyKey)
    if (context.attempt < 3)
      throw new ActivityError({ code: 'TEMPORARY', message: 'Try again', retryable: true })
    return 42
  }
}

@Workflow({
  name: 'retry-workflow',
  version: 1,
  input: Input,
  output: z.number(),
  idempotencyKey: (input) => input.id
})
class RetryWorkflow {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<number> {
    return context.activities(RetryActivities).execute(input.id, { stepId: 'try-operation' })
  }
}

test('business retries persist distinct attempts and reuse one external idempotency key', async () => {
  const app = await testApp(RetryWorkflow, { providers: [RetryActivities] })
  try {
    const handle = await app.client.start({ id: 'retry' })
    expect(await handle.result({ timeout: '5s' })).toBe(42)
    const activity = app.module.get(RetryActivities)
    expect(activity.attempts).toEqual([1, 2, 3])
    expect(new Set(activity.keys).size).toBe(1)
    expect(
      (await handle.history()).events.filter((event) => event.type === 'activity.failed')
    ).toHaveLength(2)
  } finally {
    await app.close()
  }
})

@Activities()
class BusinessActivities {
  calls = 0
  @Activity({
    name: 'business-error',
    version: 1,
    queue: 'work',
    input: z.string(),
    output: z.number(),
    retry: { maxAttempts: 5 }
  })
  async execute(_input: string): Promise<number> {
    this.calls++
    throw new ActivityError({
      code: 'INVALID_ORDER',
      message: 'Order is invalid',
      retryable: false
    })
  }
}

@Workflow({ name: 'catch-business', version: 1, input: Input, output: z.number() })
class BusinessWorkflow {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<number> {
    try {
      return await context.activities(BusinessActivities).execute(input.id, { stepId: 'business' })
    } catch (error) {
      if (error instanceof ActivityError && error.code === 'INVALID_ORDER') return -1
      throw error
    }
  }
}

test('a nonretryable business failure reaches user catch once and can be recovered', async () => {
  const app = await testApp(BusinessWorkflow, { providers: [BusinessActivities] })
  try {
    const handle = await app.client.start({ id: 'business' })
    expect(await handle.result({ timeout: '5s' })).toBe(-1)
    expect(app.module.get(BusinessActivities).calls).toBe(1)
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'approval-workflow',
  version: 1,
  input: Input,
  output: z.boolean(),
  signals: [Approval]
})
class ApprovalWorkflow {
  catches = 0
  async run(_input: z.infer<typeof Input>, context: WorkflowContext): Promise<boolean> {
    try {
      return await context.waitForSignal('approval', Approval, { timeout: '10s' })
    } catch (error) {
      this.catches++
      throw error
    }
  }
}

test('local result timeout and abort do not cancel a durable execution', async () => {
  const app = await testApp(ApprovalWorkflow)
  try {
    const handle = await app.client.start({ id: 'wait' })
    await expect(handle.result({ timeout: '40ms' })).rejects.toMatchObject({ code: 'WAIT_TIMEOUT' })
    await expect(handle.result({ signal: AbortSignal.abort() })).rejects.toMatchObject({
      code: 'WAIT_ABORTED'
    })
    expect((await handle.describe()).status).toBe('waiting')
    expect(app.module.get(ApprovalWorkflow).catches).toBe(0)
    await handle.signal(Approval, true, { idempotencyKey: 'event' })
    expect(await handle.result({ timeout: '5s' })).toBe(true)
  } finally {
    await app.close()
  }
})

test('pause preserves a delivered signal; resume consumes the same durable outcome', async () => {
  const app = await testApp(ApprovalWorkflow)
  try {
    const handle = await app.client.start({ id: 'pause' })
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await handle.pause()
    await handle.signal(Approval, true, { idempotencyKey: 'paused-approval' })
    await expect(handle.result({ timeout: '100ms' })).rejects.toMatchObject({
      code: 'WAIT_TIMEOUT'
    })
    expect((await handle.describe()).status).toBe('paused')
    await handle.resume()
    expect(await handle.result({ timeout: '5s' })).toBe(true)
    expect(
      (await handle.history()).events.filter((event) => event.type === 'signal.consumed')
    ).toHaveLength(1)
  } finally {
    await app.close()
  }
})

test('cancel is durable, does not enter user catch and cannot be resumed', async () => {
  const app = await testApp(ApprovalWorkflow)
  try {
    const handle = await app.client.start({ id: 'cancel' })
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await handle.cancel({ reason: 'No longer needed' })
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'WORKFLOW_CANCELLED'
    })
    expect(app.module.get(ApprovalWorkflow).catches).toBe(0)
    await expect(handle.resume()).rejects.toMatchObject({ code: 'TERMINAL_EXECUTION' })
    await expect(handle.signal(Approval, true, { idempotencyKey: 'late' })).rejects.toMatchObject({
      code: 'TERMINAL_EXECUTION'
    })
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'twice',
  version: 1,
  input: Input,
  output: z.array(z.boolean()),
  signals: [Approval]
})
class TwiceWorkflow {
  async run(_input: z.infer<typeof Input>, context: WorkflowContext): Promise<boolean[]> {
    const first = await context.waitForSignal('first', Approval)
    const second = await context.waitForSignal('second', Approval)
    return [first, second]
  }
}

test('signals are FIFO, deduplicated and checked against the registered contract', async () => {
  const app = await testApp(TwiceWorkflow)
  try {
    const handle = await app.client.start({ id: 'fifo' })
    expect(await handle.signal(Approval, false, { idempotencyKey: 'first' })).toEqual({
      accepted: true
    })
    expect(await handle.signal(Approval, false, { idempotencyKey: 'first' })).toEqual({
      accepted: false
    })
    await expect(handle.signal(Approval, true, { idempotencyKey: 'first' })).rejects.toMatchObject({
      code: 'IDEMPOTENCY_CONFLICT'
    })
    const forged = defineSignal('approval', z.string())
    await expect(
      handle.signal(forged, 'not-a-boolean', { idempotencyKey: 'forged' })
    ).rejects.toMatchObject({ code: 'VALIDATION_FAILED' })
    await handle.signal(Approval, true, { idempotencyKey: 'second' })
    expect(await handle.result({ timeout: '5s' })).toEqual([false, true])
    expect(await handle.signal(Approval, false, { idempotencyKey: 'first' })).toEqual({
      accepted: false
    })
    const page = await handle.history({ limit: 2 })
    expect(page.events).toHaveLength(2)
    expect(page.nextCursor).toBe(2)
    expect((await handle.history({ after: page.nextCursor!, limit: 2 })).events[0]!.sequence).toBe(
      3
    )
  } finally {
    await app.close()
  }
})

@Workflow({ name: 'timeout', version: 1, input: Input, output: z.string(), signals: [Approval] })
class TimeoutWorkflow {
  async run(_input: z.infer<typeof Input>, context: WorkflowContext): Promise<string> {
    try {
      await context.waitForSignal('first', Approval, { timeout: '30ms' })
      return 'approved'
    } catch (error) {
      if (error instanceof ActivityError && error.code === 'SIGNAL_TIMEOUT') return 'expired'
      throw error
    }
  }
}

test('signal timeouts are persisted outcomes and are catchable business failures', async () => {
  const app = await testApp(TimeoutWorkflow)
  try {
    const handle = await app.client.start({ id: 'timeout' })
    expect(await handle.result({ timeout: '5s' })).toBe('expired')
    expect(
      (await handle.history()).events.filter((event) => event.type === 'signal.timed-out')
    ).toHaveLength(1)
  } finally {
    await app.close()
  }
})
