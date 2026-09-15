import { expect, test } from 'bun:test'
import { z } from 'zod'
import { defineQueue, Activities, Activity, Workflow, defineSignal } from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { Journal } from '../src/internal/journal'
import { WorkflowsRuntime } from '../src/internal/runtime'
import { decode, encode, validate } from '../src/internal/values'
import { testApp, eventually } from './helpers'
import type { StandardSchemaV1 } from '@standard-schema/spec'

const Input = z.object({ id: z.string() })
const Continue = defineSignal('continue', z.boolean())

@Workflow({ name: 'hold', version: 1, input: Input, output: z.boolean(), signals: [Continue] })
class Hold {
  run(_input: z.infer<typeof Input>, ctx: WorkflowContext) {
    return ctx.waitForSignal('hold', Continue)
  }
}

@Workflow({ name: 'unawaited', version: 1, input: Input, output: z.string(), signals: [Continue] })
class Unawaited {
  async run(_input: z.infer<typeof Input>, ctx: WorkflowContext) {
    void ctx.waitForSignal('forgotten', Continue)
    return 'should not succeed'
  }
}

test('an unawaited durable wait fails rather than leaving a falsely suspended execution', async () => {
  const app = await testApp(Unawaited)
  try {
    const handle = await app.client.start({ id: 'forgotten' })
    await expect(handle.result({ timeout: '3s' })).rejects.toMatchObject({
      code: 'UNAWAITED_COMMAND'
    })
  } finally {
    await app.close()
  }
})

@Workflow({ name: 'duplicate', version: 1, input: Input, output: z.string() })
class Duplicate {
  catches = 0
  async run(_input: z.infer<typeof Input>, ctx: WorkflowContext) {
    await ctx.sleep('same', 1)
    try {
      await ctx.sleep('same', 1)
    } catch {
      this.catches++
    }
    return 'must not succeed'
  }
}

test('duplicate step identifiers are fatal and cannot be swallowed by user catch', async () => {
  const app = await testApp(Duplicate)
  try {
    const handle = await app.client.start({ id: 'duplicate' })
    await expect(handle.result({ timeout: '3s' })).rejects.toMatchObject({
      code: 'NON_DETERMINISTIC_WORKFLOW'
    })
    expect(app.module.get(Duplicate).catches).toBe(0)
  } finally {
    await app.close()
  }
})

@Activities()
class Timed {
  attempts: number[] = []
  aborted = 0
  @Activity({
    name: 'timed',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string(),
    timeout: '50ms',
    retry: { maxAttempts: 2, initialDelay: '10ms' }
  })
  execute(_input: string, ctx: ActivityContext): Promise<string> {
    this.attempts.push(ctx.attempt)
    return new Promise((_resolve, reject) => {
      ctx.signal.addEventListener(
        'abort',
        () => {
          this.aborted++
          reject(new Error('aborted'))
        },
        { once: true }
      )
    })
  }
}

@Workflow({ name: 'timed-workflow', version: 1, input: Input, output: z.string() })
class TimedWorkflow {
  async run(input: z.infer<typeof Input>, ctx: WorkflowContext) {
    return ctx.activities(Timed).execute(input.id, { stepId: 'slow' })
  }
}

test('activity timeout aborts the handler and retry policy counts total attempts', async () => {
  const app = await testApp(TimedWorkflow, { providers: [Timed] })
  try {
    const handle = await app.client.start({ id: 'timeout' })
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'ACTIVITY_TIMEOUT'
    })
    expect(app.module.get(Timed).attempts).toEqual([1, 2])
    expect(app.module.get(Timed).aborted).toBe(2)
  } finally {
    await app.close()
  }
})

test('cancellation aborts an in-flight activity without pretending to undo external effects', async () => {
  const app = await testApp(TimedWorkflow, { providers: [Timed] })
  try {
    const handle = await app.client.start({ id: 'cancel-running' })
    await eventually(
      async () => app.module.get(Timed).attempts.length,
      (count) => count > 0
    )
    await handle.cancel()
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'WORKFLOW_CANCELLED'
    })
    await eventually(
      async () => app.module.get(Timed).aborted,
      (count) => count > 0
    )
  } finally {
    await app.close()
  }
})

@Activities()
class Limited {
  active = 0
  peak = 0
  async work(): Promise<string> {
    this.active++
    this.peak = Math.max(this.peak, this.active)
    try {
      await new Promise((resolve) => setTimeout(resolve, 50))
      return 'done'
    } finally {
      this.active--
    }
  }
  @Activity({
    name: 'limit-one',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string()
  })
  one(_input: string) {
    return this.work()
  }
  @Activity({
    name: 'limit-two',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string()
  })
  two(_input: string) {
    return this.work()
  }
}

@Workflow({ name: 'limited', version: 1, input: Input, output: z.string() })
class LimitedWorkflow {
  run(input: z.infer<typeof Input>, ctx: WorkflowContext) {
    const activities = ctx.activities(Limited)
    return Number(input.id) % 2 === 0
      ? activities.one(input.id, { stepId: 'work' })
      : activities.two(input.id, { stepId: 'work' })
  }
}

test('a logical queue concurrency limit is shared by different activity handlers', async () => {
  const app = await testApp(LimitedWorkflow, { providers: [Limited] })
  try {
    const handles = await Promise.all(
      Array.from({ length: 8 }, (_, id) => app.client.start({ id: String(id) }))
    )
    expect(await Promise.all(handles.map((handle) => handle.result({ timeout: '5s' })))).toEqual(
      Array(8).fill('done')
    )
    expect(app.module.get(Limited).peak).toBe(2)
  } finally {
    await app.close()
  }
})

test('late activity deliveries cannot commit after fencing or lease expiry', async () => {
  const app = await testApp(Hold)
  try {
    const handle = await app.client.start({ id: 'fencing' })
    const runtime = app.module.get(WorkflowsRuntime)
    const sql = await runtime.run((await import('effect/unstable/sql')).SqlClient.SqlClient)
    const journal = new Journal(sql, 'integration')
    const old = await runtime.run(journal.claim(handle.executionId, 'step', 1, 1, 'old', 1000))
    const current = await runtime.run(
      journal.claim(handle.executionId, 'step', 1, 2, 'current', 1000)
    )
    expect(await runtime.run(journal.finishClaim(old, encode('stale'), null))).toBe(false)
    expect(await runtime.run(journal.renewClaim(old, 1000))).toBe(false)
    expect(await runtime.run(journal.finishClaim(current, encode('correct'), null))).toBe(true)
    const cached = await runtime.run(
      journal.claim(handle.executionId, 'step', 1, 3, 'redelivery', 1000)
    )
    expect(cached.state).toBe('completed')
    expect(decode<string>(cached.result_json!)).toBe('correct')
    const expired = await runtime.run(
      journal.claim(handle.executionId, 'expired', 1, 1, 'expired', 10)
    )
    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(await runtime.run(journal.finishClaim(expired, encode('too late'), null))).toBe(false)
    await expect(runtime.run(journal.heartbeat(expired, null))).rejects.toMatchObject({
      code: 'LEASE_LOST'
    })
  } finally {
    await app.close()
  }
})

test('retry wakeup and failure commit atomically in the durable journal', async () => {
  const app = await testApp(Hold)
  try {
    const handle = await app.client.start({ id: 'atomic-retry' })
    const runtime = app.module.get(WorkflowsRuntime)
    const sql = await runtime.run((await import('effect/unstable/sql')).SqlClient.SqlClient)
    const journal = new Journal(sql, 'integration')
    const claim = await runtime.run(
      journal.claim(handle.executionId, 'failure', 1, 1, 'owner', 1000)
    )
    const failure = { code: 'RETRY', message: 'retry', retryable: true }
    expect(await runtime.run(journal.finishClaim(claim, null, failure, 0))).toBe(true)
    const pending = await runtime.run(journal.pendingRetries())
    expect(pending.some((row) => row.step_id === 'failure' && row.attempt === 1)).toBe(true)
  } finally {
    await app.close()
  }
})

test('canonical JSON enforces the persistence contract and does not silently drop values', () => {
  expect(encode({ b: 2, a: [true, null] })).toBe(encode({ a: [true, null], b: 2 }))
  expect(decode(encode(undefined))).toBeUndefined()
  const invalid = [
    NaN,
    Infinity,
    1n,
    new Date(),
    { missing: undefined },
    // oxlint-disable-next-line eslint/no-sparse-arrays -- Deliberate invalid input to the persistence validator.
    [, 1],
    Symbol('x'),
    () => 1
  ]
  for (const value of invalid) expect(() => encode(value)).toThrow()
  interface CyclicValue {
    self?: CyclicValue
  }
  const cyclic: CyclicValue = {}
  cyclic.self = cyclic
  expect(() => encode(cyclic)).toThrow()
  const accessor = Object.defineProperty({}, 'value', {
    get() {
      throw new Error('must not invoke')
    },
    enumerable: true
  })
  expect(() => encode(accessor)).toThrow()
  expect(() => encode('x'.repeat(1024 * 1024))).toThrow()
})

test('Standard Schema accepts an async non-Zod validator', async () => {
  const schema: StandardSchemaV1<string> = {
    '~standard': {
      version: 1,
      vendor: 'test-validator',
      async validate(input) {
        return input === 'allowed' ? { value: 'allowed' } : { issues: [{ message: 'not allowed' }] }
      }
    }
  }
  expect(await validate(schema, 'allowed', 'input')).toBe('allowed')
  await expect(validate(schema, 'blocked', 'input')).rejects.toMatchObject({
    code: 'VALIDATION_FAILED'
  })
})
