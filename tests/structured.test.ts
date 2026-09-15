import { expect, test } from 'bun:test'
import { z } from 'zod'
import { defineQueue, Activities, Activity, ActivityError, Workflow, defineSignal } from '../src'
import type { WorkflowContext } from '../src'
import { WorkflowsRuntime } from '../src/internal/runtime'
import { WorkflowClient } from '../src/client'
import { testApp, eventually } from './helpers'

const Approved = defineSignal('approved', z.string())
@Activities()
class Work {
  active = 0
  max = 0
  seen: string[] = []
  @Activity({
    name: 'structured.work',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string()
  })
  async work(value: string) {
    this.active++
    this.max = Math.max(this.max, this.active)
    this.seen.push(value)
    await new Promise((resolve) => setTimeout(resolve, 200))
    this.active--
    if (value === 'fail')
      throw new ActivityError({ code: 'ORDER_FAILED', message: 'order rejected', retryable: false })
    return value
  }
}

@Workflow({
  name: 'structured.map',
  version: 1,
  input: z.string(),
  output: z.array(z.string()),
  signals: [Approved]
})
class Batch {
  async run(_input: string, ctx: WorkflowContext) {
    return ctx.map(
      'batch',
      ['a', 'b', 'c', 'd'],
      { key: (item) => item, concurrency: 2 },
      async (item, branch) => {
        await branch.activities(Work).work(item, { stepId: 'work' })
        const approved = await branch.waitForSignal('approval', Approved)
        return `${item}:${approved}`
      }
    )
  }
}

test('durable map keeps two waiting branches admitted, settles in input order and never repeats completed work', async () => {
  const app = await testApp(Batch, {
    providers: [Work],
    queues: [{ queue: defineQueue('work'), concurrency: 8 }]
  })
  try {
    const handle = await app.client.start('batch')
    await eventually(
      async () => app.module.get(Work).seen.length,
      (n) => n === 2
    )
    await new Promise((resolve) => setTimeout(resolve, 120))
    expect(app.module.get(Work).seen.length).toBe(2)
    for (const key of ['1', '2', '3', '4'])
      await handle.signal(Approved, key, { idempotencyKey: key })
    const result = await handle.result({ timeout: '8s' })
    expect(result.map((value) => value.split(':')[0])).toEqual(['a', 'b', 'c', 'd'])
    expect(result.map((value) => value.split(':')[1]!).sort()).toEqual(['1', '2', '3', '4'])
    expect(app.module.get(Work).seen.sort()).toEqual(['a', 'b', 'c', 'd'])
    expect(app.module.get(Work).max).toBe(2)
  } finally {
    await app.close()
  }
}, 12000)

@Workflow({ name: 'structured.child', version: 1, input: z.string(), output: z.string() })
class Child {
  async run(input: string, ctx: WorkflowContext) {
    return ctx.activities(Work).work(input, { stepId: 'work' })
  }
}
@Workflow({
  name: 'structured.parent',
  version: 1,
  input: z.string(),
  output: z.object({ first: z.string(), second: z.string() })
})
class Parent {
  async run(input: string, ctx: WorkflowContext) {
    return ctx.parallel('children', {
      first: (branch) => branch.child('one', Child, input),
      second: (branch) => branch.child('two', Child, `${input}2`)
    })
  }
}

test('parallel child workflows have distinct durable identities and typed ordered results', async () => {
  const app = await testApp(Parent, { providers: [Child, Work] })
  try {
    const handle = await app.client.start('value')
    expect(await handle.result({ timeout: '8s' })).toEqual({ first: 'value', second: 'value2' })
    const events = (await handle.history({ limit: 200 })).events
    expect(events.filter((event) => event.type === 'child.started').length).toBe(2)
    expect(app.module.get(Work).seen.sort()).toEqual(['value', 'value2'])
  } finally {
    await app.close()
  }
}, 12000)

@Workflow({ name: 'structured.saga', version: 1, input: z.string(), output: z.string() })
class Checkout {
  async run(_input: string, ctx: WorkflowContext) {
    return ctx.saga('checkout', async (saga) => {
      await saga.step(
        'pay',
        (step) => step.activities(Work).work('pay', { stepId: 'pay' }),
        (value, step) =>
          step
            .activities(Work)
            .work(`refund-${value}`, { stepId: 'refund' })
            .then(() => {})
      )
      await saga.step(
        'reserve',
        (step) => step.activities(Work).work('reserve', { stepId: 'reserve' }),
        (value, step) =>
          step
            .activities(Work)
            .work(`release-${value}`, { stepId: 'release' })
            .then(() => {})
      )
      return saga.step(
        'finalize',
        (step) => step.activities(Work).work('fail', { stepId: 'finalize' }),
        async () => {}
      )
    })
  }
}

test('saga compensates successful steps durably in reverse order and preserves the original error', async () => {
  const app = await testApp(Checkout, { providers: [Work] })
  try {
    const handle = await app.client.start('order')
    await expect(handle.result({ timeout: '8s' })).rejects.toMatchObject({
      failure: { code: 'ORDER_FAILED' }
    })
    expect(app.module.get(Work).seen).toEqual([
      'pay',
      'reserve',
      'fail',
      'release-reserve',
      'refund-pay'
    ])
    const events = (await handle.history({ limit: 200 })).events
    expect(events.filter((event) => event.type === 'compensation.completed').length).toBe(2)
  } finally {
    await app.close()
  }
}, 12000)

@Workflow({
  name: 'structured.waiting-child',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Approved]
})
class WaitingChild {
  async run(value: string, ctx: WorkflowContext) {
    await ctx.waitForSignal('hold', Approved)
    return value
  }
}
@Workflow({
  name: 'structured.detached-parent',
  version: 1,
  input: z.enum(['abandon', 'request-cancel']),
  output: z.object({ executionId: z.string() })
})
class DetachedParent {
  async run(policy: 'abandon' | 'request-cancel', ctx: WorkflowContext) {
    return ctx.startChild('child', WaitingChild, 'child-result', { parentClosePolicy: policy })
  }
}

test('child parent-close policies distinguish cancellation from explicitly abandoned independent children', async () => {
  const app = await testApp(DetachedParent, { providers: [WaitingChild] })
  try {
    const runtime = app.module.get(WorkflowsRuntime)
    const childClient = new WorkflowClient(runtime, WaitingChild)
    const cancelled = await (await app.client.start('request-cancel')).result({ timeout: '3s' })
    await expect(
      childClient.getHandle(cancelled.executionId).result({ timeout: '3s' })
    ).rejects.toMatchObject({ code: 'WORKFLOW_CANCELLED' })
    const independent = await (await app.client.start('abandon')).result({ timeout: '3s' })
    const child = childClient.getHandle(independent.executionId)
    expect((await child.describe()).status).not.toBe('cancelled')
    await child.signal(Approved, 'yes', { idempotencyKey: 'yes' })
    expect(await child.result({ timeout: '3s' })).toBe('child-result')
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'structured.compensation-failed',
  version: 1,
  input: z.string(),
  output: z.string()
})
class FailedUndo {
  async run(_input: string, ctx: WorkflowContext) {
    return ctx.saga('saga', async (saga) => {
      await saga.step(
        'first',
        (step) => step.activities(Work).work('first', { stepId: 'do' }),
        (_value, step) =>
          step
            .activities(Work)
            .work('undo-first', { stepId: 'undo' })
            .then(() => {})
      )
      await saga.step(
        'second',
        (step) => step.activities(Work).work('second', { stepId: 'do' }),
        (_value, step) =>
          step
            .activities(Work)
            .work('fail', { stepId: 'undo' })
            .then(() => {})
      )
      throw new ActivityError({
        code: 'ORDER_REJECTED',
        message: 'cannot finish',
        retryable: false
      })
    })
  }
}

test('a failed compensation does not skip the remaining reverse-order compensations', async () => {
  const app = await testApp(FailedUndo, { providers: [Work] })
  try {
    const handle = await app.client.start('fail')
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'COMPENSATION_FAILED'
    })
    expect(app.module.get(Work).seen).toEqual(['first', 'second', 'fail', 'undo-first'])
    expect(
      (await handle.history({ limit: 200 })).events.filter(
        (event) => event.type === 'compensation.completed'
      ).length
    ).toBe(1)
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'structured.empty',
  version: 1,
  input: z.string(),
  output: z.object({ mapped: z.array(z.string()), parallel: z.object({}) })
})
class EmptyGroups {
  async run(_input: string, ctx: WorkflowContext) {
    return {
      mapped: await ctx.map('empty', [], { key: () => '', concurrency: 2 }, async () => ''),
      parallel: await ctx.parallel('empty-parallel', {})
    }
  }
}

test('empty durable groups complete without deadlocking or inventing branches', async () => {
  const app = await testApp(EmptyGroups)
  try {
    expect(await (await app.client.start('empty')).result({ timeout: '3s' })).toEqual({
      mapped: [],
      parallel: {}
    })
  } finally {
    await app.close()
  }
})

test('saga-scoped activities work outside compensated steps; captured outer contexts fail instead of deadlocking', async () => {
  @Activities()
  class Echo {
    @Activity({
      name: 'saga-echo',
      version: 1,
      queue: defineQueue('work'),
      input: z.string(),
      output: z.string()
    })
    async echo(value: string) {
      return value
    }
  }
  @Workflow({ name: 'saga-context', version: 1, input: z.boolean(), output: z.string() })
  class ContextWorkflow {
    async run(wrong: boolean, ctx: WorkflowContext): Promise<string> {
      return ctx.saga('scope', async (saga) => {
        const value = await saga.step(
          'forward',
          (step) => step.activities(Echo).echo('one', { stepId: 'echo' }),
          async (_value, undo) => {
            await undo.activities(Echo).echo('undo', { stepId: 'echo' })
          }
        )
        return (wrong ? ctx : saga).activities(Echo).echo(value, { stepId: 'final' })
      })
    }
  }
  const app = await testApp(ContextWorkflow, { providers: [Echo] })
  try {
    const good = await app.client.start(false)
    expect(await good.result({ timeout: '3s' })).toBe('one')
    const bad = await app.client.start(true)
    await expect(bad.result({ timeout: '3s' })).rejects.toMatchObject({
      code: 'WRONG_WORKFLOW_CONTEXT'
    })
  } finally {
    await app.close()
  }
})
