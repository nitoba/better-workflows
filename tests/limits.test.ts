import { test, expect } from 'bun:test'
import { z } from 'zod'
import { ManagedRuntime } from 'effect'
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import { SqlClient } from 'effect/unstable/sql'
import { Activities, Activity, Workflow } from '../src'
import type { WorkflowContext, ActivityContext } from '../src'
import { Journal } from '../src/internal/journal'
import { Permits } from '../src/internal/permits'
import { testApp } from './helpers'

@Activities()
class Keyed {
  active = 0
  peak = 0
  readonly activeByKey = new Map<string, number>()
  readonly peaksByKey = new Map<string, number>()
  readonly seen: string[] = []
  @Activity({
    name: 'limits.keyed',
    version: 1,
    queue: 'work',
    input: z.object({ key: z.string(), id: z.string() }),
    output: z.string(),
    key: (input) => input.key
  })
  async run(input: { key: string; id: string }, _ctx: ActivityContext) {
    this.active++
    this.peak = Math.max(this.peak, this.active)
    const keyed = (this.activeByKey.get(input.key) ?? 0) + 1
    this.activeByKey.set(input.key, keyed)
    this.peaksByKey.set(input.key, Math.max(this.peaksByKey.get(input.key) ?? 0, keyed))
    this.seen.push(input.id)
    await new Promise((resolve) => setTimeout(resolve, 100))
    this.active--
    this.activeByKey.set(input.key, keyed - 1)
    return input.id
  }
}
@Workflow({ name: 'limits.batch', version: 1, input: z.string(), output: z.array(z.string()) })
class Batches {
  async run(_input: string, ctx: WorkflowContext) {
    return ctx.map(
      'many',
      Array.from({ length: 8 }, (_, i) => i),
      { key: (i) => String(i), concurrency: 8 },
      (i, branch) =>
        branch.activities(Keyed).run({ id: String(i), key: String(i % 3) }, { stepId: 'work' })
    )
  }
}

test('queue global and per-key permits enforce shared limits independently of local and branch slots', async () => {
  const app = await testApp(Batches, {
    providers: [Keyed],
    queues: { work: { concurrency: 8, globalConcurrency: 2, perKeyConcurrency: 1 } }
  })
  try {
    const handle = await app.client.start('all')
    expect(await handle.result({ timeout: '8s' })).toEqual(['0', '1', '2', '3', '4', '5', '6', '7'])
    const handler = app.module.get(Keyed)
    expect(handler.peak).toBe(2)
    expect([...handler.peaksByKey.values()]).toEqual([1, 1, 1])
    expect(handler.seen.length).toBe(8)
  } finally {
    await app.close()
  }
}, 12000)

test('SQL admission is atomic, releases on completion, rejects drift and fences late owners', async () => {
  const runtime = ManagedRuntime.make(SqliteClient.layer({ filename: ':memory:' }))
  try {
    const sql = await runtime.runPromise(SqlClient.SqlClient)
    const journal = new Journal(sql, 'limits')
    const permits = new Permits(journal)
    await runtime.runPromise(journal.migrate())
    await runtime.runPromise(
      permits.register('q', { concurrency: 8, globalConcurrency: 2, perKeyConcurrency: 1 })
    )
    const envelope = (id: string, key: string) => ({
      executionId: id,
      stepId: 'step',
      attempt: 1,
      name: 'job',
      version: 1,
      input: '["json",1]',
      timeoutMs: 1000,
      maxAttempts: 1,
      retryDelayMs: 0,
      concurrencyKey: key
    })
    for (const id of ['a', 'b', 'c'])
      await runtime.runPromise(journal.accept(id, 'wf', 1, id, '["json",1]'))
    const first = await runtime.runPromise(
      permits.claim('q', envelope('a', 'same'), 1, 'owner-a', 1000)
    )
    if (typeof first === 'string') throw new Error(first)
    expect(
      await runtime.runPromise(permits.claim('q', envelope('b', 'same'), 1, 'owner-b', 1000))
    ).toBe('blocked')
    const other = await runtime.runPromise(
      permits.claim('q', envelope('c', 'different'), 1, 'owner-c', 1000)
    )
    expect(typeof other).toBe('object')
    const replacement = await runtime.runPromise(
      permits.claim('q', envelope('a', 'same'), 2, 'replacement', 1000)
    )
    if (typeof replacement === 'string') throw new Error(replacement)
    expect(await runtime.runPromise(permits.finish(first, '["json",1]', null))).toBe(false)
    expect(await runtime.runPromise(permits.finish(replacement, '["json",1]', null))).toBe(true)
    expect(
      typeof (await runtime.runPromise(
        permits.claim('q', envelope('b', 'same'), 1, 'owner-b', 1000)
      ))
    ).toBe('object')
    const drift = await runtime.runPromiseExit(
      permits.register('q', { concurrency: 1, globalConcurrency: 3, perKeyConcurrency: 1 })
    )
    expect(drift._tag).toBe('Failure')
  } finally {
    await runtime.dispose()
  }
})
