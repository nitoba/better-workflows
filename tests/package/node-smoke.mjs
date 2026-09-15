import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:net'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  defineQueue,
  Activities,
  Activity,
  ActivityError,
  Workflow,
  WorkflowsModule,
  getWorkflowToken,
  defineSignal
} from 'better-workflows'
import { sqlite } from 'better-workflows/sqlite'
import { postgres } from 'better-workflows/postgres'

const directory = await mkdtemp(join(tmpdir(), 'better-workflows-node-'))
const signal = defineSignal('approve', z.boolean())
const input = z.object({ id: z.string(), value: z.number() })
let calls = 0
class Maths {
  async double(value, context) {
    calls++
    await context.heartbeat({ attempt: context.attempt })
    if (context.attempt === 1)
      throw new ActivityError({ code: 'TEMPORARY', message: 'retry', retryable: true })
    return value * 2
  }
}
Activities()(Maths)
Activity({
  name: 'double',
  version: 1,
  queue: defineQueue('maths'),
  input: z.number(),
  output: z.number(),
  retry: { maxAttempts: 2, initialDelay: '20ms' }
})(Maths.prototype, 'double', Object.getOwnPropertyDescriptor(Maths.prototype, 'double'))
class Calculation {
  async run(value, ctx) {
    const doubled = await ctx.activities(Maths).double(value.value, { stepId: 'double' })
    await ctx.sleep('timer', '20ms')
    return (await ctx.waitForSignal('approval', signal, { timeout: '15s' })) ? doubled : 0
  }
}
Workflow({
  name: 'calculation',
  version: 1,
  input,
  output: z.number(),
  signals: [signal],
  idempotencyKey: (value) => value.id
})(Calculation)

const namespace = `package-${randomUUID()}`
const url = process.env.WORKFLOWS_TEST_POSTGRES_URL
const storage = url
  ? postgres({ connectionString: url })
  : sqlite({ filename: join(directory, 'workflows.sqlite'), runtime: 'node' })
const base = {
  namespace,
  storage,
  queues: [{ queue: defineQueue('maths'), concurrency: 2 }],
  pollInterval: '50ms',
  lease: { duration: '3s', refreshInterval: '800ms' }
}
async function app(options, providers) {
  class App {}
  Module({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({
        name: 'calculation',
        clients: [Calculation],
        workflows: providers.includes(Calculation) ? [Calculation] : [],
        activities: providers.includes(Maths) ? [Maths] : [],
        activityContracts:
          providers.includes(Calculation) && !providers.includes(Maths) ? [Maths] : []
      })
    ]
  })(App)
  return NestFactory.createApplicationContext(App, { logger: false, abortOnError: false })
}
async function port() {
  const server = createServer()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const value = server.address().port
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve()))
  )
  return value
}
const apps = []
try {
  let producer
  if (url) {
    // Independent Nest roots: two cluster runners, a queue worker and a client-only API.
    for (let index = 0; index < 2; index++) {
      apps.push(
        await app(
          {
            ...base,
            topology: 'distributed',
            cluster: { address: { host: '127.0.0.1', port: await port() } },
            execution: { activities: { enabled: false } }
          },
          [Calculation]
        )
      )
    }
    apps.push(
      await app(
        { ...base, topology: 'distributed', execution: { workflows: { enabled: false } } },
        [Maths]
      )
    )
    producer = await app(
      {
        ...base,
        topology: 'distributed',
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      },
      []
    )
  } else {
    producer = await app(base, [Calculation, Maths])
  }
  apps.push(producer)
  const client = producer.get(getWorkflowToken(Calculation))
  const handle = await client.start({ id: 'one', value: 21 })
  assert.equal(handle.created, true)
  await handle.signal(signal, true, { idempotencyKey: 'approval-one' })
  assert.equal(await handle.result({ timeout: '30s' }), 42)
  assert.equal(calls, 2)
  const duplicate = await client.start({ value: 21, id: 'one' })
  assert.equal(duplicate.created, false)
  assert.equal(duplicate.executionId, handle.executionId)
  assert.equal((await handle.describe()).status, 'completed')
  assert.equal(
    (await handle.history()).events.filter((event) => event.type === 'activity.retry-scheduled')
      .length,
    1
  )
  if (url) {
    // Remove a runner; the surviving runner must continue accepting new executions.
    await apps.shift().close()
    const second = await client.start({ id: 'after-runner-stop', value: 10 })
    await second.signal(signal, true, { idempotencyKey: 'approval-two' })
    assert.equal(await second.result({ timeout: '30s' }), 20)
  }
  console.log(
    `${process.version}: ${url ? 'PostgreSQL distributed roots + runner handoff' : 'Node native SQLite'} package smoke passed`
  )
} finally {
  for (const context of apps.reverse()) await context.close()
  await rm(directory, { recursive: true, force: true })
}
