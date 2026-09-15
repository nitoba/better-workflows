import { expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Injectable, Inject } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  defineQueue,
  Activities,
  Activity,
  Workflow,
  WorkflowsModule,
  WorkflowClient,
  getWorkflowToken,
  defineSignal,
  InjectWorkflow
} from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { sqlite } from '../src/sqlite'

const Input = z.object({ id: z.string(), value: z.number() })
const Approved = defineSignal('approved', z.boolean())

@Injectable()
class Calculator {
  calls = 0
  double(value: number) {
    this.calls++
    return value * 2
  }
}

@Activities()
class Maths {
  constructor(@Inject(Calculator) private readonly calculator: Calculator) {}

  @Activity({
    name: 'double',
    version: 1,
    queue: defineQueue('maths'),
    input: z.number(),
    output: z.number()
  })
  async double(value: number, context: ActivityContext): Promise<number> {
    await context.heartbeat({ value })
    return this.calculator.double(value)
  }
}

@Workflow({
  name: 'maths',
  version: 1,
  input: Input,
  output: z.number(),
  signals: [Approved],
  idempotencyKey: (input) => input.id
})
class Calculation {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<number> {
    const value = await context.activities(Maths).double(input.value, { stepId: 'double' })
    await context.sleep('brief-pause', '30ms')
    const approved = await context.waitForSignal('approval', Approved, { timeout: '3s' })
    return approved ? value : 0
  }
}

@Injectable()
class Submitter {
  constructor(@InjectWorkflow(Calculation) readonly client: WorkflowClient<typeof Calculation>) {}
}

test('Nest DI + SQLite + queued activity + durable timer + early signal complete end to end', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-'))
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'test',
        storage: sqlite({ filename: join(directory, 'workflows.sqlite') }),
        queues: [{ queue: defineQueue('maths'), concurrency: 2 }],
        pollInterval: '20ms',
        lease: { duration: '2s', refreshInterval: '500ms' }
      }),
      WorkflowsModule.forFeature({
        name: 'calculation',
        workflows: [Calculation],
        activities: [Maths],
        providers: [Calculator]
      })
    ],
    providers: [Submitter]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof Calculation>>(getWorkflowToken(Calculation))
    expect(app.get(Submitter).client).toBe(client)
    const handle = await client.start({ id: 'one', value: 21 })
    expect(handle.created).toBe(true)
    await handle.signal(Approved, true, { idempotencyKey: 'approve-one' })
    expect(await handle.result({ timeout: '8s' })).toBe(42)
    expect(app.get(Calculator).calls).toBe(1)
    const duplicate = await client.start({ value: 21, id: 'one' })
    expect(duplicate.executionId).toBe(handle.executionId)
    expect(duplicate.created).toBe(false)
    expect(await duplicate.result()).toBe(42)
    expect(app.get(Calculator).calls).toBe(1)
    expect((await handle.describe()).status).toBe('completed')
    expect((await handle.history()).events.map((event) => event.type)).toContain('signal.consumed')
    await expect(client.start({ id: 'one', value: 99 })).rejects.toMatchObject({
      code: 'IDEMPOTENCY_CONFLICT'
    })
  } finally {
    await app.close()
    await rm(directory, { recursive: true, force: true })
  }
}, 20000)
