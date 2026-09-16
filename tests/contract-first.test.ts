import { expect, test } from 'bun:test'
import { Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  InjectWorkflow,
  Workflow,
  WorkflowContract,
  WorkflowClient,
  WorkflowsModule,
  getWorkflowToken
} from '../src'
import type { WorkflowContext, WorkflowInput, WorkflowOutput } from '../src'
import { sqlite } from '../src/sqlite'

const Input = z.object({ id: z.string() })

@WorkflowContract({
  name: 'contract-first.generate',
  version: 1,
  input: Input,
  output: z.string(),
  idempotencyKey: (input) => input.id
})
abstract class GenerateWorkflow {
  abstract run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<string>
}

@Injectable()
class Prefix {
  readonly value = 'generated:'
}

@Workflow(GenerateWorkflow)
class GenerateHandler implements GenerateWorkflow {
  constructor(private readonly prefix: Prefix) {}

  async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
    return this.prefix.value + input.id
  }
}

@Injectable()
class Consumer {
  constructor(
    @InjectWorkflow(GenerateWorkflow)
    readonly workflow: WorkflowClient<typeof GenerateWorkflow>
  ) {}
}

test('advanced handlers use Nest DI and expose clients under the contract token', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'contract-first',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'contract-first',
        workflows: [GenerateHandler],
        providers: [Prefix]
      })
    ],
    providers: [Consumer]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof GenerateWorkflow>>(
      getWorkflowToken(GenerateWorkflow)
    )
    expect(app.get(Consumer).workflow).toBe(client)
    expect(await (await client.start({ id: 'one' })).result({ timeout: '3s' })).toBe(
      'generated:one'
    )
    expect(app.get(GenerateHandler)).toBeDefined()
    expect(() => getWorkflowToken(GenerateHandler)).toThrow('workflow handler')
  } finally {
    await app.close()
  }
})

test('client-only registration accepts an abstract contract without constructing its handler', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'contract-client-only',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({ clients: [GenerateWorkflow] })
    ]
  }).compile()
  try {
    await app.init()
    expect(
      app.get<WorkflowClient<typeof GenerateWorkflow>>(getWorkflowToken(GenerateWorkflow))
    ).toBeDefined()
    expect(() => app.get(GenerateHandler)).toThrow()
    expect(() => app.get(GenerateWorkflow)).toThrow()
  } finally {
    await app.close()
  }
})

test('duplicate advanced handlers for one contract are rejected', async () => {
  @Workflow(GenerateWorkflow)
  class AnotherHandler implements GenerateWorkflow {
    async run(input: z.infer<typeof Input>, _context: WorkflowContext): Promise<string> {
      return input.id
    }
  }
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'contract-duplicate',
        storage: sqlite({ filename: ':memory:' })
      }),
      WorkflowsModule.forFeature({
        name: 'contract-duplicate',
        workflows: [GenerateHandler, AnotherHandler],
        providers: [Prefix]
      })
    ]
  }).compile()
  await expect(app.init()).rejects.toMatchObject({ code: 'DUPLICATE_WORKFLOW_HANDLER' })
  await app.close().catch(() => {})
})

test('advanced child workflows resolve by contract identity', async () => {
  const ChildInput = z.object({ value: z.string() })
  @WorkflowContract({
    name: 'contract-first.child',
    version: 1,
    input: ChildInput,
    output: z.string()
  })
  abstract class ChildWorkflow {
    abstract run(input: z.infer<typeof ChildInput>, context: WorkflowContext): Promise<string>
  }
  @Workflow(ChildWorkflow)
  class ChildHandler implements ChildWorkflow {
    async run(input: z.infer<typeof ChildInput>, _context: WorkflowContext): Promise<string> {
      return `child:${input.value}`
    }
  }
  @WorkflowContract({
    name: 'contract-first.parent',
    version: 1,
    input: z.string(),
    output: z.string()
  })
  abstract class ParentWorkflow {
    abstract run(input: string, context: WorkflowContext): Promise<string>
  }
  @Workflow(ParentWorkflow)
  class ParentHandler implements ParentWorkflow {
    async run(input: string, context: WorkflowContext): Promise<string> {
      return context.child('child', ChildWorkflow, { value: input })
    }
  }
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'contract-first-child',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'contract-first-child',
        workflows: [ParentHandler, ChildHandler]
      })
    ]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof ParentWorkflow>>(getWorkflowToken(ParentWorkflow))
    expect(await (await client.start('one')).result({ timeout: '3s' })).toBe('child:one')
  } finally {
    await app.close()
  }
})

test('@Workflow(contract) rejects an undecorated class', () => {
  class Undecorated {
    async run(input: string, _context: WorkflowContext): Promise<string> {
      return input
    }
  }
  let error: unknown
  try {
    Workflow(Undecorated)
  } catch (caught) {
    error = caught
  }
  expect(error).toMatchObject({ code: 'INVALID_WORKFLOW_CONTRACT' })
})

test('simple workflow classes cannot be used as advanced contract arguments', () => {
  @Workflow({ name: 'contract-first.simple', version: 1, input: z.string(), output: z.string() })
  class SimpleWorkflow {
    async run(input: string, _context: WorkflowContext): Promise<string> {
      return input
    }
  }
  expect(() => Workflow(SimpleWorkflow)).toThrow('@WorkflowContract')
})

async function assertAbstractContractTypes(client: WorkflowClient<typeof GenerateWorkflow>) {
  const input: WorkflowInput<typeof GenerateWorkflow> = { id: 'typed' }
  const handle = await client.start(input)
  const output: WorkflowOutput<typeof GenerateWorkflow> = await handle.result()
  const typed: string = output
  return typed
}

test('workflow input and output infer from an abstract contract run signature', () => {
  expect(assertAbstractContractTypes).toBeFunction()
})
