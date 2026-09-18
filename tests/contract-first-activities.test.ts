import { expect, test } from 'bun:test'
import { Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Activities,
  ActivitiesContract,
  Activity,
  defineQueue,
  getWorkflowToken,
  Workflow,
  WorkflowContract,
  WorkflowClient,
  WorkflowsModule
} from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { sqlite } from '../src/sqlite'

const Queue = defineQueue('contract-first-activities')
const Input = z.object({ value: z.string() })

@ActivitiesContract({ queue: Queue })
class ContractActivities {
  @Activity({ name: 'contract-first.activity', version: 1, input: Input, output: z.string() })
  execute(_input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
    throw new Error('contract-only')
  }
}

@Injectable()
class Prefix {
  readonly value = 'handled:'
}

@Activities(ContractActivities)
class ActivitiesHandler implements ContractActivities {
  constructor(private readonly prefix: Prefix) {}

  async execute(input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
    return this.prefix.value + input.value
  }
}

@Workflow({
  name: 'contract-first.activity-workflow',
  version: 1,
  input: Input,
  output: z.string()
})
class WorkflowHandler {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<string> {
    return context.activities(ContractActivities).execute(input, { stepId: 'execute' })
  }
}

@ActivitiesContract({ queue: Queue })
class DescriptorlessActivities {
  @Activity({ name: 'descriptorless.activity', version: 1, input: Input, output: z.string() })
  execute(_input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
    throw new Error('contract-only')
  }
}
Reflect.deleteProperty(DescriptorlessActivities.prototype, 'execute')

@Activities(DescriptorlessActivities)
class DescriptorlessHandler {
  async execute(input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
    return `descriptorless:${input.value}`
  }
}

@Workflow({
  name: 'descriptorless.activity-workflow',
  version: 1,
  input: Input,
  output: z.string()
})
class DescriptorlessWorkflow {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<string> {
    return context.activities(DescriptorlessActivities).execute(input, { stepId: 'execute' })
  }
}

const CompositionOutput = z.object({
  mapped: z.array(z.string()),
  parallel: z.object({ one: z.string(), two: z.string() }),
  child: z.string(),
  saga: z.string()
})

@ActivitiesContract({ queue: Queue })
abstract class CompositionActivities {
  @Activity({ name: 'composition.activity', version: 1, input: z.string(), output: z.string() })
  execute(_input: string, _context: ActivityContext): Promise<string> {
    throw new Error('contract-only')
  }
}

@Activities(CompositionActivities)
class CompositionActivitiesHandler implements CompositionActivities {
  async execute(input: string, _context: ActivityContext): Promise<string> {
    return `activity:${input}`
  }
}

@WorkflowContract({ name: 'composition.child', version: 1, input: z.string(), output: z.string() })
abstract class CompositionChild {
  abstract run(input: string, context: WorkflowContext): Promise<string>
}

@Workflow(CompositionChild)
class CompositionChildHandler implements CompositionChild {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(CompositionActivities).execute(input, { stepId: 'activity' })
  }
}

@WorkflowContract({
  name: 'composition.parent',
  version: 1,
  input: z.string(),
  output: CompositionOutput
})
abstract class CompositionParent {
  abstract run(input: string, context: WorkflowContext): Promise<z.infer<typeof CompositionOutput>>
}

@Workflow(CompositionParent)
class CompositionParentHandler implements CompositionParent {
  async run(input: string, context: WorkflowContext): Promise<z.infer<typeof CompositionOutput>> {
    const mapped = await context.map(
      'map',
      ['one', 'two'],
      { key: (item) => item, concurrency: 2 },
      (item, branch) =>
        branch.activities(CompositionActivities).execute(item, { stepId: 'activity' })
    )
    const parallel = await context.parallel('parallel', {
      one: (branch) =>
        branch.activities(CompositionActivities).execute('one', { stepId: 'activity' }),
      two: (branch) =>
        branch.activities(CompositionActivities).execute('two', { stepId: 'activity' })
    })
    const child = await context.child('child', CompositionChild, input)
    const saga = await context.saga('saga', (scope) =>
      scope.step(
        'activity',
        (step) => step.activities(CompositionActivities).execute(input, { stepId: 'forward' }),
        async () => {}
      )
    )
    return { mapped, parallel, child, saga }
  }
}

@Activities({ queue: Queue })
class SimpleMatrixActivities {
  @Activity({ name: 'matrix.simple.activity', version: 1, input: z.string(), output: z.string() })
  async execute(input: string): Promise<string> {
    return `simple:${input}`
  }
}

@WorkflowContract({
  name: 'matrix.advanced.workflow',
  version: 1,
  input: z.string(),
  output: z.string()
})
abstract class MatrixWorkflow {
  abstract run(input: string, context: WorkflowContext): Promise<string>
}

@Workflow(MatrixWorkflow)
class MatrixWorkflowHandler implements MatrixWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(SimpleMatrixActivities).execute(input, { stepId: 'execute' })
  }
}

test('abstract activity contracts are metadata-only and advanced handlers use Nest DI', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'contract-first-activities',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'contract-first-activities',
        workflows: [WorkflowHandler],
        activities: [ActivitiesHandler],
        providers: [Prefix],
        queues: [{ queue: Queue, concurrency: 1 }]
      })
    ]
  }).compile()
  try {
    await app.init()
    expect(app.get(ActivitiesHandler)).toBeDefined()
    expect(() => app.get(ContractActivities)).toThrow()
    const client = app.get<WorkflowClient<typeof WorkflowHandler>>(
      getWorkflowToken(WorkflowHandler)
    )
    expect(await (await client.start({ value: 'one' })).result({ timeout: '3s' })).toBe(
      'handled:one'
    )
  } finally {
    await app.close()
  }
})

test('activity handlers cannot be registered as contracts', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'invalid-activity-contract',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false }, activities: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'invalid-activity-contract',
        activityContracts: [ActivitiesHandler],
        queues: [{ queue: Queue }]
      })
    ]
  }).compile()
  await expect(app.init()).rejects.toMatchObject({ code: 'INVALID_ACTIVITIES_CONTRACT' })
  await app.close().catch(() => {})
})

test('two handlers for one activity contract fail bootstrap', async () => {
  @Activities(ContractActivities)
  class SecondHandler implements ContractActivities {
    async execute(input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
      return input.value
    }
  }
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'duplicate-activity-handler',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'duplicate-activity-handler',
        activities: [ActivitiesHandler, SecondHandler],
        providers: [Prefix],
        queues: [{ queue: Queue }]
      })
    ]
  }).compile()
  await expect(app.init()).rejects.toMatchObject({ code: 'DUPLICATE_ACTIVITY_HANDLER' })
  await app.close().catch(() => {})
})

test('advanced handlers cannot decorate auxiliary methods as activities', async () => {
  @Activities(ContractActivities)
  class InvalidHandler implements ContractActivities {
    @Activity({ name: 'handler-only.activity', version: 1, input: Input, output: z.string() })
    async helper(_input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
      return 'invalid'
    }

    async execute(input: z.infer<typeof Input>, _context: ActivityContext): Promise<string> {
      return input.value
    }
  }
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'redeclared-activity-handler',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'redeclared-activity-handler',
        activities: [InvalidHandler],
        queues: [{ queue: Queue }]
      })
    ]
  }).compile()
  await expect(app.init()).rejects.toMatchObject({
    code: 'ACTIVITY_HANDLER_REDECLARES_CONTRACT'
  })
  await app.close().catch(() => {})
})

test('explicit method metadata routes a contract with no runtime prototype method', async () => {
  expect(Object.getOwnPropertyNames(DescriptorlessActivities.prototype)).not.toContain('execute')
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'descriptorless-activity',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'descriptorless-activity',
        workflows: [DescriptorlessWorkflow],
        activities: [DescriptorlessHandler],
        queues: [{ queue: Queue }]
      })
    ]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof DescriptorlessWorkflow>>(
      getWorkflowToken(DescriptorlessWorkflow)
    )
    expect(await (await client.start({ value: 'one' })).result({ timeout: '3s' })).toBe(
      'descriptorless:one'
    )
  } finally {
    await app.close()
  }
})

test('runtime binding validation catches a handler missing a contract method', async () => {
  // @ts-expect-error This intentionally bypasses compile-time conformance to test the bootstrap guard.
  @Activities(DescriptorlessActivities)
  class MissingHandler {}
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'missing-activity-handler',
        storage: sqlite({ filename: ':memory:' }),
        execution: { workflows: { enabled: false } }
      }),
      WorkflowsModule.forFeature({
        name: 'missing-activity-handler',
        activities: [MissingHandler],
        queues: [{ queue: Queue }]
      })
    ]
  }).compile()
  await expect(app.init()).rejects.toMatchObject({ code: 'MISSING_ACTIVITY_HANDLER' })
  await app.close().catch(() => {})
})

test('advanced workflow composition supports child, map, parallel and saga activities', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'composition-contract-first',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'composition-contract-first',
        workflows: [CompositionParentHandler, CompositionChildHandler],
        activities: [CompositionActivitiesHandler],
        queues: [{ queue: Queue, concurrency: 4 }]
      })
    ]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof CompositionParent>>(
      getWorkflowToken(CompositionParent)
    )
    expect(await (await client.start('root')).result({ timeout: '8s' })).toEqual({
      mapped: ['activity:one', 'activity:two'],
      parallel: { one: 'activity:one', two: 'activity:two' },
      child: 'activity:root',
      saga: 'activity:root'
    })
  } finally {
    await app.close()
  }
})

test('advanced workflows can call simple-mode activities', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'advanced-workflow-simple-activity',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'advanced-workflow-simple-activity',
        workflows: [MatrixWorkflowHandler],
        activities: [SimpleMatrixActivities],
        queues: [{ queue: Queue }]
      })
    ]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof MatrixWorkflow>>(getWorkflowToken(MatrixWorkflow))
    expect(await (await client.start('value')).result({ timeout: '3s' })).toBe('simple:value')
  } finally {
    await app.close()
  }
})
