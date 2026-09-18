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
