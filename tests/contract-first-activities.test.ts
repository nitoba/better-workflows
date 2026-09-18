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
