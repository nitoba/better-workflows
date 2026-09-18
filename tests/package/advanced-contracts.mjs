import { defineQueue, ActivitiesContract, Activity, WorkflowContract } from 'better-workflows'
import { z } from 'zod'

export const input = z.object({ index: z.number().int(), tenant: z.string() })
export const queues = [
  { queue: defineQueue('shared'), concurrency: 4, globalConcurrency: 2, perKeyConcurrency: 1 }
]

export class EvenActivities {
  run() {
    throw new Error('contract-only')
  }
}
ActivitiesContract({ queue: queues[0].queue })(EvenActivities)
Activity({
  name: 'shared-even',
  version: 1,
  input,
  output: z.number(),
  key: (value) => value.tenant
})(EvenActivities.prototype, 'run', undefined)

export class OddActivities {
  run() {
    throw new Error('contract-only')
  }
}
ActivitiesContract({ queue: queues[0].queue })(OddActivities)
Activity({
  name: 'shared-odd',
  version: 1,
  input,
  output: z.number(),
  key: (value) => value.tenant
})(OddActivities.prototype, 'run', undefined)

export class ChildWorkflow {}
WorkflowContract({ name: 'advanced-child', version: 1, input, output: z.number() })(ChildWorkflow)

export class BatchWorkflow {}
WorkflowContract({
  name: 'advanced-batch',
  version: 1,
  input: z.string(),
  output: z.array(z.number()),
  idempotencyKey: (id) => id
})(BatchWorkflow)
