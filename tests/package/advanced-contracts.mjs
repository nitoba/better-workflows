import {
  defineQueue,
  Activities,
  ActivitiesContract,
  Activity,
  Workflow,
  WorkflowContract
} from 'better-workflows'
import { z } from 'zod'

const input = z.object({ index: z.number().int(), tenant: z.string() })
let audit = (event) => process.send?.(event)
export function setAudit(callback) {
  audit = callback
}

async function work(value, context) {
  const base = { index: value.index, tenant: value.tenant, pid: process.pid }
  audit({ ...base, event: 'start', at: Date.now() })
  await context.heartbeat()
  await new Promise((resolve) => setTimeout(resolve, 180))
  audit({ ...base, event: 'end', at: Date.now() })
  return value.index * 2
}
export class EvenActivities {
  run() {
    throw new Error('contract-only')
  }
}
ActivitiesContract({ queue: defineQueue('shared') })(EvenActivities)
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
ActivitiesContract({ queue: defineQueue('shared') })(OddActivities)
Activity({
  name: 'shared-odd',
  version: 1,
  input,
  output: z.number(),
  key: (value) => value.tenant
})(OddActivities.prototype, 'run', undefined)

export class Even {
  run(value, context) {
    return work(value, context)
  }
}
export class Odd {
  run(value, context) {
    return work(value, context)
  }
}
Activities(EvenActivities)(Even)
Activities(OddActivities)(Odd)
export class Child {
  async run(value, ctx) {
    return ctx
      .activities(value.index % 2 === 0 ? EvenActivities : OddActivities)
      .run(value, { stepId: 'work' })
  }
}
Workflow({ name: 'advanced-child', version: 1, input, output: z.number() })(Child)
export class BatchWorkflow {}
WorkflowContract({
  name: 'advanced-batch',
  version: 1,
  input: z.string(),
  output: z.array(z.number()),
  idempotencyKey: (id) => id
})(BatchWorkflow)
export class Batch {
  async run(id, ctx) {
    return ctx.map(
      'fan-out',
      Array.from({ length: 12 }, (_, index) => ({ index, tenant: `tenant-${index % 3}` })),
      { key: (value) => String(value.index), concurrency: 8 },
      (value, branch) => branch.child('unit', Child, value)
    )
  }
}
Workflow(BatchWorkflow)(Batch)
export const queues = [
  { queue: defineQueue('shared'), concurrency: 4, globalConcurrency: 2, perKeyConcurrency: 1 }
]
