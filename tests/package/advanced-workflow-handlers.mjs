import { Workflow } from 'better-workflows'
import {
  BatchWorkflow,
  ChildWorkflow,
  EvenActivities,
  OddActivities
} from './advanced-contracts.mjs'

export class Child {
  async run(value, ctx) {
    return ctx
      .activities(value.index % 2 === 0 ? EvenActivities : OddActivities)
      .run(value, { stepId: 'work' })
  }
}
Workflow(ChildWorkflow)(Child)

export class Batch {
  async run(id, ctx) {
    return ctx.map(
      'fan-out',
      Array.from({ length: 12 }, (_, index) => ({ index, tenant: `tenant-${index % 3}` })),
      { key: (value) => String(value.index), concurrency: 8 },
      (value, branch) => branch.child('unit', ChildWorkflow, value)
    )
  }
}
Workflow(BatchWorkflow)(Batch)
