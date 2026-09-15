import { defineQueue, Activities, Activity, Workflow } from 'better-workflows'
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
for (const { provider, name } of [
  { provider: Even, name: 'even' },
  { provider: Odd, name: 'odd' }
]) {
  Activities()(provider)
  Activity({
    name: `shared-${name}`,
    version: 1,
    queue: defineQueue('shared'),
    input,
    output: z.number(),
    key: (value) => value.tenant
  })(provider.prototype, 'run', Object.getOwnPropertyDescriptor(provider.prototype, 'run'))
}
export class Child {
  async run(value, ctx) {
    return ctx.activities(value.index % 2 === 0 ? Even : Odd).run(value, { stepId: 'work' })
  }
}
Workflow({ name: 'advanced-child', version: 1, input, output: z.number() })(Child)
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
Workflow({
  name: 'advanced-batch',
  version: 1,
  input: z.string(),
  output: z.array(z.number()),
  idempotencyKey: (id) => id
})(Batch)
export const queues = [
  { queue: defineQueue('shared'), concurrency: 4, globalConcurrency: 2, perKeyConcurrency: 1 }
]
