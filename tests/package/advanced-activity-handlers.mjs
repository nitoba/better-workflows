import { Activities } from 'better-workflows'
import { EvenActivities, OddActivities } from './advanced-contracts.mjs'
import { recordAudit } from './advanced-activity-events.mjs'

async function work(value, context) {
  const base = { index: value.index, tenant: value.tenant, pid: process.pid }
  recordAudit({ ...base, event: 'start', at: Date.now() })
  await context.heartbeat()
  await new Promise((resolve) => setTimeout(resolve, 180))
  recordAudit({ ...base, event: 'end', at: Date.now() })
  return value.index * 2
}

export class Even {
  run(value, context) {
    return work(value, context)
  }
}
Activities(EvenActivities)(Even)

export class Odd {
  run(value, context) {
    return work(value, context)
  }
}
Activities(OddActivities)(Odd)
