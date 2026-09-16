import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { z } from 'zod'
import {
  ActivityError,
  getWorkflowToken,
  Workflow,
  WorkflowContract,
  WorkflowClient,
  WorkflowsAdmin,
  WorkflowsModule,
  defineSignal
} from '../src'
import type { WorkflowContext } from '../src'
import { WorkflowsRuntime } from '../src/internal/runtime'
import { testApp, eventually } from './helpers'
import { Test } from '@nestjs/testing'
import { sqlite } from '../src/sqlite'

const Input = z.object({ generation: z.number().int().nonnegative() })
const Wake = defineSignal('wake', z.boolean())

@Workflow({
  name: 'continue-as-new.simple',
  version: 1,
  input: Input,
  output: z.string(),
  signals: [Wake]
})
class SimpleContinuation {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<string> {
    if (input.generation < 2) return context.continueAsNew({ generation: input.generation + 1 })
    return `generation-${input.generation}`
  }
}

@Workflow({
  name: 'continue-as-new.infinite',
  version: 1,
  input: z.number().int().nonnegative(),
  output: z.string()
})
class InfiniteContinuation {
  async run(input: number, context: WorkflowContext): Promise<string> {
    return context.continueAsNew(input + 1)
  }
}

test('continueAsNew creates generations and result follows the chain', async () => {
  const app = await testApp(SimpleContinuation)
  try {
    const handle = await app.client.start({ generation: 0 })
    const first = await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'continued'
    )
    expect(first.continuation).toMatchObject({ generation: 1 })
    expect(first.continuation?.executionId).not.toBe(handle.executionId)
    expect(await handle.result({ timeout: '5s' })).toBe('generation-2')
    expect((await handle.describe()).status).toBe('continued')
    expect((await handle.history()).events.map((event) => event.type)).toContain(
      'workflow.continued'
    )
    const second = app.client.getHandle(first.continuation!.executionId)
    expect((await second.describe()).status).toBe('continued')
    expect((await second.describe()).continuation?.generation).toBe(2)
  } finally {
    await app.close()
  }
})

test('result timeout also applies while following an unbounded continuation chain', async () => {
  const app = await testApp(InfiniteContinuation)
  try {
    const handle = await app.client.start(0)
    await expect(handle.result({ timeout: '100ms' })).rejects.toMatchObject({
      code: 'WAIT_TIMEOUT'
    })
  } finally {
    await app.close()
  }
})

test('signals and controls remain execution-specific after continuation', async () => {
  const app = await testApp(SimpleContinuation)
  try {
    const handle = await app.client.start({ generation: 0 })
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'continued'
    )
    await expect(handle.signal(Wake, true, { idempotencyKey: 'late' })).rejects.toMatchObject({
      code: 'TERMINAL_EXECUTION'
    })
    await handle.cancel()
    await expect(handle.resume()).rejects.toMatchObject({ code: 'TERMINAL_EXECUTION' })
    expect(await handle.result({ timeout: '5s' })).toBe('generation-2')
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'continue-as-new.invalid',
  version: 1,
  input: Input,
  output: z.string()
})
class InvalidContinuation {
  async run(_input: z.infer<typeof Input>, context: WorkflowContext): Promise<string> {
    return context.continueAsNew({ generation: -1 })
  }
}

test('continueAsNew validates the next input before persisting a generation', async () => {
  const app = await testApp(InvalidContinuation)
  try {
    const handle = await app.client.start({ generation: 0 })
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'VALIDATION_FAILED'
    })
    expect((await handle.describe()).status).toBe('failed')
    expect(
      (await handle.history()).events.some((event) => event.type === 'workflow.continued')
    ).toBe(false)
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'continue-as-new.branch',
  version: 1,
  input: z.number(),
  output: z.string()
})
class BranchContinuation {
  async run(_input: number, context: WorkflowContext): Promise<string> {
    try {
      await context.parallel('branches', {
        one: async (branch) => branch.continueAsNew(1)
      })
    } catch (error) {
      if (error instanceof ActivityError && error.code === 'CONTINUE_AS_NEW_NOT_ROOT')
        return 'rejected'
      throw error
    }
    return 'unexpected'
  }
}

test('continueAsNew is rejected from structured branches', async () => {
  const app = await testApp(BranchContinuation)
  try {
    const handle = await app.client.start(0)
    expect(await handle.result({ timeout: '5s' })).toBe('rejected')
    expect((await handle.describe()).status).toBe('completed')
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'continue-as-new.control-flow',
  version: 1,
  input: z.number(),
  output: z.string()
})
class ControlFlowContinuation {
  async run(input: number, context: WorkflowContext): Promise<string> {
    try {
      if (input === 0) return context.continueAsNew(1)
      return 'done'
    } catch {
      return 'caught'
    }
  }
}

test('continueAsNew is interpreter control flow and does not enter user catch', async () => {
  const app = await testApp(ControlFlowContinuation)
  try {
    expect(await (await app.client.start(0)).result({ timeout: '5s' })).toBe('done')
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'continue-as-new.saga',
  version: 1,
  input: z.number(),
  output: z.string()
})
class SagaContinuation {
  compensationRuns = 0

  async run(_input: number, context: WorkflowContext): Promise<string> {
    return context.saga('scope', async (saga) => {
      await saga.step(
        'first',
        async () => 'registered',
        async () => {
          this.compensationRuns++
        }
      )
      return saga.continueAsNew(1)
    })
  }
}

test('continueAsNew from saga contexts fails without compensation', async () => {
  const app = await testApp(SagaContinuation)
  try {
    const handle = await app.client.start(0)
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'CONTINUE_AS_NEW_NOT_ROOT'
    })
    expect(app.module.get(SagaContinuation).compensationRuns).toBe(0)
  } finally {
    await app.close()
  }
})

@WorkflowContract({
  name: 'continue-as-new.contract',
  version: 1,
  input: Input,
  output: z.string()
})
abstract class ContractContinuation {
  abstract run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<string>
}

@Workflow(ContractContinuation)
class ContractContinuationHandler implements ContractContinuation {
  async run(input: z.infer<typeof Input>, context: WorkflowContext): Promise<string> {
    if (input.generation === 0) return context.continueAsNew({ generation: 1 })
    return 'contract-done'
  }
}

test('contract-first continuation keeps contract identity', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'continue-as-new-contract',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '10ms'
      }),
      WorkflowsModule.forFeature({
        name: 'continue-as-new-contract',
        workflows: [ContractContinuationHandler]
      })
    ]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof ContractContinuation>>(
      getWorkflowToken(ContractContinuation)
    )
    const handle = await client.start({ generation: 0 })
    expect(await handle.result({ timeout: '5s' })).toBe('contract-done')
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'continue-as-new.child',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Wake]
})
class ContinuationChild {
  async run(_input: string, context: WorkflowContext): Promise<string> {
    await context.waitForSignal('hold', Wake)
    return 'child-done'
  }
}

@Workflow({
  name: 'continue-as-new.parent',
  version: 1,
  input: z.object({ policy: z.enum(['abandon', 'request-cancel']), generation: z.number() }),
  output: z.string()
})
class ContinuationParent {
  async run(
    input: { policy: 'abandon' | 'request-cancel'; generation: number },
    context: WorkflowContext
  ): Promise<string> {
    if (input.generation === 0) {
      await context.startChild('child', ContinuationChild, 'child', {
        parentClosePolicy: input.policy
      })
      return context.continueAsNew({ ...input, generation: 1 })
    }
    return 'parent-done'
  }
}

test('continued parents apply child close policy without moving children to the next generation', async () => {
  const app = await testApp(ContinuationParent, { providers: [ContinuationChild] })
  try {
    const runtime = app.module.get(WorkflowsRuntime)
    const childClient = new WorkflowClient(runtime, ContinuationChild)
    for (const policy of ['request-cancel', 'abandon'] as const) {
      const parent = await app.client.start({ policy, generation: 0 })
      const first = await eventually(
        () => parent.describe(),
        (snapshot) => snapshot.status === 'continued'
      )
      expect(await parent.result({ timeout: '5s' })).toBe('parent-done')
      const events = (await parent.history({ limit: 100 })).events
      const childStarted = events.find((event) => event.type === 'child.started')
      expect(childStarted).toBeDefined()
      // SAFETY: child.started details always contain the linked child execution ID.
      const childId = (childStarted!.details as { executionId: string }).executionId
      const child = childClient.getHandle(childId)
      if (policy === 'request-cancel') {
        await expect(child.result({ timeout: '5s' })).rejects.toMatchObject({
          code: 'WORKFLOW_CANCELLED'
        })
      } else {
        expect((await child.describe()).status).not.toBe('cancelled')
        await child.signal(Wake, true, { idempotencyKey: `wake-${policy}` })
        expect(await child.result({ timeout: '5s' })).toBe('child-done')
      }
      expect(first.continuation?.executionId).not.toBe(childId)
    }
  } finally {
    await app.close()
  }
})

@Workflow({
  name: 'continue-as-new.child-chain',
  version: 1,
  input: z.number(),
  output: z.string()
})
class ChainedChild {
  async run(generation: number, context: WorkflowContext): Promise<string> {
    if (generation === 0) return context.continueAsNew(1)
    return 'child-chain-done'
  }
}

@Workflow({
  name: 'continue-as-new.child-owner',
  version: 1,
  input: z.string(),
  output: z.string()
})
class ChildOwner {
  async run(_input: string, context: WorkflowContext): Promise<string> {
    return context.child('child', ChainedChild, 0)
  }
}

test('parent child delivery follows a child continuation chain', async () => {
  const app = await testApp(ChildOwner, { providers: [ChainedChild] })
  try {
    const handle = await app.client.start('child-chain')
    expect(await handle.result({ timeout: '5s' })).toBe('child-chain-done')
  } finally {
    await app.close()
  }
})

test('retention removes a continuation chain as one unit', async () => {
  const app = await testApp(SimpleContinuation)
  try {
    const handle = await app.client.start({ generation: 0 })
    expect(await handle.result({ timeout: '5s' })).toBe('generation-2')
    await new Promise((resolve) => setTimeout(resolve, 80))
    const admin = app.module.get(WorkflowsAdmin)
    const plan = await admin.retention.preview({ before: new Date(Date.now() - 20).toISOString() })
    expect(plan.blocked).toEqual([])
    expect(plan.candidates).toHaveLength(3)
    const pruned = await admin.retention.prune(plan, { confirm: true })
    expect(pruned.deleted).toBe(3)
    expect(pruned.tombstonesRetained).toBe(3)
  } finally {
    await app.close()
  }
})

test('retention does not preview a partial continuation chain when one generation is blocked', async () => {
  const app = await testApp(SimpleContinuation)
  try {
    const handle = await app.client.start({ generation: 0 })
    expect(await handle.result({ timeout: '5s' })).toBe('generation-2')
    const first = await handle.describe()
    const second = app.client.getHandle(first.continuation!.executionId)
    const db = new Database(app.filename)
    db.query(
      `INSERT INTO better_workflows_claims(
         execution_id, step_id, attempt, delivery_attempt, owner_token, lease_until, state
       ) VALUES (?, 'retention-block', 1, 1, 'retention-owner', ?, 'running')`
    ).run(second.executionId, Date.now() + 60_000)
    db.close()
    await new Promise((resolve) => setTimeout(resolve, 80))
    const admin = app.module.get(WorkflowsAdmin)
    const plan = await admin.retention.preview({ before: new Date(Date.now() - 20).toISOString() })
    expect(plan.candidates).toEqual([])
    expect(plan.blocked).toContainEqual({
      executionId: second.executionId,
      reason: 'live-activity-claim'
    })
  } finally {
    await app.close()
  }
})

test('result detects a corrupted continuation cycle instead of looping forever', async () => {
  const app = await testApp(SimpleContinuation)
  try {
    const handle = await app.client.start({ generation: 0 })
    expect(await handle.result({ timeout: '5s' })).toBe('generation-2')
    const first = await handle.describe()
    const second = app.client.getHandle(first.continuation!.executionId)
    const secondSnapshot = await second.describe()
    const third = app.client.getHandle(secondSnapshot.continuation!.executionId)
    const db = new Database(app.filename)
    db.query(
      `UPDATE better_workflows_runs SET state='continued', continued_to=?
       WHERE execution_id=? AND namespace='integration'`
    ).run(handle.executionId, third.executionId)
    db.close()
    await expect(handle.result({ timeout: '5s' })).rejects.toMatchObject({
      code: 'STORAGE_INTEGRITY'
    })
  } finally {
    await app.close()
  }
})
