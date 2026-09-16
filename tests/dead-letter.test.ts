import { test, expect } from 'bun:test'
import { execFile } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { promisify } from 'node:util'
import { Database } from 'bun:sqlite'
import { z } from 'zod'
import { Activities, Activity, Workflow, WorkflowsAdmin, defineQueue, defineSignal } from '../src'
import type { WorkflowContext } from '../src'
import { createWorkflowsAdmin } from '../src/admin'
import { sqlite } from '../src/sqlite'
import { DurableDeferred } from 'effect/unstable/workflow'
import { activityDeferred, workflowDefinition } from '../src/internal/wire'
import { encode } from '../src/internal/values'
import { testApp, eventually } from './helpers'

const Work = defineQueue('work')
const Wake = defineSignal('wake', z.string())

@Activities()
class KnownActivities {
  @Activity({
    name: 'known.activity',
    version: 1,
    queue: Work,
    input: z.string(),
    output: z.string()
  })
  async run(value: string) {
    return value
  }
}

@Activities()
class MissingActivities {
  @Activity({
    name: 'missing.activity',
    version: 1,
    queue: Work,
    input: z.string(),
    output: z.string()
  })
  async run(value: string) {
    return `recovered:${value}`
  }
}

@Activities()
class ContractOnlyActivities {
  @Activity({
    name: 'contract-only.activity',
    version: 1,
    queue: Work,
    input: z.string(),
    output: z.string()
  })
  async run(value: string) {
    return `recovered:${value}`
  }
}

@Workflow({
  name: 'dead-letter-owner',
  version: 1,
  input: z.string(),
  output: z.string(),
  signals: [Wake]
})
class DeadLetterOwner {
  async run(value: string, ctx: WorkflowContext) {
    await ctx.waitForSignal('wake', Wake)
    return value
  }
}

@Workflow({
  name: 'dead-letter-activity-owner',
  version: 1,
  input: z.string(),
  output: z.string()
})
class DeadLetterActivityOwner {
  async run(value: string, ctx: WorkflowContext) {
    return ctx.activities(MissingActivities).run(value, { stepId: 'recovery-step' })
  }
}

@Workflow({
  name: 'discard-catch-owner',
  version: 1,
  input: z.string(),
  output: z.string()
})
class DiscardCatchOwner {
  async run(value: string, ctx: WorkflowContext) {
    try {
      return await ctx.activities(ContractOnlyActivities).run(value, { stepId: 'discard-step' })
    } catch {
      return 'caught'
    }
  }
}

const execFileAsync = promisify(execFile)

async function runCli(filename: string, ...args: string[]) {
  const { stdout } = await execFileAsync(process.execPath, ['src/cli.ts', ...args], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      WORKFLOWS_NAMESPACE: 'integration',
      WORKFLOWS_SQLITE_FILE: filename,
      WORKFLOWS_DATABASE_URL: ''
    }
  })
  return JSON.parse(stdout)
}

function payload(executionId: string, token: string, activityName = 'missing.activity') {
  return JSON.stringify({
    token,
    activityName,
    activityVersion: 1,
    executionId,
    stepId: 'blocked-step',
    input: encode('payload'),
    attempt: 1,
    timeoutMs: 5000,
    maxAttempts: 1,
    retryDelayMs: 0,
    traceId: 'trace',
    spanId: 'span',
    sampled: false
  })
}

async function seedUnknown(
  filename: string,
  executionId: string | null,
  raw: string,
  deliveryId = 'blocked-delivery'
) {
  const db = new Database(filename)
  db.query(
    `INSERT INTO better_workflows_activity_deliveries
      (namespace, queue_name, delivery_id, execution_id, payload_json, attempts, state, visible_at, created_at, updated_at)
     VALUES ('integration', 'work', ?, ?, ?, 0, 'pending', ?, ?, ?)`
  ).run(deliveryId, executionId, raw, Date.now(), Date.now(), Date.now())
  db.close()
}

async function validToken(executionId: string) {
  const deferred = activityDeferred('blocked-step', 1)
  return DurableDeferred.tokenFromExecutionId(deferred, {
    workflow: workflowDefinition('integration', 'dead-letter-owner', 1),
    executionId
  })
}

test('unknown activity becomes a blocked dead letter with metadata-only admin reads', async () => {
  const app = await testApp(DeadLetterOwner, { providers: [KnownActivities] })
  try {
    const handle = await app.client.start('order')
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await seedUnknown(
      app.filename,
      handle.executionId,
      payload(handle.executionId, await validToken(handle.executionId))
    )
    const snapshot = await eventually(
      () => handle.describe(),
      (value) => value.status === 'blocked' && value.blockedOn !== undefined
    )
    expect(snapshot.blockedOn).toMatchObject({
      type: 'activity',
      stepId: 'blocked-step',
      activity: 'missing.activity',
      version: 1,
      queue: 'work'
    })
    const admin = app.module.get(WorkflowsAdmin)
    const page = await eventually(
      () => admin.listDeadLetters({ state: 'open' }),
      (value) => value.deadLetters.length === 1
    )
    const [deadLetter] = page.deadLetters
    expect(deadLetter).toMatchObject({
      executionId: handle.executionId,
      stepId: 'blocked-step',
      reasonCode: 'UNKNOWN_ACTIVITY',
      state: 'open',
      deliveryAttempt: 1,
      businessAttempt: 1
    })
    expect(deadLetter).not.toHaveProperty('payload')
    expect((await admin.getDeadLetter(deadLetter!.id)).payload).toBeUndefined()
    expect(
      (await admin.getDeadLetter(deadLetter!.id, { includePayload: true })).payload
    ).toBeDefined()
    expect((await handle.history()).events).toContainEqual(
      expect.objectContaining({ type: 'activity.dead-lettered', stepId: 'blocked-step' })
    )
    expect(
      (await runCli(app.filename, 'dead-letters', 'list', '--state', 'open')).deadLetters
    ).toContainEqual(expect.objectContaining({ id: deadLetter!.id, state: 'open' }))
    expect(await runCli(app.filename, 'dead-letters', 'show', deadLetter!.id)).not.toHaveProperty(
      'payload'
    )
    expect(
      (await runCli(app.filename, 'dead-letters', 'show', deadLetter!.id, '--payload')).payload
    ).toBeDefined()
    await admin.discardDeadLetter(deadLetter!.id, { reason: 'The activity deployment was retired' })
    await expect(handle.result({ timeout: '3s' })).rejects.toMatchObject({
      code: 'WORKFLOW_DEAD_LETTER_DISCARDED',
      failure: { code: 'WORKFLOW_DEAD_LETTER_DISCARDED' }
    })
  } finally {
    await app.close()
  }
})

test('discard is an administrative terminal failure outside user catch', async () => {
  const app = await testApp(DiscardCatchOwner, {
    providers: [KnownActivities],
    activityContracts: [ContractOnlyActivities]
  })
  try {
    const handle = await app.client.start('order')
    const blocked = await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'blocked' && snapshot.blockedOn !== undefined
    )
    const admin = app.module.get(WorkflowsAdmin)
    await admin.discardDeadLetter(blocked.blockedOn!.deadLetterId, { reason: 'Retire contract' })
    await expect(handle.result({ timeout: '3s' })).rejects.toMatchObject({
      code: 'WORKFLOW_DEAD_LETTER_DISCARDED',
      failure: { code: 'WORKFLOW_DEAD_LETTER_DISCARDED' }
    })
    expect((await handle.describe()).status).toBe('failed')
  } finally {
    await app.close()
  }
})

test('concurrent requeue produces one delivery and a corrected deployment resolves it', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'bw-dlq-'))
  const filename = join(directory, 'workflows.sqlite')
  const app = await testApp(DeadLetterActivityOwner, {
    filename,
    providers: [KnownActivities],
    activityContracts: [MissingActivities]
  })
  try {
    const handle = await app.client.start('order')
    const blocked = await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'blocked' && snapshot.blockedOn !== undefined
    )
    const admin = app.module.get(WorkflowsAdmin)
    const deadLetter = (
      await eventually(
        () => admin.listDeadLetters(),
        (value) => value.deadLetters.length === 1
      )
    ).deadLetters[0]!
    expect(deadLetter.id).toBe(blocked.blockedOn!.deadLetterId)
    await app.close()
    const first = await createWorkflowsAdmin({
      namespace: 'integration',
      storage: sqlite({ filename })
    })
    const second = await createWorkflowsAdmin({
      namespace: 'integration',
      storage: sqlite({ filename })
    })
    try {
      const results = await Promise.all([
        first.requeueDeadLetter(deadLetter.id),
        second.requeueDeadLetter(deadLetter.id)
      ])
      expect(results.map((result) => result.requeueCount)).toEqual([1, 1])
      expect((await first.getDeadLetter(deadLetter.id)).state).toBe('requeued')
    } finally {
      await first.close()
      await second.close()
    }
    const recovered = await testApp(DeadLetterActivityOwner, {
      filename,
      providers: [MissingActivities]
    })
    try {
      const recoveryAdmin = recovered.module.get(WorkflowsAdmin)
      await eventually(
        () => recoveryAdmin.getDeadLetter(deadLetter.id),
        (value) => value.state === 'resolved',
        3000
      )
      expect((await recoveryAdmin.getDeadLetter(deadLetter.id)).state).toBe('resolved')
      await expect(
        recovered.client.getHandle(handle.executionId).result({ timeout: '3s' })
      ).resolves.toBe('recovered:order')
    } finally {
      await recovered.close()
    }
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('a failed requeue records the replacement dead letter instead of orphaning the owner', async () => {
  const app = await testApp(DeadLetterOwner, { providers: [KnownActivities] })
  try {
    const handle = await app.client.start('order')
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await seedUnknown(
      app.filename,
      handle.executionId,
      payload(handle.executionId, await validToken(handle.executionId))
    )
    const admin = app.module.get(WorkflowsAdmin)
    const original = (
      await eventually(
        () => admin.listDeadLetters({ state: 'open' }),
        (value) => value.deadLetters.length === 1
      )
    ).deadLetters[0]!
    await admin.requeueDeadLetter(original.id)
    const replacement = (
      await eventually(
        () => admin.listDeadLetters({ state: 'open' }),
        (value) => value.deadLetters.length === 1 && value.deadLetters[0]!.id !== original.id
      )
    ).deadLetters[0]!
    expect((await admin.getDeadLetter(original.id)).state).toBe('resolved')
    expect((await handle.describe()).blockedOn?.deadLetterId).toBe(replacement.id)
    await admin.discardDeadLetter(replacement.id, { reason: 'Do not retry this deployment' })
  } finally {
    await app.close()
  }
})

test('corrupt payloads are dead-lettered and cancellation prevents resurrection', async () => {
  const app = await testApp(DeadLetterOwner, { providers: [KnownActivities] })
  try {
    const handle = await app.client.start('order')
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await seedUnknown(app.filename, null, 'not-json', 'corrupt-delivery')
    const admin = app.module.get(WorkflowsAdmin)
    await eventually(
      () => admin.listDeadLetters({ state: 'open' }),
      (value) => value.deadLetters.some((item) => item.reasonCode === 'PAYLOAD_DECODE_FAILED')
    )
    await seedUnknown(app.filename, null, '{}', 'invalid-envelope')
    await seedUnknown(
      app.filename,
      null,
      payload('unowned', await validToken(handle.executionId), 'known.activity').replace(
        '"activityVersion":1',
        '"activityVersion":99'
      ),
      'unknown-version'
    )
    await eventually(
      () => admin.listDeadLetters({ state: 'open' }),
      (value) =>
        value.deadLetters.some((item) => item.reasonCode === 'INVALID_ACTIVITY_ENVELOPE') &&
        value.deadLetters.some((item) => item.reasonCode === 'UNKNOWN_ACTIVITY_VERSION')
    )
    await seedUnknown(
      app.filename,
      handle.executionId,
      payload(handle.executionId, await validToken(handle.executionId)),
      'cancelled-delivery'
    )
    const blocked = await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'blocked'
    )
    const blockedId = blocked.blockedOn!.deadLetterId
    await handle.cancel({ reason: 'Stop owner before restoring deployment' })
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'cancelled'
    )
    expect((await admin.getDeadLetter(blockedId)).state).toBe('discarded')
    await expect(admin.requeueDeadLetter(blockedId)).rejects.toMatchObject({
      code: 'DEAD_LETTER_NOT_OPEN'
    })
  } finally {
    await app.close()
  }
})

test('transport delivery exhaustion becomes an operational dead letter without a business attempt', async () => {
  const app = await testApp(DeadLetterOwner, {
    providers: [KnownActivities],
    deadLetter: { maxDeliveryAttempts: 2 }
  })
  try {
    const handle = await app.client.start('order')
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await seedUnknown(
      app.filename,
      'missing-execution',
      payload('missing-execution', await validToken(handle.executionId), 'known.activity'),
      'exhausted-delivery'
    )
    const admin = app.module.get(WorkflowsAdmin)
    const deadLetter = (
      await eventually(
        () => admin.listDeadLetters({ state: 'open' }),
        (value) =>
          value.deadLetters.some((item) => item.reasonCode === 'DELIVERY_ATTEMPTS_EXHAUSTED')
      )
    ).deadLetters.find((item) => item.reasonCode === 'DELIVERY_ATTEMPTS_EXHAUSTED')!
    expect(deadLetter.deliveryAttempt).toBe(2)
    expect(deadLetter.businessAttempt).toBe(1)
    expect((await handle.describe()).status).toBe('waiting')
  } finally {
    await app.close()
  }
})
