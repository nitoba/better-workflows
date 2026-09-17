import { test, expect } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Database } from 'bun:sqlite'
import { z } from 'zod'
import { Workflow, WorkflowsAdmin } from '../src'
import type { WorkflowContext } from '../src'
import { createWorkflowsAdmin } from '../src/admin'
import { sqlite } from '../src/sqlite'
import { testApp } from './helpers'

@Workflow({
  name: 'admin-test',
  version: 1,
  input: z.string(),
  output: z.string(),
  idempotencyKey: (value) => value
})
class Example {
  async run(value: string, _ctx: WorkflowContext) {
    return value
  }
}

test('standalone migration status/validate are read-only; run creates all schemas without executing workflows', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bw-admin-'))
  const filename = join(dir, 'data.sqlite')
  const admin = await createWorkflowsAdmin({ namespace: 'admin', storage: sqlite({ filename }) })
  try {
    expect((await admin.migrations.status()).valid).toBe(false)
    await expect(admin.migrations.validate()).rejects.toMatchObject({ code: 'MIGRATIONS_REQUIRED' })
    const db = new Database(filename)
    expect(db.query("SELECT name FROM sqlite_master WHERE type='table'").all()).toEqual([])
    const migrated = await admin.migrations.run()
    expect(migrated.journal.applied).toEqual([1, 2, 3, 4, 5, 6])
    expect(migrated.cluster.applied).toEqual([1, 2, 3])
    expect(migrated.queue.applied).toEqual([1, 2])
    expect((await admin.migrations.validate()).valid).toBe(true)
    expect((await admin.migrations.run()).valid).toBe(true)
    expect(db.query('SELECT * FROM better_workflows_runs').all()).toEqual([])
    db.close()
  } finally {
    await admin.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test('stats returns a namespace-wide operational snapshot without payloads', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bw-stats-'))
  const filename = join(dir, 'data.sqlite')
  const admin = await createWorkflowsAdmin({ namespace: 'stats', storage: sqlite({ filename }) })
  const db = new Database(filename)
  const now = Date.now()
  try {
    await admin.migrations.run()
    const addRun = (id: string, state: string, control = 'run') =>
      db
        .query(
          `INSERT INTO better_workflows_runs(
             execution_id, namespace, workflow_name, version, dedupe_key, input_json,
             created_at, updated_at, state, control, chain_id, generation
           ) VALUES (?, 'stats', 'example', 1, ?, 'private-input', ?, ?, ?, ?, ?, 0)`
        )
        .run(id, id, now - 1_000, now - 1_000, state, control, id)
    addRun('accepted', 'accepted')
    addRun('running', 'running')
    addRun('waiting', 'waiting')
    addRun('blocked', 'blocked')
    addRun('paused', 'waiting', 'pause')
    addRun('cancelling', 'running', 'cancel')
    addRun('completed', 'completed')
    addRun('continued', 'continued')

    const addDelivery = (id: string, state: string, createdAt: number) =>
      db
        .query(
          `INSERT INTO better_workflows_activity_deliveries(
             namespace, queue_name, delivery_id, payload_json, attempts, state,
             visible_at, created_at, updated_at
           ) VALUES ('stats', 'emails', ?, 'private-activity-payload', 0, ?, ?, ?, ?)`
        )
        .run(id, state, now, createdAt, createdAt)
    addDelivery('pending-delivery', 'pending', now - 12_400)
    addDelivery('processing-delivery', 'processing', now)

    const addDeadLetter = (id: string, state: string, failedAt: number) =>
      db
        .query(
          `INSERT INTO better_workflows_dead_letters(
             id, namespace, queue_name, delivery_id, delivery_attempt,
             reason_code, reason_message, first_failed_at, updated_at, state, payload_json
           ) VALUES (?, 'stats', 'emails', ?, 1, 'TEST_FAILURE', 'private-message', ?, ?, ?, 'private-dlq-payload')`
        )
        .run(id, `${id}-delivery`, failedAt, failedAt, state)
    addDeadLetter('open-letter', 'open', now - 86_000)
    addDeadLetter('requeued-letter', 'requeued', now - 1_000)

    db.query(
      `INSERT INTO better_workflows_timers(execution_id, step_id, deadline, delivered)
         VALUES ('waiting', 'due-timer', ?, 0), ('waiting', 'future-timer', ?, 0)`
    ).run(now - 2_000, now + 60_000)
    db.query(
      `INSERT INTO better_workflows_retries(execution_id, step_id, attempt, deadline, delivered)
         VALUES ('running', 'retry', 1, ?, 0)`
    ).run(now - 500)
    db.query(
      `INSERT INTO better_workflows_runs(
         execution_id, namespace, workflow_name, version, dedupe_key, input_json,
         created_at, updated_at, state, control, chain_id, generation
       ) VALUES ('foreign-run', 'other-namespace', 'example', 1, 'foreign-key', 'foreign-input', ?, ?, 'waiting', 'run', 'foreign-chain', 0)`
    ).run(now - 1_000, now - 1_000)
    db.query(
      `INSERT INTO better_workflows_timers(execution_id, step_id, deadline, delivered)
         VALUES ('foreign-run', 'foreign-timer', ?, 0)`
    ).run(now - 2_000)
    db.query(
      `INSERT INTO better_workflows_retries(execution_id, step_id, attempt, deadline, delivered)
         VALUES ('foreign-run', 'foreign-retry', 1, ?, 0)`
    ).run(now - 2_000)

    const stats = await admin.stats()
    expect(stats.executions).toEqual({
      accepted: 1,
      running: 1,
      waiting: 1,
      blocked: 1,
      paused: 1,
      cancelling: 1
    })
    expect(stats.queues).toEqual([
      {
        name: 'emails',
        pending: 1,
        processing: 1,
        oldestPendingAgeMs: expect.any(Number)
      }
    ])
    expect(stats.queues[0]!.oldestPendingAgeMs).toBeGreaterThanOrEqual(10_000)
    expect(stats.deadLetters.open).toBe(1)
    expect(stats.deadLetters.requeued).toBe(1)
    expect(stats.deadLetters.oldestOpenAgeMs).toBeGreaterThanOrEqual(80_000)
    expect(stats.deadlines).toMatchObject({ dueTimers: 1, overdueRetries: 1 })
    expect(stats.deadlines.oldestLagMs).toBeGreaterThanOrEqual(1_500)
    expect(JSON.stringify(stats)).not.toContain('private-')
  } finally {
    db.close()
    await admin.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test('journal v6 backfills existing v5 executions into singleton continuation chains', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bw-v4-v5-'))
  const filename = join(dir, 'data.sqlite')
  const admin = await createWorkflowsAdmin({
    namespace: 'v4-upgrade',
    storage: sqlite({ filename })
  })
  const db = new Database(filename)
  try {
    await admin.migrations.run()
    db.exec(`
      INSERT INTO better_workflows_runs(
        execution_id, namespace, workflow_name, version, dedupe_key, input_json, created_at, updated_at,
        chain_id, generation, continued_from, continued_to
      ) VALUES ('legacy-run', 'v4-upgrade', 'legacy', 1, 'legacy-key', '"legacy"', 1, 1, NULL, 0, NULL, NULL);
       DELETE FROM better_workflows_schema WHERE version IN (5, 6);
    `)
    expect((await admin.migrations.status()).journal.pending).toEqual([5, 6])
    await admin.migrations.run()
    expect(
      db
        .query(
          `SELECT chain_id, generation, continued_from, continued_to
           FROM better_workflows_runs WHERE execution_id='legacy-run'`
        )
        .get()
    ).toEqual({
      chain_id: 'legacy-run',
      generation: 0,
      continued_from: null,
      continued_to: null
    })
  } finally {
    db.close()
    await admin.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test('retention requires an intact preview, deletes terminal history and keeps idempotency tombstones', async () => {
  const app = await testApp(Example)
  try {
    const handle = await app.client.start('unique')
    expect(await handle.result({ timeout: '3s' })).toBe('unique')
    await new Promise((resolve) => setTimeout(resolve, 80))
    const admin = app.module.get(WorkflowsAdmin)
    const plan = await admin.retention.preview({ before: new Date(Date.now() - 20).toISOString() })
    expect(plan.blocked).toEqual([])
    expect(plan.candidates.map((item) => item.executionId)).toEqual([handle.executionId])
    const changed = { ...plan, before: '2020-01-01T00:00:00.000Z' }
    await expect(admin.retention.prune(changed, { confirm: true })).rejects.toMatchObject({
      code: 'INVALID_RETENTION_PLAN'
    })
    expect((await handle.history()).events.length).toBeGreaterThan(0)
    const pruned = await admin.retention.prune(plan, { confirm: true })
    expect(pruned.deleted).toBe(1)
    expect(pruned.tombstonesRetained).toBe(1)
    await expect(app.client.start('unique')).rejects.toMatchObject({ code: 'EXECUTION_PRUNED' })
    await expect(app.client.start('different', { idempotencyKey: 'unique' })).rejects.toMatchObject(
      { code: 'IDEMPOTENCY_CONFLICT' }
    )
  } finally {
    await app.close()
  }
})

test('version 1 migration preserves command identities and timer protocol, and rejects incomplete old schemas', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bw-upgrade-'))
  const filename = join(dir, 'data.sqlite')
  const admin = await createWorkflowsAdmin({ namespace: 'upgrade', storage: sqlite({ filename }) })
  const db = new Database(filename)
  try {
    await admin.migrations.run()
    // Reconstruct the exact v1 command layout and ledger, without a second engine implementation.
    db.exec(`DROP TABLE better_workflows_commands;
      CREATE TABLE better_workflows_commands(execution_id TEXT NOT NULL,step_id TEXT NOT NULL,ordinal INTEGER NOT NULL,signature TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'scheduled',PRIMARY KEY(execution_id,step_id),UNIQUE(execution_id,ordinal));
       INSERT INTO better_workflows_commands VALUES('old-run','sleep',0,'old-signature','scheduled');
        DELETE FROM better_workflows_schema WHERE version IN (2, 3, 4, 5, 6);`)
    db.exec(`DROP TABLE better_workflows_waits;
      CREATE TABLE better_workflows_waits(execution_id TEXT NOT NULL,step_id TEXT NOT NULL,signal_name TEXT NOT NULL,deadline DOUBLE PRECISION,state TEXT NOT NULL DEFAULT 'pending',result_json TEXT,delivered INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(execution_id,step_id));`)
    for (const table of [
      'branches',
      'children',
      'sagas',
      'compensations',
      'timers',
      'limits',
      'permits',
      'tombstones'
    ])
      db.exec(`DROP TABLE better_workflows_${table}`)
    expect((await admin.migrations.status()).journal.pending).toEqual([2, 3, 4, 5, 6])
    await expect(admin.migrations.validate()).rejects.toMatchObject({ code: 'MIGRATIONS_REQUIRED' })
    await admin.migrations.run()
    expect(
      db
        .query(
          'SELECT execution_id,step_id,scope,ordinal,signature,protocol FROM better_workflows_commands'
        )
        .all()
    ).toEqual([
      {
        execution_id: 'old-run',
        step_id: 'sleep',
        scope: '',
        ordinal: 0,
        signature: 'old-signature',
        protocol: 1
      }
    ])
    db.exec(
      'DELETE FROM better_workflows_schema WHERE version IN (2, 3, 4, 5, 6); DROP TABLE better_workflows_claims'
    )
    await expect(admin.migrations.run()).rejects.toMatchObject({ code: 'SCHEMA_CORRUPT' })
    expect(
      db.query("SELECT name FROM sqlite_master WHERE name='better_workflows_claims'").all()
    ).toEqual([])
  } finally {
    db.close()
    await admin.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test('retention ignores other namespaces, rechecks stale plans and blocks live claims', async () => {
  const app = await testApp(Example)
  const db = new Database(app.filename)
  const foreign = await createWorkflowsAdmin({
    namespace: 'not-integration',
    storage: sqlite({ filename: app.filename })
  })
  try {
    const handle = await app.client.start('retention-locks')
    await handle.result({ timeout: '3s' })
    await new Promise((resolve) => setTimeout(resolve, 80))
    const admin = app.module.get(WorkflowsAdmin)
    const before = new Date(Date.now() - 20).toISOString()
    expect((await foreign.retention.preview({ before })).candidates).toEqual([])
    const plan = await admin.retention.preview({ before })
    db.query('UPDATE better_workflows_runs SET updated_at=updated_at+1 WHERE execution_id=?').run(
      handle.executionId
    )
    await expect(admin.retention.prune(plan, { confirm: true })).rejects.toMatchObject({
      code: 'RETENTION_PLAN_STALE'
    })
    db.query(
      `INSERT INTO better_workflows_claims(execution_id,step_id,attempt,delivery_attempt,owner_token,lease_until,state) VALUES(?, 'active',1,1,'owner',?,'running')`
    ).run(handle.executionId, Date.now() + 60000)
    const blocked = await admin.retention.preview({ before })
    expect(blocked.candidates).toEqual([])
    expect(blocked.blocked).toEqual([
      { executionId: handle.executionId, reason: 'live-activity-claim' }
    ])
    expect((await handle.describe()).status).toBe('completed')
  } finally {
    db.close()
    await foreign.close()
    await app.close()
  }
})

test('retention protects terminal executions with open or requeued dead letters', async () => {
  const app = await testApp(Example)
  const db = new Database(app.filename)
  try {
    const admin = app.module.get(WorkflowsAdmin)
    for (const [index, state] of ['open', 'requeued'].entries()) {
      const handle = await app.client.start(`dead-letter-${state}`)
      await handle.result({ timeout: '3s' })
      await new Promise((resolve) => setTimeout(resolve, 80))
      const now = Date.now()
      db.query(
        `INSERT INTO better_workflows_dead_letters
          (id, namespace, queue_name, delivery_id, execution_id, step_id, activity_name,
           activity_version, business_attempt, delivery_attempt, reason_code, reason_message,
           first_failed_at, updated_at, requeue_count, state, payload_json)
         VALUES (?, 'integration', 'work', ?, ?, 'step', 'missing', 1, 1, 1,
                 'UNKNOWN_ACTIVITY', 'missing@1', ?, ?, ?, ?, '{}')`
      ).run(
        `retention-dlq-${index}`,
        `retention-delivery-${index}`,
        handle.executionId,
        now,
        now,
        state === 'requeued' ? 1 : 0,
        state
      )
      const before = new Date(Date.now() - 20).toISOString()
      const blocked = await admin.retention.preview({ before })
      expect(blocked.candidates).not.toContainEqual(
        expect.objectContaining({ executionId: handle.executionId })
      )
      expect(blocked.blocked).toContainEqual({
        executionId: handle.executionId,
        reason: 'open-dead-letter'
      })
      db.query(
        `UPDATE better_workflows_dead_letters SET state='resolved', updated_at=?
         WHERE namespace='integration' AND execution_id=?`
      ).run(Date.now(), handle.executionId)
      const eligible = await admin.retention.preview({ before })
      expect(eligible.candidates).toContainEqual(
        expect.objectContaining({ executionId: handle.executionId })
      )
    }
  } finally {
    db.close()
    await app.close()
  }
})
