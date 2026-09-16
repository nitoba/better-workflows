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
    expect(migrated.journal.applied).toEqual([1, 2, 3, 4])
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
      DELETE FROM better_workflows_schema WHERE version IN (2, 3, 4);`)
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
    expect((await admin.migrations.status()).journal.pending).toEqual([2, 3, 4])
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
      'DELETE FROM better_workflows_schema WHERE version IN (2, 3, 4); DROP TABLE better_workflows_claims'
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
