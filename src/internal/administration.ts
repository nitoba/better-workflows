import { createHash } from 'node:crypto'
import { Effect } from 'effect'
import type { Failure } from '../errors'
import type { QueueOptions } from '../types'
import type { RetentionOptions, RetentionPlan, RetentionResult } from '../admin-types'
import type { Journal, RunRow } from './journal'
import { encode, identifier, positiveInteger } from './values'
import { workflowDefinition } from './wire'

const terminal = (run: RunRow) => ['completed', 'failed', 'cancelled'].includes(run.state)
const fail = (code: string, message: string) =>
  Effect.fail<Failure>({ code, message, retryable: false })
const hash = (value: string) => createHash('sha256').update(value).digest('hex')
const tokenFor = (plan: Omit<RetentionPlan, 'token'>) => hash(encode(plan))
const tables = [
  'commands',
  'events',
  'signals',
  'waits',
  'retries',
  'claims',
  'branches',
  'sagas',
  'compensations',
  'timers',
  'permits'
]

/** Preview is read-only; apply revalidates and locks each candidate in one transaction. */
export class SqlAdministration {
  constructor(readonly journal: Journal) {}

  private queuePayload() {
    const sql = this.journal.sql
    return sql.onDialectOrElse({
      pg: () => sql.literal("element::jsonb #>> '{payload,executionId}'"),
      orElse: () => sql.literal("json_extract(element, '$.payload.executionId')")
    })
  }

  private reason(run: RunRow) {
    const self = this
    const sql = self.journal.sql
    return Effect.gen(function* () {
      if (!terminal(run)) return 'execution-active'
      const now = yield* self.journal.databaseNow()
      const linked = yield* sql`SELECT c.child_id FROM better_workflows_children c
        JOIN better_workflows_runs other ON other.execution_id = CASE WHEN c.parent_id=${run.execution_id} THEN c.child_id ELSE c.parent_id END
        WHERE (c.parent_id=${run.execution_id} OR c.child_id=${run.execution_id}) AND other.state NOT IN ('completed','failed','cancelled') LIMIT 1`
      if (linked.length) return 'active-parent-or-child'
      const claims =
        yield* sql`SELECT owner_token FROM better_workflows_claims WHERE execution_id=${run.execution_id} AND state='running' AND lease_until>${now} LIMIT 1`
      if (claims.length) return 'live-activity-claim'
      const permits =
        yield* sql`SELECT owner_token FROM better_workflows_permits WHERE execution_id=${run.execution_id} AND lease_until>${now} LIMIT 1`
      if (permits.length) return 'live-concurrency-permit'
      const queued =
        yield* sql`SELECT id FROM better_workflows_queue WHERE ${self.queuePayload()}=${run.execution_id} AND acquired_by IS NOT NULL LIMIT 1`
      if (queued.length) return 'unacknowledged-queue-delivery'
      const entity = `Workflow/${workflowDefinition(self.journal.namespace, run.workflow_name, run.version)._tag}`
      const messages =
        yield* sql`SELECT id FROM cluster_messages WHERE entity_id=${run.execution_id}
        AND entity_type IN (${entity}, 'Workflow/-/DurableClock') AND processed=FALSE LIMIT 1`
      if (messages.length) return 'unprocessed-engine-message'
      return null
    })
  }

  preview(options: RetentionOptions) {
    const self = this
    const limit = options.limit ?? 100
    positiveInteger(limit, 'Retention limit')
    const before = Date.parse(options.before)
    return Effect.gen(function* () {
      if (!Number.isFinite(before) || limit > 1000)
        return yield* fail(
          'INVALID_RETENTION',
          'Provide a valid UTC before timestamp and limit <=1000'
        )
      const now = yield* self.journal.databaseNow()
      if (before > now)
        return yield* fail('INVALID_RETENTION', 'Retention cutoff cannot be in the future')
      const runs = yield* self.journal
        .sql<RunRow>`SELECT * FROM better_workflows_runs WHERE namespace=${self.journal.namespace}
        AND state IN ('completed','failed','cancelled') AND updated_at<${before} ORDER BY execution_id LIMIT ${limit}`
      const candidates: RetentionPlan['candidates'][number][] = []
      const blocked: RetentionPlan['blocked'][number][] = []
      for (const run of runs) {
        const reason = yield* self.reason(run)
        if (reason) blocked.push({ executionId: run.execution_id, reason })
        else candidates.push({ executionId: run.execution_id, updatedAt: run.updated_at })
      }
      const plan = {
        namespace: self.journal.namespace,
        before: new Date(before).toISOString(),
        candidates,
        blocked
      }
      return { ...plan, token: tokenFor(plan) }
    })
  }

  prune(plan: RetentionPlan, confirm: boolean) {
    const self = this
    const sql = self.journal.sql
    return sql.withTransaction(
      Effect.gen(function* () {
        if (confirm !== true)
          return yield* fail(
            'CONFIRMATION_REQUIRED',
            'Preview retention first, then call prune(plan, {confirm:true})'
          )
        if (
          !plan ||
          plan.namespace !== self.journal.namespace ||
          !Array.isArray(plan.candidates) ||
          !Array.isArray(plan.blocked) ||
          plan.candidates.length > 1000
        )
          return yield* fail('INVALID_RETENTION_PLAN', 'Plan does not belong to this namespace')
        const expected = tokenFor({
          namespace: plan.namespace,
          before: plan.before,
          candidates: plan.candidates,
          blocked: plan.blocked
        })
        if (expected !== plan.token || !Number.isFinite(Date.parse(plan.before)))
          return yield* fail(
            'INVALID_RETENTION_PLAN',
            'The preview was modified; generate a new preview'
          )
        if (
          new Set(plan.candidates.map((item) => item.executionId)).size !== plan.candidates.length
        )
          return yield* fail('INVALID_RETENTION_PLAN', 'Duplicate execution in preview')
        const now = yield* self.journal.databaseNow()
        if (Date.parse(plan.before) > now)
          return yield* fail('INVALID_RETENTION_PLAN', 'Cutoff is in the future')
        const selected: RunRow[] = []
        for (const candidate of [...plan.candidates].sort((a, b) =>
          a.executionId.localeCompare(b.executionId)
        )) {
          identifier(candidate.executionId, 'Execution ID')
          const run = yield* self.journal.get(candidate.executionId)
          yield* self.journal.lockDedupe(run.workflow_name, run.dedupe_key)
          yield* self.journal.lockRun(run.execution_id)
          const locked = yield* self.journal.get(run.execution_id)
          if (
            locked.updated_at !== candidate.updatedAt ||
            locked.updated_at >= Date.parse(plan.before)
          )
            return yield* fail(
              'RETENTION_PLAN_STALE',
              `Execution ${run.execution_id} changed after preview`
            )
          const reason = yield* self.reason(locked)
          if (reason)
            return yield* fail('RETENTION_PLAN_STALE', `Execution ${run.execution_id}: ${reason}`)
          selected.push(locked)
        }
        for (const run of selected) {
          const entity = `Workflow/${workflowDefinition(self.journal.namespace, run.workflow_name, run.version)._tag}`
          yield* sql`INSERT INTO better_workflows_tombstones(execution_id,namespace,workflow_name,version,dedupe_key,input_hash,state,pruned_at)
          VALUES (${run.execution_id},${run.namespace},${run.workflow_name},${run.version},${run.dedupe_key},${hash(run.input_json)},${run.state},${now})`
          yield* sql`DELETE FROM better_workflows_queue WHERE ${self.queuePayload()}=${run.execution_id}`
          yield* sql`DELETE FROM cluster_replies WHERE request_id IN (SELECT id FROM cluster_messages WHERE entity_id=${run.execution_id} AND entity_type IN (${entity}, 'Workflow/-/DurableClock'))`
          yield* sql`DELETE FROM cluster_messages WHERE entity_id=${run.execution_id} AND entity_type IN (${entity}, 'Workflow/-/DurableClock')`
          for (const table of tables)
            yield* sql`DELETE FROM ${sql(`better_workflows_${table}`)} WHERE execution_id=${run.execution_id}`
          yield* sql`DELETE FROM better_workflows_children WHERE parent_id=${run.execution_id} OR child_id=${run.execution_id}`
          yield* sql`DELETE FROM better_workflows_runs WHERE execution_id=${run.execution_id} AND namespace=${self.journal.namespace}`
        }
        const result: RetentionResult = {
          deleted: selected.length,
          executionIds: selected.map((run) => run.execution_id),
          tombstonesRetained: selected.length
        }
        return result
      })
    )
  }

  setQueueLimits(
    queue: string,
    options: Pick<QueueOptions, 'globalConcurrency' | 'perKeyConcurrency'>
  ) {
    identifier(queue, 'Queue name')
    if (options.globalConcurrency !== undefined)
      positiveInteger(options.globalConcurrency, 'Global concurrency')
    if (options.perKeyConcurrency !== undefined)
      positiveInteger(options.perKeyConcurrency, 'Per-key concurrency')
    const { sql, namespace } = this.journal
    const now = this.journal.databaseNow()
    return sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`UPDATE better_workflows_limits SET queue_name=queue_name WHERE namespace=${namespace} AND queue_name=${queue}`
        const active =
          yield* sql`SELECT execution_id FROM better_workflows_runs WHERE namespace=${namespace} AND state NOT IN ('completed','failed','cancelled') LIMIT 1`
        if (active.length)
          return yield* fail(
            'NAMESPACE_NOT_DRAINED',
            'Drain workflows and stop producers before changing shared queue limits'
          )
        const instant = yield* now
        yield* sql`DELETE FROM better_workflows_permits WHERE namespace=${namespace} AND queue_name=${queue} AND lease_until<=${instant}`
        const permits =
          yield* sql`SELECT owner_token FROM better_workflows_permits WHERE namespace=${namespace} AND queue_name=${queue} LIMIT 1`
        if (permits.length)
          return yield* fail(
            'LIVE_PERMITS',
            'Wait for workers to release outstanding permits before changing limits'
          )
        yield* sql`INSERT INTO better_workflows_limits(namespace,queue_name,global_limit,key_limit) VALUES (${namespace},${queue},${options.globalConcurrency ?? null},${options.perKeyConcurrency ?? null})
        ON CONFLICT(namespace,queue_name) DO UPDATE SET global_limit=${options.globalConcurrency ?? null},key_limit=${options.perKeyConcurrency ?? null}`
      })
    )
  }
}
