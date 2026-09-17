import { createHash } from 'node:crypto'
import { Effect } from 'effect'
import type { Failure } from '../errors'
import type { QueueOptions } from '../types'
import type {
  DeadLetterListOptions,
  DeadlineStats,
  DeadLetterStats,
  DiscardDeadLetterOptions,
  QueueStats,
  RetentionOptions,
  RetentionPlan,
  RetentionResult,
  WorkflowExecutionStats,
  WorkflowsStats
} from '../admin-types'
import type { Journal, RunRow } from './journal'
import { encode, identifier, positiveInteger } from './values'
import { workflowDefinition } from './wire'
import { ActivityTransport } from './activity-transport'

const terminal = (run: RunRow) =>
  ['continued', 'completed', 'failed', 'cancelled'].includes(run.state)
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
  'permits',
  'reconciliations',
  'activity_deliveries'
]

/** Preview is read-only; apply revalidates and locks each candidate in one transaction. */
export class SqlAdministration {
  constructor(readonly journal: Journal) {}

  /** Read a namespace-wide operational snapshot using fixed-size aggregation queries. */
  stats() {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.journal.databaseNow()
      const executionRows = yield* self.journal.sql<{
        status: string
        count: number
      }>`SELECT status, COUNT(*) AS count FROM (
        SELECT CASE
          WHEN state IN ('continued', 'completed', 'failed', 'cancelled') THEN state
          WHEN control = 'pause' THEN 'paused'
          WHEN control = 'cancel' THEN 'cancelling'
          ELSE state
        END AS status
        FROM better_workflows_runs
        WHERE namespace = ${self.journal.namespace}
      ) AS current_executions
      WHERE status IN ('accepted', 'running', 'waiting', 'blocked', 'paused', 'cancelling')
      GROUP BY status`
      const executions = {
        accepted: 0,
        running: 0,
        waiting: 0,
        blocked: 0,
        paused: 0,
        cancelling: 0
      } satisfies WorkflowExecutionStats
      for (const row of executionRows) {
        switch (row.status) {
          case 'accepted':
            executions.accepted = Number(row.count)
            break
          case 'running':
            executions.running = Number(row.count)
            break
          case 'waiting':
            executions.waiting = Number(row.count)
            break
          case 'blocked':
            executions.blocked = Number(row.count)
            break
          case 'paused':
            executions.paused = Number(row.count)
            break
          case 'cancelling':
            executions.cancelling = Number(row.count)
            break
        }
      }

      const queueRows = yield* self.journal.sql<{
        name: string
        pending: number
        processing: number
        oldest_pending_at: number | null
      }>`SELECT queue_name AS name,
        COUNT(CASE WHEN state = 'pending' THEN 1 END) AS pending,
        COUNT(CASE WHEN state = 'processing' THEN 1 END) AS processing,
        MIN(CASE WHEN state = 'pending' THEN created_at END) AS oldest_pending_at
        FROM better_workflows_activity_deliveries
        WHERE namespace = ${self.journal.namespace}
        GROUP BY queue_name
        ORDER BY queue_name`
      const queues: QueueStats[] = queueRows.map((row) => ({
        name: row.name,
        pending: Number(row.pending),
        processing: Number(row.processing),
        oldestPendingAgeMs:
          row.oldest_pending_at === null
            ? 0
            : Math.max(0, Number(now) - Number(row.oldest_pending_at))
      }))

      const [deadLettersRow] = yield* self.journal.sql<{
        open_count: number
        requeued_count: number
        oldest_open_at: number | null
      }>`SELECT
        COUNT(CASE WHEN state = 'open' THEN 1 END) AS open_count,
        COUNT(CASE WHEN state = 'requeued' THEN 1 END) AS requeued_count,
        MIN(CASE WHEN state = 'open' THEN first_failed_at END) AS oldest_open_at
        FROM better_workflows_dead_letters
        WHERE namespace = ${self.journal.namespace}`
      const deadLetters: DeadLetterStats = {
        open: Number(deadLettersRow?.open_count ?? 0),
        requeued: Number(deadLettersRow?.requeued_count ?? 0),
        oldestOpenAgeMs:
          deadLettersRow?.oldest_open_at === null || deadLettersRow?.oldest_open_at === undefined
            ? 0
            : Math.max(0, Number(now) - Number(deadLettersRow.oldest_open_at))
      }

      const [deadlineRow] = yield* self.journal.sql<{
        due_timers: number
        overdue_retries: number
        oldest_timer_lag: number | null
        oldest_retry_lag: number | null
      }>`SELECT
        (SELECT COUNT(*) FROM better_workflows_timers
          JOIN better_workflows_runs ON better_workflows_runs.execution_id = better_workflows_timers.execution_id
          WHERE better_workflows_runs.namespace = ${self.journal.namespace}
            AND better_workflows_runs.control <> 'cancel'
            AND better_workflows_runs.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
            AND better_workflows_timers.delivered = 0 AND better_workflows_timers.deadline <= ${now}) AS due_timers,
        (SELECT COUNT(*) FROM better_workflows_retries
          JOIN better_workflows_runs ON better_workflows_runs.execution_id = better_workflows_retries.execution_id
          WHERE better_workflows_runs.namespace = ${self.journal.namespace}
            AND better_workflows_runs.control <> 'cancel'
            AND better_workflows_runs.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
            AND better_workflows_retries.delivered = 0 AND better_workflows_retries.deadline <= ${now}) AS overdue_retries,
        (SELECT MAX(${now} - better_workflows_timers.deadline) FROM better_workflows_timers
          JOIN better_workflows_runs ON better_workflows_runs.execution_id = better_workflows_timers.execution_id
          WHERE better_workflows_runs.namespace = ${self.journal.namespace}
            AND better_workflows_runs.control <> 'cancel'
            AND better_workflows_runs.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
            AND better_workflows_timers.delivered = 0 AND better_workflows_timers.deadline <= ${now}) AS oldest_timer_lag,
        (SELECT MAX(${now} - better_workflows_retries.deadline) FROM better_workflows_retries
          JOIN better_workflows_runs ON better_workflows_runs.execution_id = better_workflows_retries.execution_id
          WHERE better_workflows_runs.namespace = ${self.journal.namespace}
            AND better_workflows_runs.control <> 'cancel'
            AND better_workflows_runs.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
            AND better_workflows_retries.delivered = 0 AND better_workflows_retries.deadline <= ${now}) AS oldest_retry_lag`
      const timerLag = Number(deadlineRow?.oldest_timer_lag ?? 0)
      const retryLag = Number(deadlineRow?.oldest_retry_lag ?? 0)
      const deadlines: DeadlineStats = {
        dueTimers: Number(deadlineRow?.due_timers ?? 0),
        overdueRetries: Number(deadlineRow?.overdue_retries ?? 0),
        oldestLagMs: Math.max(0, timerLag, retryLag)
      }
      const snapshot: WorkflowsStats = {
        generatedAt: new Date(Number(now)).toISOString(),
        executions,
        queues,
        deadLetters,
        deadlines
      }
      return snapshot
    })
  }

  private reason(run: RunRow) {
    const self = this
    const sql = self.journal.sql
    return Effect.gen(function* () {
      if (!terminal(run)) return 'execution-active'
      const now = yield* self.journal.databaseNow()
      const linked = yield* sql<{ execution_id: string }>`SELECT CASE
          WHEN c.parent_id=${run.execution_id} THEN c.child_id ELSE c.parent_id END AS execution_id
        FROM better_workflows_children c
        WHERE c.parent_id=${run.execution_id} OR c.child_id=${run.execution_id}`
      for (const relation of linked) {
        const owner = yield* self.journal.followContinuation(relation.execution_id)
        if (!terminal(owner)) return 'active-parent-or-child'
      }
      const claims =
        yield* sql`SELECT owner_token FROM better_workflows_claims WHERE execution_id=${run.execution_id} AND state='running' AND lease_until>${now} LIMIT 1`
      if (claims.length) return 'live-activity-claim'
      const permits =
        yield* sql`SELECT owner_token FROM better_workflows_permits WHERE execution_id=${run.execution_id} AND lease_until>${now} LIMIT 1`
      if (permits.length) return 'live-concurrency-permit'
      const queued = yield* sql`SELECT delivery_id FROM better_workflows_activity_deliveries
          WHERE namespace=${self.journal.namespace}
          AND execution_id=${run.execution_id}
          AND state IN ('pending','processing')
          LIMIT 1`
      if (queued.length) return 'unacknowledged-queue-delivery'
      const reconciliation = yield* sql`SELECT execution_id FROM better_workflows_reconciliations
          WHERE execution_id=${run.execution_id} AND namespace=${self.journal.namespace} AND delivered=0 LIMIT 1`
      if (reconciliation.length) return 'pending-reconciliation'
      const deadLetters = yield* sql`SELECT id FROM better_workflows_dead_letters
        WHERE namespace=${self.journal.namespace} AND execution_id=${run.execution_id}
        AND state IN ('open','requeued') LIMIT 1`
      if (deadLetters.length) return 'open-dead-letter'
      const entity = `Workflow/${workflowDefinition(self.journal.namespace, run.workflow_name, run.version)._tag}`
      if (run.state !== 'continued') {
        const messages =
          yield* sql`SELECT id FROM cluster_messages WHERE entity_id=${run.execution_id}
          AND entity_type IN (${entity}, 'Workflow/-/DurableClock') AND processed=FALSE LIMIT 1`
        if (messages.length) return 'unprocessed-engine-message'
      }
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
        AND state IN ('continued','completed','failed','cancelled') AND updated_at<${before} ORDER BY execution_id LIMIT ${limit}`
      const candidates: RetentionPlan['candidates'][number][] = []
      const blocked: RetentionPlan['blocked'][number][] = []
      const seenChains = new Set<string>()
      for (const run of runs) {
        if (seenChains.has(run.chain_id)) continue
        seenChains.add(run.chain_id)
        const chain = yield* self.journal.sql<RunRow>`SELECT * FROM better_workflows_runs
          WHERE namespace=${self.journal.namespace} AND chain_id=${run.chain_id} ORDER BY generation, execution_id`
        const byId = new Map(chain.map((member) => [member.execution_id, member]))
        const broken =
          chain.some((member, index) =>
            index === 0
              ? member.continued_from !== null
              : member.continued_from !== chain[index - 1]!.execution_id
          ) ||
          chain.some(
            (member) =>
              member.state === 'continued' &&
              (!member.continued_to || !byId.has(member.continued_to))
          )
        const chainReason = broken
          ? 'broken-continuation-chain'
          : chain.some((member) => !terminal(member))
            ? 'continuation-chain-active'
            : chain.some((member) => member.updated_at >= before)
              ? 'continuation-chain-not-expired'
              : null
        if (chainReason) {
          blocked.push({ executionId: run.execution_id, reason: chainReason })
          continue
        }
        const chainCandidates: RetentionPlan['candidates'][number][] = []
        for (const member of chain) {
          const reason = yield* self.reason(member)
          if (reason) {
            blocked.push({ executionId: member.execution_id, reason })
            chainCandidates.length = 0
            break
          }
          chainCandidates.push({ executionId: member.execution_id, updatedAt: member.updated_at })
        }
        candidates.push(...chainCandidates)
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
        const selectedIds = new Set(selected.map((run) => run.execution_id))
        for (const run of selected) {
          const chain = yield* sql<{
            execution_id: string
          }>`SELECT execution_id FROM better_workflows_runs
            WHERE namespace=${self.journal.namespace} AND chain_id=${run.chain_id}`
          if (chain.some((member) => !selectedIds.has(member.execution_id)))
            return yield* fail(
              'RETENTION_PLAN_STALE',
              `Continuation chain for ${run.execution_id} was not selected as a unit`
            )
          const chainRows = yield* sql<
            Pick<
              RunRow,
              'execution_id' | 'generation' | 'state' | 'continued_from' | 'continued_to'
            >
          >`SELECT execution_id, generation, state, continued_from, continued_to
            FROM better_workflows_runs WHERE namespace=${self.journal.namespace} AND chain_id=${run.chain_id}
            ORDER BY generation, execution_id`
          const chainIds = new Set(chainRows.map((member) => member.execution_id))
          const broken =
            chainRows.some((member, index) =>
              index === 0
                ? member.continued_from !== null
                : member.continued_from !== chainRows[index - 1]!.execution_id
            ) ||
            chainRows.some(
              (member) =>
                member.state === 'continued' &&
                (!member.continued_to || !chainIds.has(member.continued_to))
            )
          if (broken)
            return yield* fail(
              'RETENTION_PLAN_STALE',
              `Continuation chain for ${run.execution_id} is inconsistent`
            )
        }
        for (const run of selected) {
          const entity = `Workflow/${workflowDefinition(self.journal.namespace, run.workflow_name, run.version)._tag}`
          yield* sql`INSERT INTO better_workflows_tombstones(execution_id,namespace,workflow_name,version,dedupe_key,input_hash,state,pruned_at)
          VALUES (${run.execution_id},${run.namespace},${run.workflow_name},${run.version},${run.dedupe_key},${hash(run.input_json)},${run.state},${now})`
          yield* sql`DELETE FROM cluster_replies WHERE request_id IN (SELECT id FROM cluster_messages WHERE entity_id=${run.execution_id} AND entity_type IN (${entity}, 'Workflow/-/DurableClock'))`
          yield* sql`DELETE FROM cluster_messages WHERE entity_id=${run.execution_id} AND entity_type IN (${entity}, 'Workflow/-/DurableClock')`
          for (const table of tables)
            yield* sql`DELETE FROM ${sql(`better_workflows_${table}`)} WHERE execution_id=${run.execution_id}`
          yield* sql`DELETE FROM better_workflows_dead_letters
            WHERE namespace=${self.journal.namespace} AND execution_id=${run.execution_id}`
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
          yield* sql`SELECT execution_id FROM better_workflows_runs WHERE namespace=${namespace} AND state NOT IN ('continued','completed','failed','cancelled') LIMIT 1`
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

  listDeadLetters(options: DeadLetterListOptions = {}) {
    return new ActivityTransport(this.journal).deadLetters.list(options)
  }

  getDeadLetter(id: string, includePayload = false) {
    return new ActivityTransport(this.journal).deadLetters.get(id, includePayload)
  }

  requeueDeadLetter(id: string) {
    const transport = new ActivityTransport(this.journal)
    return transport.deadLetters.requeue(
      id,
      (queue, payload, deliveryId, initialAttempt, deadLetterId) =>
        transport.offerInTransaction(queue, payload, deliveryId, initialAttempt, deadLetterId)
    )
  }

  discardDeadLetter(id: string, options: DiscardDeadLetterOptions) {
    return new ActivityTransport(this.journal).deadLetters.discard(id, options)
  }
}
