import { Effect } from 'effect'
import type { Failure } from '../errors'
import type { QueueOptions } from '../types'
import type { Journal, ClaimRow } from './journal'
import type { ActivityEnvelope } from './wire'

type ActivityPermitPayload = Pick<
  ActivityEnvelope,
  'executionId' | 'stepId' | 'attempt' | 'concurrencyKey'
>

interface LimitRow {
  readonly global_limit: number | null
  readonly key_limit: number | null
}

/** A queue-row lock serializes admission; the claim and permit commit together. */
export class Permits {
  constructor(readonly journal: Journal) {}

  register(queue: string, options: QueueOptions) {
    const { sql, namespace } = this.journal
    return sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO better_workflows_limits(namespace, queue_name, global_limit, key_limit)
        VALUES (${namespace}, ${queue}, ${options.globalConcurrency ?? null}, ${options.perKeyConcurrency ?? null}) ON CONFLICT DO NOTHING`
        yield* sql`UPDATE better_workflows_limits SET queue_name = queue_name WHERE namespace = ${namespace} AND queue_name = ${queue}`
        const [row] =
          yield* sql<LimitRow>`SELECT global_limit, key_limit FROM better_workflows_limits WHERE namespace = ${namespace} AND queue_name = ${queue}`
        if (
          !row ||
          row.global_limit !== (options.globalConcurrency ?? null) ||
          row.key_limit !== (options.perKeyConcurrency ?? null)
        ) {
          return yield* Effect.fail<Failure>({
            code: 'QUEUE_LIMIT_CONFLICT',
            message: `All processes must agree on shared limits for queue ${queue}; change them offline with the admin API`,
            retryable: false
          })
        }
      })
    )
  }

  claim(
    queue: string,
    payload: ActivityPermitPayload,
    delivery: number,
    owner: string,
    lease: number
  ) {
    const { sql, namespace } = this.journal
    const self = this
    return sql.withTransaction(
      Effect.gen(function* () {
        // Lock order: queue, run, claim. Completion locks run before claim as well.
        yield* sql`UPDATE better_workflows_limits SET queue_name = queue_name WHERE namespace = ${namespace} AND queue_name = ${queue}`
        yield* self.journal.lockRun(payload.executionId)
        const run = yield* self.journal.get(payload.executionId)
        if (run.control === 'cancel' || ['completed', 'failed', 'cancelled'].includes(run.state))
          return 'closed' as const
        const [existing] =
          yield* sql<ClaimRow>`SELECT * FROM better_workflows_claims WHERE execution_id = ${payload.executionId} AND step_id = ${payload.stepId} AND attempt = ${payload.attempt}`
        if (existing?.state === 'completed') return existing
        if (existing && existing.delivery_attempt >= delivery && existing.owner_token !== owner)
          return 'stale' as const
        const [limits] =
          yield* sql<LimitRow>`SELECT global_limit, key_limit FROM better_workflows_limits WHERE namespace = ${namespace} AND queue_name = ${queue}`
        if (!limits) return yield* Effect.die(`Missing policy for queue ${queue}`)
        if (limits.key_limit !== null && payload.concurrencyKey === undefined)
          return yield* Effect.fail<Failure>({
            code: 'ACTIVITY_KEY_REQUIRED',
            message: `Queue ${queue} requires Activity.key`,
            retryable: false
          })
        const now = yield* self.journal.databaseNow()
        yield* sql`DELETE FROM better_workflows_permits WHERE namespace = ${namespace} AND queue_name = ${queue} AND lease_until <= ${now}`
        const [counts] = yield* sql<{ total: number; keyed: number }>`SELECT COUNT(*) AS total,
        COALESCE(SUM(CASE WHEN key_name = ${payload.concurrencyKey ?? ''} THEN 1 ELSE 0 END), 0) AS keyed
        FROM better_workflows_permits WHERE namespace = ${namespace} AND queue_name = ${queue}
        AND NOT(execution_id = ${payload.executionId} AND step_id = ${payload.stepId} AND attempt = ${payload.attempt})`
        if (
          (limits.global_limit !== null && Number(counts!.total) >= limits.global_limit) ||
          (limits.key_limit !== null && Number(counts!.keyed) >= limits.key_limit)
        )
          return 'blocked' as const
        const claim = yield* self.journal.claim(
          payload.executionId,
          payload.stepId,
          payload.attempt,
          delivery,
          owner,
          lease
        )
        if (claim.owner_token !== owner) return 'stale' as const
        yield* sql`INSERT INTO better_workflows_permits(namespace, queue_name, key_name, execution_id, step_id, attempt, owner_token, lease_until)
        VALUES (${namespace}, ${queue}, ${payload.concurrencyKey ?? ''}, ${payload.executionId}, ${payload.stepId}, ${payload.attempt}, ${owner}, ${claim.lease_until})
        ON CONFLICT(execution_id, step_id, attempt) DO UPDATE SET owner_token = ${owner}, lease_until = ${claim.lease_until}, key_name = ${payload.concurrencyKey ?? ''}`
        return claim
      })
    )
  }

  release(claim: ClaimRow) {
    return this.journal
      .sql`DELETE FROM better_workflows_permits WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt} AND owner_token = ${claim.owner_token}`
  }

  renew(claim: ClaimRow, lease: number) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const now = yield* self.journal.databaseNow()
        const rows = yield* self.journal
          .sql`UPDATE better_workflows_permits SET lease_until = lease_until
        WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt} AND owner_token = ${claim.owner_token} AND lease_until > ${now} RETURNING owner_token`
        if (!rows.length) return false
        if (!(yield* self.journal.renewClaim(claim, lease))) return false
        yield* self.journal
          .sql`UPDATE better_workflows_permits SET lease_until = ${now + lease} WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt} AND owner_token = ${claim.owner_token}`
        return true
      })
    )
  }

  finish(claim: ClaimRow, result: string | null, failure: Failure | null, retryDelay?: number) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.journal.lockRun(claim.execution_id)
        const now = yield* self.journal.databaseNow()
        const rows = yield* self.journal
          .sql`UPDATE better_workflows_permits SET lease_until = lease_until
        WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt} AND owner_token = ${claim.owner_token} AND lease_until > ${now} RETURNING owner_token`
        if (!rows.length) return false
        const finished = yield* self.journal.finishClaim(claim, result, failure, retryDelay)
        if (finished) yield* self.release(claim)
        return finished
      })
    )
  }
}
