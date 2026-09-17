import { createHash, randomUUID } from 'node:crypto'
import { Effect, Predicate, Tracer } from 'effect'
import { SqlError } from 'effect/unstable/sql/SqlError'
import type { Failure } from '../errors'
import type {
  DeadLetter,
  DeadLetterListOptions,
  DeadLetterPage,
  DeadLetterState,
  DiscardDeadLetterOptions
} from '../admin-types'
import { toFailure, WorkflowError } from '../errors'
import { encode, identifier, positiveInteger } from './values'
import type { Journal, RunRow } from './journal'
import {
  logAnnotations,
  TelemetryAttributeKey,
  TelemetryLogComponent,
  TelemetrySpanName
} from './telemetry'
import type { TelemetryAttributes } from './telemetry'

export interface ActivityDelivery {
  readonly sequence: number
  readonly namespace: string
  readonly queue: string
  readonly id: string
  readonly payload: string
  readonly deliveryAttempt: number
  readonly createdAt: number
  readonly acquiredAt: number | null
  readonly acquiredBy: string | null
  readonly deadLetterId: string | null
}

export interface ActivityMetadata {
  readonly executionId: string | null
  readonly stepId: string | null
  readonly activityName: string | null
  readonly activityVersion: number | null
  readonly businessAttempt: number | null
}

interface DeliveryRow {
  readonly sequence: number
  readonly namespace: string
  readonly queue_name: string
  readonly delivery_id: string
  readonly payload_json: string
  readonly attempts: number
  readonly state: string
  readonly acquired_at: number | null
  readonly acquired_by: string | null
  readonly dead_letter_id: string | null
  readonly created_at: number
}

interface DeadLetterRow {
  readonly id: string
  readonly namespace: string
  readonly queue_name: string
  readonly delivery_id: string
  readonly execution_id: string | null
  readonly step_id: string | null
  readonly activity_name: string | null
  readonly activity_version: number | null
  readonly business_attempt: number | null
  readonly delivery_attempt: number
  readonly reason_code: string
  readonly reason_message: string
  readonly first_failed_at: number
  readonly updated_at: number
  readonly requeue_count: number
  readonly state: DeadLetterState
  readonly payload_json: string
  readonly discard_reason: string | null
}

const terminal = (state: RunRow['state']) =>
  ['continued', 'completed', 'failed', 'cancelled'].includes(state)

const activityMetricAttributes = (
  queue: string,
  activityName: string | null,
  activityVersion: number | null,
  reason?: string
): TelemetryAttributes => {
  const attributes: TelemetryAttributes = { [TelemetryAttributeKey.queueName]: queue }
  if (activityName !== null) attributes[TelemetryAttributeKey.activityName] = activityName
  if (activityVersion !== null) attributes[TelemetryAttributeKey.activityVersion] = activityVersion
  if (reason !== undefined) attributes[TelemetryAttributeKey.deadLetterReason] = reason
  return attributes
}

const activitySpanAttributes = (
  queue: string,
  metadata: ActivityMetadata,
  options: {
    readonly deliveryAttempt?: number
    readonly deadLetterId?: string
    readonly reason?: string
  } = {}
): TelemetryAttributes => {
  const attributes: TelemetryAttributes = { [TelemetryAttributeKey.queueName]: queue }
  if (metadata.activityName !== null)
    attributes[TelemetryAttributeKey.activityName] = metadata.activityName
  if (metadata.activityVersion !== null)
    attributes[TelemetryAttributeKey.activityVersion] = metadata.activityVersion
  if (metadata.executionId !== null)
    attributes[TelemetryAttributeKey.executionId] = metadata.executionId
  if (metadata.stepId !== null) attributes[TelemetryAttributeKey.stepId] = metadata.stepId
  if (metadata.businessAttempt !== null)
    attributes[TelemetryAttributeKey.activityBusinessAttempt] = metadata.businessAttempt
  if (options.deliveryAttempt !== undefined)
    attributes[TelemetryAttributeKey.activityDeliveryAttempt] = options.deliveryAttempt
  if (options.deadLetterId !== undefined)
    attributes[TelemetryAttributeKey.deadLetterId] = options.deadLetterId
  if (options.reason !== undefined)
    attributes[TelemetryAttributeKey.deadLetterReason] = options.reason
  return attributes
}

const activityMetadataFromDeadLetter = (row: DeadLetterRow): ActivityMetadata => ({
  executionId: row.execution_id,
  stepId: row.step_id,
  activityName: row.activity_name,
  activityVersion: row.activity_version,
  businessAttempt: row.business_attempt
})

const annotateSpan = (span: Tracer.Span, attributes: TelemetryAttributes): void => {
  for (const [key, value] of Object.entries(attributes)) span.attribute(key, value)
}

const deadLetterLogAnnotations = (journal: Journal, attributes: TelemetryAttributes) =>
  logAnnotations(TelemetryLogComponent.deadLetter, {
    [TelemetryAttributeKey.namespace]: journal.namespace,
    ...attributes
  })

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- JSON.parse produces an untrusted value at the transport boundary.
const metadataFromParsed = (value: unknown): ActivityMetadata => {
  if (!Predicate.isObject(value))
    return {
      executionId: null,
      stepId: null,
      activityName: null,
      activityVersion: null,
      businessAttempt: null
    }
  const stringValue = (key: string) => (Predicate.isString(value[key]) ? value[key] : null)
  const numberValue = (key: string) => (Predicate.isNumber(value[key]) ? value[key] : null)
  return {
    executionId: stringValue('executionId'),
    stepId: stringValue('stepId'),
    activityName: stringValue('activityName'),
    activityVersion: numberValue('activityVersion'),
    businessAttempt: numberValue('attempt')
  }
}

/** Best-effort metadata extraction that never makes a corrupt payload executable. */
export function activityMetadata(payload: string): ActivityMetadata {
  try {
    return metadataFromParsed(JSON.parse(payload))
  } catch {
    return metadataFromParsed(null)
  }
}

function toDelivery(row: DeliveryRow): ActivityDelivery {
  return {
    sequence: Number(row.sequence),
    namespace: row.namespace,
    queue: row.queue_name,
    id: row.delivery_id,
    payload: row.payload_json,
    deliveryAttempt: Number(row.attempts),
    createdAt: Number(row.created_at),
    acquiredAt: row.acquired_at === null ? null : Number(row.acquired_at),
    acquiredBy: row.acquired_by,
    deadLetterId: row.dead_letter_id
  }
}

function toDeadLetter(row: DeadLetterRow, includePayload: boolean): DeadLetter {
  const value: DeadLetter = {
    id: row.id,
    namespace: row.namespace,
    queue: row.queue_name,
    executionId: row.execution_id,
    stepId: row.step_id,
    activityName: row.activity_name,
    activityVersion: row.activity_version === null ? null : Number(row.activity_version),
    businessAttempt: row.business_attempt === null ? null : Number(row.business_attempt),
    deliveryAttempt: Number(row.delivery_attempt),
    reasonCode: row.reason_code,
    reasonMessage: row.reason_message,
    firstFailedAt: new Date(Number(row.first_failed_at)).toISOString(),
    updatedAt: new Date(Number(row.updated_at)).toISOString(),
    requeueCount: Number(row.requeue_count),
    state: row.state
  }
  return includePayload ? { ...value, payload: row.payload_json } : value
}

const failure = (code: string, message: string) =>
  Effect.fail<Failure>({ code, message, retryable: false })

const deliveryIdentity = (payload: string) => {
  try {
    const value: unknown = JSON.parse(payload)
    if (!Predicate.isObject(value)) return payload
    return JSON.stringify(
      Object.fromEntries(
        Object.entries(value).filter(([key]) => !['traceId', 'spanId', 'sampled'].includes(key))
      )
    )
  } catch {
    return payload
  }
}

/**
 * Application-owned dead-letter persistence. The public admin API only uses this
 * store and never reads the private schema of Effect's persisted primitives.
 */
export class DeadLetterStore {
  constructor(readonly journal: Journal) {}

  private idFor(delivery: ActivityDelivery) {
    return `dlq-${createHash('sha256')
      .update(JSON.stringify([this.journal.namespace, delivery.queue, delivery.id]))
      .digest('hex')}`
  }

  private row(id: string) {
    return this.journal.sql<DeadLetterRow>`SELECT * FROM better_workflows_dead_letters
      WHERE namespace=${this.journal.namespace} AND id=${id}`
  }

  private validateList(options: DeadLetterListOptions) {
    const limit = options.limit ?? 100
    positiveInteger(limit, 'Dead-letter limit')
    if (limit > 1000)
      throw new WorkflowError('INVALID_PAGINATION', 'Dead-letter limit must be <=1000')
    for (const [value, label] of [
      [options.queue, 'Queue name'],
      [options.executionId, 'Execution ID'],
      [options.activity, 'Activity name'],
      [options.cursor, 'Dead-letter cursor']
    ] as const)
      if (value !== undefined) identifier(value, label)
    if (options.state && !['open', 'requeued', 'resolved', 'discarded'].includes(options.state))
      throw new WorkflowError('INVALID_ARGUMENT', `Unknown dead-letter state: ${options.state}`)
    return limit
  }

  list(options: DeadLetterListOptions = {}) {
    const self = this
    const limit = this.validateList(options)
    return Effect.gen(function* () {
      const conditions = [self.journal.sql`namespace=${self.journal.namespace}`]
      if (options.queue !== undefined)
        conditions.push(self.journal.sql`queue_name=${options.queue}`)
      if (options.executionId !== undefined)
        conditions.push(self.journal.sql`execution_id=${options.executionId}`)
      if (options.activity !== undefined)
        conditions.push(self.journal.sql`activity_name=${options.activity}`)
      if (options.state !== undefined) conditions.push(self.journal.sql`state=${options.state}`)
      if (options.cursor !== undefined) conditions.push(self.journal.sql`id>${options.cursor}`)
      const rows = yield* self.journal
        .sql<DeadLetterRow>`SELECT * FROM better_workflows_dead_letters
        WHERE ${self.journal.sql.and(conditions)}
        ORDER BY id LIMIT ${limit + 1}`
      const pageRows = rows.slice(0, limit)
      const page: DeadLetterPage = { deadLetters: pageRows.map((row) => toDeadLetter(row, false)) }
      if (rows.length > limit) return { ...page, nextCursor: pageRows.at(-1)!.id }
      return page
    })
  }

  get(id: string, includePayload = false) {
    identifier(id, 'Dead-letter ID')
    const self = this
    return Effect.gen(function* () {
      const [row] = yield* self.row(id)
      if (!row) return yield* failure('DEAD_LETTER_NOT_FOUND', `Dead letter ${id} was not found`)
      return toDeadLetter(row, includePayload)
    })
  }

  /** Return the first active blocked dependency used by WorkflowHandle.describe. */
  blockedOn(executionId: string) {
    return this.journal.sql<DeadLetterRow>`SELECT * FROM better_workflows_dead_letters
      WHERE namespace=${this.journal.namespace} AND execution_id=${executionId}
      AND state IN ('open','requeued') ORDER BY updated_at, id LIMIT 1`.pipe(
      Effect.map((rows) =>
        rows[0]
          ? {
              type: 'activity' as const,
              deadLetterId: rows[0].id,
              stepId: rows[0].step_id ?? '',
              activity: rows[0].activity_name,
              version: rows[0].activity_version === null ? null : Number(rows[0].activity_version),
              queue: rows[0].queue_name
            }
          : null
      )
    )
  }

  create(
    delivery: ActivityDelivery,
    metadata: ActivityMetadata,
    reasonCode: string,
    reasonMessage: string
  ) {
    const self = this
    const id = this.idFor(delivery)
    let inserted = false
    let resolved = false
    let discarded = false
    const operation = self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const now = yield* self.journal.databaseNow()
        if (metadata.executionId) yield* self.journal.lockRun(metadata.executionId)
        const ownership =
          delivery.acquiredBy === null
            ? self.journal.sql`state='pending' AND attempts=${delivery.deliveryAttempt}`
            : self.journal
                .sql`state='processing' AND acquired_by=${delivery.acquiredBy} AND attempts=${delivery.deliveryAttempt}`
        const updated = yield* self.journal.sql`UPDATE better_workflows_activity_deliveries
          SET state='failed', acquired_at=NULL, acquired_by=NULL,
            last_failure=${reasonMessage.slice(0, 4096)}, dead_letter_id=${id}, updated_at=${now}
          WHERE namespace=${self.journal.namespace} AND delivery_id=${delivery.id}
          AND queue_name=${delivery.queue} AND ${ownership} RETURNING delivery_id`
        if (!updated.length) {
          const [existing] = yield* self.row(id)
          if (existing) return toDeadLetter(existing, false)
          return yield* failure(
            'STORAGE_CONFLICT',
            `Activity delivery ${delivery.id} is no longer owned by this worker`
          )
        }
        const insertedRows = yield* self.journal.sql`INSERT INTO better_workflows_dead_letters
          (id, namespace, queue_name, delivery_id, execution_id, step_id, activity_name,
           activity_version, business_attempt, delivery_attempt, reason_code, reason_message,
           first_failed_at, updated_at, state, payload_json)
          VALUES (${id}, ${self.journal.namespace}, ${delivery.queue}, ${delivery.id},
            ${metadata.executionId}, ${metadata.stepId}, ${metadata.activityName},
            ${metadata.activityVersion}, ${metadata.businessAttempt}, ${delivery.deliveryAttempt},
            ${reasonCode}, ${reasonMessage.slice(0, 4096)}, ${now}, ${now}, 'open', ${delivery.payload})
          ON CONFLICT(namespace, delivery_id) DO NOTHING RETURNING id`
        inserted = insertedRows.length > 0
        const [existing] = yield* self.row(id)
        if (!existing)
          return yield* failure('STORAGE_CONFLICT', `Dead letter ${id} was not persisted`)
        if (delivery.deadLetterId) {
          const resolvedRows = yield* self.journal.sql<{
            id: string
          }>`UPDATE better_workflows_dead_letters SET state='resolved', updated_at=${now}
            WHERE namespace=${self.journal.namespace} AND id=${delivery.deadLetterId} AND state='requeued'
            RETURNING id`
          resolved = resolvedRows.length > 0
        }
        if (inserted && metadata.executionId) {
          const [run] = yield* self.journal.sql<RunRow>`SELECT * FROM better_workflows_runs
            WHERE execution_id=${metadata.executionId} AND namespace=${self.journal.namespace}`
          if (run && (run.control === 'cancel' || terminal(run.state))) {
            const discardedRows = yield* self.journal.sql<{
              id: string
            }>`UPDATE better_workflows_dead_letters
              SET state='discarded', discard_reason=${
                run.control === 'cancel' || run.state === 'cancelled'
                  ? 'Owner execution was cancelled'
                  : 'Owner execution is terminal'
              }, updated_at=${now}
              WHERE namespace=${self.journal.namespace} AND id=${id} AND state='open' RETURNING id`
            discarded = discardedRows.length > 0
          } else {
            yield* self.journal
              .sql`UPDATE better_workflows_runs SET state='blocked', wait_type='activity', wait_step=${metadata.stepId}
              WHERE execution_id=${metadata.executionId} AND namespace=${self.journal.namespace}
              AND control <> 'cancel' AND state NOT IN ('continued','completed','failed','cancelled')`
            const [blocked] = yield* self.journal.sql<RunRow>`SELECT * FROM better_workflows_runs
              WHERE execution_id=${metadata.executionId} AND namespace=${self.journal.namespace}`
            if (blocked && blocked.state === 'blocked')
              yield* self.journal.event(
                metadata.executionId,
                'activity.dead-lettered',
                {
                  deadLetterId: id,
                  reasonCode,
                  activity: metadata.activityName,
                  version: metadata.activityVersion,
                  queue: delivery.queue,
                  deliveryAttempt: delivery.deliveryAttempt
                },
                metadata.stepId
              )
          }
        }
        return toDeadLetter(existing, false)
      })
    )
    const tracedOperation = Effect.useSpan(
      TelemetrySpanName.deadLetterCreate,
      {
        attributes: activitySpanAttributes(delivery.queue, metadata, {
          deliveryAttempt: delivery.deliveryAttempt,
          deadLetterId: id,
          reason: reasonCode
        }),
        kind: 'producer'
      },
      () => operation
    ).pipe(
      Effect.tapError((error) =>
        Effect.annotateCurrentSpan(TelemetryAttributeKey.failureCode, toFailure(error).code)
      )
    )
    return tracedOperation.pipe(
      Effect.tap(() =>
        Effect.gen(function* () {
          const attributes = activityMetricAttributes(
            delivery.queue,
            metadata.activityName,
            metadata.activityVersion,
            reasonCode
          )
          if (inserted) {
            self.journal.telemetry?.count('deadLetterCreated', attributes)
            self.journal.telemetry?.count('activityDeadLettered', attributes)
            yield* Effect.annotateLogs(
              Effect.logWarning('Dead letter created'),
              deadLetterLogAnnotations(
                self.journal,
                activitySpanAttributes(delivery.queue, metadata, {
                  deliveryAttempt: delivery.deliveryAttempt,
                  deadLetterId: id,
                  reason: reasonCode
                })
              )
            )
          }
          if (resolved) self.journal.telemetry?.count('deadLetterResolved', attributes)
          if (discarded) self.journal.telemetry?.count('deadLetterDiscarded', attributes)
        })
      )
    )
  }

  requeue(
    id: string,
    offer: (
      queue: string,
      payload: string,
      deliveryId: string,
      initialAttempt: number,
      deadLetterId: string
    ) => Effect.Effect<boolean, Failure | SqlError, never>
  ) {
    identifier(id, 'Dead-letter ID')
    const self = this
    let requeued = false
    let dispatched = false
    let metricCurrent: DeadLetterRow | undefined
    const operation = self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const [current] = yield* self.row(id)
        if (!current)
          return yield* failure('DEAD_LETTER_NOT_FOUND', `Dead letter ${id} was not found`)
        if (current.state === 'requeued') return toDeadLetter(current, false)
        if (current.state !== 'open')
          return yield* failure(
            'DEAD_LETTER_NOT_OPEN',
            `Dead letter ${id} is already ${current.state}`
          )
        let run: RunRow | undefined
        if (current.execution_id) {
          yield* self.journal.lockRun(current.execution_id)
          run = (yield* self.journal.sql<RunRow>`SELECT * FROM better_workflows_runs
            WHERE execution_id=${current.execution_id} AND namespace=${self.journal.namespace}`)[0]
        }
        if (run && (run.control === 'cancel' || terminal(run.state)))
          return yield* failure(
            'TERMINAL_EXECUTION',
            `Execution ${run.execution_id} is ${run.state === 'cancelled' || run.control === 'cancel' ? 'cancelled' : 'terminal'}`
          )
        metricCurrent = current
        const count = Number(current.requeue_count) + 1
        const deliveryId = `${id}:r${count}`
        dispatched = yield* offer(
          current.queue_name,
          current.payload_json,
          deliveryId,
          Number(current.delivery_attempt),
          id
        )
        const now = yield* self.journal.databaseNow()
        const requeuedRows = yield* self.journal.sql<{
          id: string
        }>`UPDATE better_workflows_dead_letters
          SET state='requeued', requeue_count=${count}, updated_at=${now}
          WHERE namespace=${self.journal.namespace} AND id=${id} AND state='open' RETURNING id`
        requeued = requeuedRows.length > 0
        if (run && run.state === 'blocked') {
          const remaining = yield* self.journal.sql`SELECT id FROM better_workflows_dead_letters
            WHERE namespace=${self.journal.namespace} AND execution_id=${run.execution_id}
            AND state IN ('open','requeued') LIMIT 1`
          if (!remaining.length)
            yield* self.journal.sql`UPDATE better_workflows_runs SET state='waiting'
              WHERE execution_id=${run.execution_id} AND namespace=${self.journal.namespace} AND state='blocked'`
        }
        if (run)
          yield* self.journal.event(
            run.execution_id,
            'activity.dead-letter-requeued',
            {
              deadLetterId: id,
              deliveryAttempt: Number(current.delivery_attempt) + 1,
              requeueCount: count
            },
            current.step_id
          )
        const [updatedRow] = yield* self.row(id)
        return toDeadLetter(updatedRow!, false)
      })
    )
    const tracedOperation = Effect.useSpan(
      TelemetrySpanName.deadLetterRequeue,
      {
        attributes: { [TelemetryAttributeKey.deadLetterId]: id },
        kind: 'producer'
      },
      (span) =>
        operation.pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              if (metricCurrent)
                annotateSpan(
                  span,
                  activitySpanAttributes(
                    metricCurrent.queue_name,
                    activityMetadataFromDeadLetter(metricCurrent),
                    {
                      deadLetterId: id,
                      reason: metricCurrent.reason_code
                    }
                  )
                )
            })
          )
        )
    ).pipe(
      Effect.tapError((error) =>
        Effect.annotateCurrentSpan(TelemetryAttributeKey.failureCode, toFailure(error).code)
      )
    )
    return tracedOperation.pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          if (!requeued || !metricCurrent) return
          const attributes = activityMetricAttributes(
            metricCurrent.queue_name,
            metricCurrent.activity_name,
            metricCurrent.activity_version,
            metricCurrent.reason_code
          )
          self.journal.telemetry?.count('deadLetterRequeued', attributes)
          if (dispatched) self.journal.telemetry?.count('activityDispatched', attributes)
        })
      )
    )
  }

  discard(id: string, options: DiscardDeadLetterOptions) {
    identifier(id, 'Dead-letter ID')
    if (!options.reason.trim())
      throw new WorkflowError('INVALID_REASON', 'A discard reason is required')
    if (options.reason.length > 4096)
      throw new WorkflowError('INVALID_REASON', 'Discard reasons are limited to 4096 characters')
    const self = this
    let discarded: ReadonlyArray<{
      queue_name: string
      activity_name: string | null
      activity_version: number | null
      reason_code: string
    }> = []
    let metricCurrent: DeadLetterRow | undefined
    let failedRun: RunRow | undefined
    let failedAt: number | undefined
    let failedChainCreatedAt: number | undefined
    const operation = self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const [current] = yield* self.row(id)
        if (!current)
          return yield* failure('DEAD_LETTER_NOT_FOUND', `Dead letter ${id} was not found`)
        metricCurrent = current
        if (current.state === 'discarded') return toDeadLetter(current, false)
        if (current.state !== 'open' && current.state !== 'requeued')
          return yield* failure(
            'DEAD_LETTER_NOT_OPEN',
            `Dead letter ${id} is already ${current.state}`
          )
        let run: RunRow | undefined
        if (current.execution_id) {
          yield* self.journal.lockRun(current.execution_id)
          run = (yield* self.journal.sql<RunRow>`SELECT * FROM better_workflows_runs
            WHERE execution_id=${current.execution_id} AND namespace=${self.journal.namespace}`)[0]
        }
        const now = yield* self.journal.databaseNow()
        discarded = yield* self.journal.sql<{
          queue_name: string
          activity_name: string | null
          activity_version: number | null
          reason_code: string
        }>`UPDATE better_workflows_dead_letters SET state='discarded', discard_reason=${options.reason}, updated_at=${now}
          WHERE namespace=${self.journal.namespace}
          AND (id=${id} OR (execution_id=${current.execution_id} AND execution_id IS NOT NULL))
          AND state IN ('open','requeued')
          RETURNING queue_name, activity_name, activity_version, reason_code`
        yield* self.journal.sql`UPDATE better_workflows_activity_deliveries SET state='failed',
          acquired_at=NULL, acquired_by=NULL, updated_at=${now}
          WHERE namespace=${self.journal.namespace} AND state IN ('pending','processing')
          AND (dead_letter_id=${id} OR (${current.execution_id} IS NOT NULL AND execution_id=${current.execution_id}))`
        if (run && !terminal(run.state) && run.control !== 'cancel') {
          const operational: Failure = {
            code: 'WORKFLOW_DEAD_LETTER_DISCARDED',
            message: options.reason,
            retryable: false
          }
          const failed = yield* self.journal.sql<{
            execution_id: string
          }>`UPDATE better_workflows_runs SET state='failed', control='cancel',
            control_revision=control_revision+1, wait_type=NULL, wait_step=NULL,
            result_json=NULL, failure_json=${encode(operational)}
            WHERE execution_id=${run.execution_id} AND namespace=${self.journal.namespace}
            AND state NOT IN ('continued','completed','failed','cancelled') RETURNING execution_id`
          if (failed.length) {
            failedRun = run
            failedAt = now
            const [chain] = yield* self.journal.sql<{
              created_at: number
            }>`SELECT MIN(created_at) AS created_at
              FROM better_workflows_runs WHERE namespace=${self.journal.namespace} AND chain_id=${run.chain_id}`
            if (chain) failedChainCreatedAt = Number(chain.created_at)
          }
          yield* self.journal.event(
            run.execution_id,
            'activity.dead-letter-discarded',
            {
              deadLetterId: id,
              reason: options.reason
            },
            current.step_id
          )
          yield* self.journal.event(run.execution_id, 'workflow.failed', {
            code: operational.code,
            message: operational.message
          })
        }
        const [updated] = yield* self.row(id)
        return toDeadLetter(updated!, false)
      })
    )
    const tracedOperation = Effect.useSpan(
      TelemetrySpanName.deadLetterDiscard,
      {
        attributes: { [TelemetryAttributeKey.deadLetterId]: id },
        kind: 'producer'
      },
      (span) =>
        operation.pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              if (metricCurrent)
                annotateSpan(
                  span,
                  activitySpanAttributes(
                    metricCurrent.queue_name,
                    activityMetadataFromDeadLetter(metricCurrent),
                    {
                      deadLetterId: id,
                      reason: metricCurrent.reason_code
                    }
                  )
                )
            })
          )
        )
    ).pipe(
      Effect.tapError((error) =>
        Effect.annotateCurrentSpan(TelemetryAttributeKey.failureCode, toFailure(error).code)
      )
    )
    return tracedOperation.pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          for (const row of discarded)
            self.journal.telemetry?.count(
              'deadLetterDiscarded',
              activityMetricAttributes(
                row.queue_name,
                row.activity_name,
                row.activity_version,
                row.reason_code
              )
            )
          if (failedRun && failedAt !== undefined) {
            const attributes = {
              [TelemetryAttributeKey.workflowName]: failedRun.workflow_name,
              [TelemetryAttributeKey.workflowVersion]: failedRun.version
            }
            self.journal.telemetry?.count('workflowFailed', attributes)
            self.journal.telemetry?.observe(
              'workflowDuration',
              Math.max(0, failedAt - Number(failedRun.created_at)),
              attributes
            )
            if (failedChainCreatedAt !== undefined)
              self.journal.telemetry?.observe(
                'workflowChainDuration',
                Math.max(0, failedAt - failedChainCreatedAt),
                attributes
              )
          }
        })
      )
    )
  }

  /** Mark all open dependencies as discarded when their owner is cancelled. */
  discardForCancelled(executionId: string, now: number) {
    return this.journal
      .sql`UPDATE better_workflows_dead_letters SET state='discarded', discard_reason='Owner execution was cancelled', updated_at=${now}
      WHERE namespace=${this.journal.namespace} AND execution_id=${executionId} AND state IN ('open','requeued')`
  }
}

function offerInTransaction(
  journal: Journal,
  queue: string,
  payload: string,
  deliveryId: string,
  initialAttempt: number,
  deadLetterId: string | null,
  now: number
) {
  return Effect.gen(function* () {
    const metadata = activityMetadata(payload)
    const inserted = yield* journal.sql`INSERT INTO better_workflows_activity_deliveries
      (namespace, queue_name, delivery_id, execution_id, payload_json, attempts, state, visible_at, dead_letter_id, created_at, updated_at)
      VALUES (${journal.namespace}, ${queue}, ${deliveryId}, ${metadata.executionId}, ${payload}, ${initialAttempt}, 'pending', ${now}, ${deadLetterId}, ${now}, ${now})
      ON CONFLICT(namespace, queue_name, delivery_id) DO NOTHING RETURNING delivery_id`
    if (!inserted.length) {
      const [existing] =
        yield* journal.sql<DeliveryRow>`SELECT * FROM better_workflows_activity_deliveries
        WHERE namespace=${journal.namespace} AND queue_name=${queue} AND delivery_id=${deliveryId}`
      if (!existing || deliveryIdentity(existing.payload_json) !== deliveryIdentity(payload))
        return yield* failure(
          'STORAGE_CONFLICT',
          `Activity delivery ${deliveryId} differs from its existing payload`
        )
    }
    return inserted.length > 0
  })
}

/** Durable activity transport and its worker-facing lease operations. */
export class ActivityTransport {
  readonly deadLetters: DeadLetterStore

  constructor(readonly journal: Journal) {
    this.deadLetters = new DeadLetterStore(journal)
  }

  offer(queue: string, payload: string, deliveryId: string) {
    const self = this
    const operation = self.journal.sql.withTransaction(
      Effect.gen(function* () {
        return yield* offerInTransaction(
          self.journal,
          queue,
          payload,
          deliveryId,
          0,
          null,
          yield* self.journal.databaseNow()
        )
      })
    )
    return operation.pipe(
      Effect.tap((inserted) =>
        Effect.sync(() => {
          if (inserted)
            self.journal.telemetry?.count(
              'activityDispatched',
              activityMetricAttributes(
                queue,
                activityMetadata(payload).activityName,
                activityMetadata(payload).activityVersion
              )
            )
        })
      )
    )
  }

  /** Same operation as offer, for an enclosing dead-letter transaction. */
  offerInTransaction(
    queue: string,
    payload: string,
    deliveryId: string,
    initialAttempt: number,
    deadLetterId: string | null
  ) {
    const self = this
    return Effect.gen(function* () {
      return yield* offerInTransaction(
        self.journal,
        queue,
        payload,
        deliveryId,
        initialAttempt,
        deadLetterId,
        yield* self.journal.databaseNow()
      )
    })
  }

  take(queue: string, maxAttempts: number, lease: number) {
    const self = this
    const owner = randomUUID()
    return Effect.gen(function* () {
      const now = yield* self.journal.databaseNow()
      const rows = yield* self.journal.sql<DeliveryRow>`UPDATE better_workflows_activity_deliveries
        SET state='processing', attempts=attempts+1, acquired_at=${now}, acquired_by=${owner}, updated_at=${now}
        WHERE namespace=${self.journal.namespace} AND queue_name=${queue}
        AND attempts < ${maxAttempts}
        AND ((state='pending' AND visible_at<=${now})
          OR (state='processing' AND acquired_at IS NOT NULL AND acquired_at<=${now - lease}))
        AND sequence = (
          SELECT sequence FROM better_workflows_activity_deliveries
          WHERE namespace=${self.journal.namespace} AND queue_name=${queue}
          AND attempts < ${maxAttempts}
          AND ((state='pending' AND visible_at<=${now})
            OR (state='processing' AND acquired_at IS NOT NULL AND acquired_at<=${now - lease}))
          ORDER BY visible_at, sequence LIMIT 1
        ) RETURNING *`
      const row = rows[0]
      if (!row) return undefined
      const delivery = toDelivery(row)
      if (delivery.deliveryAttempt === 1)
        self.journal.telemetry?.observe(
          'activityQueueWait',
          Math.max(0, now - delivery.createdAt),
          activityMetricAttributes(
            queue,
            activityMetadata(delivery.payload).activityName,
            activityMetadata(delivery.payload).activityVersion
          )
        )
      return delivery
    })
  }

  retry(delivery: ActivityDelivery, reason: string, delay: number) {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.journal.databaseNow()
      const rows = yield* self.journal.sql<{
        delivery_id: string
      }>`UPDATE better_workflows_activity_deliveries SET state='pending',
        acquired_at=NULL, acquired_by=NULL,
        visible_at=${now + delay}, last_failure=${reason.slice(0, 4096)}, updated_at=${now}
        WHERE namespace=${self.journal.namespace} AND queue_name=${delivery.queue}
        AND delivery_id=${delivery.id} AND state='processing' AND acquired_by=${delivery.acquiredBy}
        RETURNING delivery_id`
      const retried = rows.length > 0
      if (retried)
        self.journal.telemetry?.count(
          'activityDeliveryRetried',
          activityMetricAttributes(
            delivery.queue,
            activityMetadata(delivery.payload).activityName,
            activityMetadata(delivery.payload).activityVersion
          )
        )
      return retried
    })
  }

  release(delivery: ActivityDelivery, delay = 0) {
    const self = this
    return Effect.gen(function* () {
      const instant = yield* self.journal.databaseNow()
      const rows = yield* self.journal.sql<{
        delivery_id: string
      }>`UPDATE better_workflows_activity_deliveries SET state='pending',
        attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END, acquired_at=NULL, acquired_by=NULL,
        visible_at=${instant + delay}, updated_at=${instant}
      WHERE namespace=${self.journal.namespace} AND queue_name=${delivery.queue}
      AND delivery_id=${delivery.id} AND state='processing' AND acquired_by=${delivery.acquiredBy}
      RETURNING delivery_id`
      const released = rows.length > 0
      if (released)
        self.journal.telemetry?.count(
          'activityDeliveryRetried',
          activityMetricAttributes(
            delivery.queue,
            activityMetadata(delivery.payload).activityName,
            activityMetadata(delivery.payload).activityVersion
          )
        )
      return released
    })
  }

  complete(delivery: ActivityDelivery) {
    const self = this
    let resolved = false
    const operation = self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const now = yield* self.journal.databaseNow()
        const metadata = activityMetadata(delivery.payload)
        if (metadata.executionId) yield* self.journal.lockRun(metadata.executionId)
        const rows = yield* self.journal
          .sql`UPDATE better_workflows_activity_deliveries SET state='completed',
          acquired_at=NULL, acquired_by=NULL, updated_at=${now}
          WHERE namespace=${self.journal.namespace} AND queue_name=${delivery.queue}
          AND delivery_id=${delivery.id} AND state='processing' AND acquired_by=${delivery.acquiredBy}
          RETURNING delivery_id`
        if (rows.length && delivery.deadLetterId)
          resolved = yield* self.journal.sql<{
            id: string
          }>`UPDATE better_workflows_dead_letters SET state='resolved', updated_at=${now}
            WHERE namespace=${self.journal.namespace} AND id=${delivery.deadLetterId} AND state='requeued'
            RETURNING id`.pipe(Effect.map((updated) => updated.length > 0))
        if (rows.length && delivery.deadLetterId) {
          if (metadata.executionId) {
            const remaining = yield* self.journal.sql`SELECT id FROM better_workflows_dead_letters
              WHERE namespace=${self.journal.namespace} AND execution_id=${metadata.executionId}
              AND state IN ('open','requeued') LIMIT 1`
            if (!remaining.length)
              yield* self.journal.sql`UPDATE better_workflows_runs SET state='waiting'
                WHERE namespace=${self.journal.namespace} AND execution_id=${metadata.executionId}
                AND state='blocked'`
          }
        }
        return rows.length > 0
      })
    )
    return operation.pipe(
      Effect.tap((completed) =>
        Effect.sync(() => {
          if (completed && resolved && delivery.deadLetterId)
            self.journal.telemetry?.count(
              'deadLetterResolved',
              activityMetricAttributes(
                delivery.queue,
                activityMetadata(delivery.payload).activityName,
                activityMetadata(delivery.payload).activityVersion
              )
            )
        })
      )
    )
  }

  deadLetter(
    delivery: ActivityDelivery,
    metadata: ActivityMetadata,
    reasonCode: string,
    reasonMessage: string
  ) {
    return this.deadLetters.create(delivery, metadata, reasonCode, reasonMessage)
  }

  exhausted(queue: string, maxAttempts: number, lease: number) {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.journal.databaseNow()
      const rows = yield* self.journal
        .sql<DeliveryRow>`SELECT * FROM better_workflows_activity_deliveries
        WHERE namespace=${self.journal.namespace} AND queue_name=${queue} AND attempts>=${maxAttempts}
        AND ((state='pending') OR (state='processing' AND acquired_at IS NOT NULL AND acquired_at<=${now - lease}))
        ORDER BY sequence LIMIT 20`
      for (const row of rows)
        yield* self.deadLetter(
          toDelivery(row),
          activityMetadata(row.payload_json),
          'DELIVERY_ATTEMPTS_EXHAUSTED',
          `Activity delivery exceeded ${maxAttempts} transport attempts`
        )
    })
  }
}
